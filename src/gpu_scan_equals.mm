// gpu_scan_equals.mm
// I build & run a Metal equality filter and return matching rowIDs.
// I cache the device/queue/pipeline so I don't recompile every call.


#include <../metal-cpp/Metal/Metal.hpp>
#include <../metal-cpp/Foundation/Foundation.hpp>

#include <vector>
#include <cstdint>
#include <cstring>
#include <algorithm>
#include <cstdio>
#include <atomic>
#include <mutex>

static const char* kShaderSrc = R"METAL(
#include <metal_stdlib>
using namespace metal;

struct Params {
    uint32_t needle;
    uint32_t n;
};

kernel void filter_equals(device const uint32_t* values    [[buffer(0)]],
                          device const uint32_t* rowIDs    [[buffer(1)]],
                          device uint32_t*       outRowIDs [[buffer(2)]],
                          device atomic_uint*    outCount  [[buffer(3)]],
                          constant Params&       P         [[buffer(4)]],
                          uint gid [[thread_position_in_grid]])
{
    if (gid >= P.n) return;
    uint v = values[gid];
    if (v == P.needle) {
        uint idx = atomic_fetch_add_explicit(outCount, 1, memory_order_relaxed);
        outRowIDs[idx] = rowIDs[gid];
    }
}
)METAL";

// C-callable helpers for tests and other TUs
extern "C" bool metalIsAvailable();
extern "C" void metalPrintDevices();

// ---- Pipeline cache for filter_equals --------------------------------------
// I keep these retained for the lifetime of the process.

namespace {
    static MTL::Device*               s_dev  = nullptr;
    static MTL::CommandQueue*         s_q    = nullptr;
    static MTL::ComputePipelineState* s_pso  = nullptr;
    static std::atomic<bool>          s_ready{false};
    static std::mutex                 s_initMu;

    bool ensureFilterEqualsPipeline() {
        if (s_ready.load(std::memory_order_acquire)) return true;
        std::lock_guard<std::mutex> g(s_initMu);
        if (s_ready.load(std::memory_order_relaxed)) return true;

        // device
        s_dev = MTL::CreateSystemDefaultDevice();
        if (!s_dev) {
            auto* arr = MTL::CopyAllDevices();
            if (arr && arr->count() > 0) {
                s_dev = static_cast<MTL::Device*>(arr->object(0));
                s_dev->retain(); // keep past arr->release()
            }
            if (arr) arr->release();
        }
        if (!s_dev) return false;

        // queue
        s_q = s_dev->newCommandQueue();
        if (!s_q) return false;

        // compile library once
        NS::Error* err = nullptr;
        NS::String* src = NS::String::string(kShaderSrc, NS::UTF8StringEncoding); // autoreleased
        MTL::CompileOptions* opts = MTL::CompileOptions::alloc()->init();
        if (s_dev->supportsFamily(MTL::GPUFamilyApple7)) {
            opts->setLanguageVersion(MTL::LanguageVersion3_0);
        } else {
            opts->setLanguageVersion(MTL::LanguageVersion2_4);
        }
        MTL::Library* lib = s_dev->newLibrary(src, opts, &err);
        opts->release();
        if (!lib) return false;

        // function → pipeline
        NS::String* fnName = NS::String::string("filter_equals", NS::UTF8StringEncoding); // autoreleased
        MTL::Function* fn = lib->newFunction(fnName);
        lib->release();
        if (!fn) return false;

        NS::Error* psoErr = nullptr;
        s_pso = s_dev->newComputePipelineState(fn, &psoErr);
        fn->release();
        if (!s_pso) {
            if (psoErr) psoErr->release();
            return false;
        }

        s_ready.store(true, std::memory_order_release);
        return true;
    }

    inline MTL::Device* dev()  { return s_dev; }
    inline MTL::CommandQueue* q() { return s_q; }
    inline MTL::ComputePipelineState* pso() { return s_pso; }
} // namespace

bool metalIsAvailable() {
    NS::AutoreleasePool* pool = NS::AutoreleasePool::alloc()->init();
    bool ok = false;
    if (auto* d = MTL::CreateSystemDefaultDevice()) { ok = true; d->release(); }
    else {
        auto* arr = MTL::CopyAllDevices();
        if (arr && arr->count() > 0) ok = true;
        if (arr) arr->release();
    }
    pool->release();
    return ok;
}

void metalPrintDevices() {
    NS::AutoreleasePool* pool = NS::AutoreleasePool::alloc()->init();
    auto* arr = MTL::CopyAllDevices();
    if (!arr) {
        std::fprintf(stderr, "[MetalDBG] CopyAllDevices returned null\n");
        pool->release();
        return;
    }
    std::fprintf(stderr, "[MetalDBG] Devices (%lu):\n", (unsigned long)arr->count());
    for (NS::UInteger i = 0; i < arr->count(); ++i) {
        auto* dev  = static_cast<MTL::Device*>(arr->object(i));
        auto* name = dev->name();
        std::fprintf(stderr, "  #%lu: %s\n", (unsigned long)i, name ? name->utf8String() : "(null)");
    }
    arr->release();
    pool->release();
}

std::vector<uint32_t>
gpuScanEquals(const std::vector<uint32_t>& values,
              const std::vector<uint32_t>& rowIDs,
              uint32_t needle)
{
    std::vector<uint32_t> empty;
    if (values.size() != rowIDs.size()) {
        std::fprintf(stderr, "[MetalDBG] values.size()!=rowIDs.size() (%zu vs %zu)\n",
                     values.size(), rowIDs.size());
        return empty;
    }

    NS::AutoreleasePool* pool = NS::AutoreleasePool::alloc()->init();
    std::fprintf(stderr, "[MetalDBG] Step 0: begin, n=%zu\n", values.size());

    // ensure cached device/queue/pipeline
    if (!ensureFilterEqualsPipeline()) {
        std::fprintf(stderr, "[MetalDBG] FAIL: ensureFilterEqualsPipeline\n");
        pool->release();
        return empty;
    }

    MTL::Device* dev = ::dev();
    MTL::CommandQueue* cq = ::q();
    MTL::ComputePipelineState* cps = ::pso();

    std::fprintf(stderr, "[MetalDBG] Step 1: device ok\n");
    std::fprintf(stderr, "[MetalDBG] Step 2: command queue ok\n");
    std::fprintf(stderr, "[MetalDBG] Step 5: pipeline ok, maxThreads=%lu\n",
                 (unsigned long)cps->maxTotalThreadsPerThreadgroup());

    const uint32_t n = static_cast<uint32_t>(values.size());
    const size_t valuesBytes = values.size() * sizeof(uint32_t);
    const size_t rowIDsBytes = rowIDs.size() * sizeof(uint32_t);

    // buffers
    MTL::Buffer* inValues  = dev->newBuffer(valuesBytes, MTL::ResourceStorageModeShared);
    MTL::Buffer* inRowIDs  = dev->newBuffer(rowIDsBytes, MTL::ResourceStorageModeShared);
    MTL::Buffer* outRowIDs = dev->newBuffer(valuesBytes, MTL::ResourceStorageModeShared);
    MTL::Buffer* outCount  = dev->newBuffer(sizeof(uint32_t), MTL::ResourceStorageModeShared);
    if (!inValues || !inRowIDs || !outRowIDs || !outCount) {
        std::fprintf(stderr, "[MetalDBG] FAIL: buffer alloc\n");
        if (outCount)  outCount->release();
        if (outRowIDs) outRowIDs->release();
        if (inRowIDs)  inRowIDs->release();
        if (inValues)  inValues->release();
        pool->release();
        return empty;
    }
    std::fprintf(stderr, "[MetalDBG] Step 6: buffers ok\n");

    struct Params { uint32_t needle; uint32_t n; } P{needle, n};
    MTL::Buffer* paramsBuf = dev->newBuffer(sizeof(Params), MTL::ResourceStorageModeShared);
    if (!paramsBuf) {
        std::fprintf(stderr, "[MetalDBG] FAIL: params buffer alloc\n");
        outCount->release(); outRowIDs->release(); inRowIDs->release(); inValues->release();
        pool->release();
        return empty;
    }

    std::memcpy(inValues->contents(), values.data(), valuesBytes);
    std::memcpy(inRowIDs->contents(), rowIDs.data(), rowIDsBytes);
    std::memcpy(paramsBuf->contents(), &P, sizeof(P));
    std::memset(outCount->contents(), 0, sizeof(uint32_t));
    std::fprintf(stderr, "[MetalDBG] Step 7: uploads ok\n");

    // encode/dispatch
    MTL::CommandBuffer* cb  = cq->commandBuffer();                  // autoreleased
    MTL::ComputeCommandEncoder* enc = cb ? cb->computeCommandEncoder() : nullptr;
    if (!cb || !enc) {
        std::fprintf(stderr, "[MetalDBG] FAIL: commandBuffer/encoder null\n");
        paramsBuf->release(); outCount->release(); outRowIDs->release();
        inRowIDs->release(); inValues->release();
        pool->release();
        return empty;
    }

    enc->setComputePipelineState(cps);
    enc->setBuffer(inValues,  0, 0);
    enc->setBuffer(inRowIDs,  0, 1);
    enc->setBuffer(outRowIDs, 0, 2);
    enc->setBuffer(outCount,  0, 3);
    enc->setBuffer(paramsBuf, 0, 4);

    const uint32_t tgs = std::min<uint32_t>(cps->maxTotalThreadsPerThreadgroup(), 256);
    MTL::Size threadsPerTg = MTL::Size::Make(tgs, 1, 1);
    MTL::Size grid         = MTL::Size::Make(n,   1, 1);
    enc->dispatchThreads(grid, threadsPerTg);
    enc->endEncoding();
    std::fprintf(stderr, "[MetalDBG] Step 8: encoded dispatch (n=%u, tgs=%u)\n", n, tgs);

    cb->commit();
    cb->waitUntilCompleted();
    std::fprintf(stderr, "[MetalDBG] Step 9: GPU completed\n");

    // read back
    uint32_t count = *reinterpret_cast<uint32_t*>(outCount->contents());
    if (count > n) {
        std::fprintf(stderr, "[MetalDBG] WARN: count %u > n %u (clamping)\n", count, n);
        count = n;
    }
    std::vector<uint32_t> result(count);
    if (count) {
        std::memcpy(result.data(), outRowIDs->contents(), count * sizeof(uint32_t));
    }
    std::fprintf(stderr, "[MetalDBG] Step 10: readback ok (count=%u)\n", count);

    // cleanup per-call resources
    paramsBuf->release();
    outCount->release();
    outRowIDs->release();
    inRowIDs->release();
    inValues->release();
    pool->release();

    return result;
}