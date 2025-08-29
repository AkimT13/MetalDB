// gpu_scan_equals.mm
// Builds & runs a Metal equality filter, returning matching rowIDs.
// Fixed: do NOT release autoreleased NS::String::string(...) results.

#define NS_PRIVATE_IMPLEMENTATION
#define MTL_PRIVATE_IMPLEMENTATION
#include <../metal-cpp/Metal/Metal.hpp>
#include <../metal-cpp/Foundation/Foundation.hpp>

#include <vector>
#include <cstdint>
#include <cstring>
#include <algorithm>
#include <cstdio>

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

// C-callable helpers for the test
extern "C" bool metalIsAvailable();
extern "C" void metalPrintDevices();

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

    // Device (default, else enumerate)
    MTL::Device* dev = MTL::CreateSystemDefaultDevice();
    if (!dev) {
        std::fprintf(stderr, "[MetalDBG] No default device, enumerating…\n");
        auto* arr = MTL::CopyAllDevices();
        if (arr && arr->count() > 0) {
            dev = static_cast<MTL::Device*>(arr->object(0));
            dev->retain(); // retain because arr will be released
        }
        if (arr) arr->release();
    }
    if (!dev) {
        std::fprintf(stderr, "[MetalDBG] FAIL: no Metal device available.\n");
        pool->release();
        return empty;
    }
    // (Don’t release dev->name() — it’s autoreleased)
    std::fprintf(stderr, "[MetalDBG] Step 1: device ok\n");

    MTL::CommandQueue* q = dev->newCommandQueue();
    if (!q) {
        std::fprintf(stderr, "[MetalDBG] FAIL: newCommandQueue returned null\n");
        dev->release(); pool->release(); return empty;
    }
    std::fprintf(stderr, "[MetalDBG] Step 2: command queue ok\n");

    // Compile library (NOTE: NS::String::string returns autoreleased; do NOT release)
    NS::Error* err = nullptr;
    NS::String* src = NS::String::string(kShaderSrc, NS::UTF8StringEncoding);
    MTL::CompileOptions* opts = MTL::CompileOptions::alloc()->init();
    if (dev->supportsFamily(MTL::GPUFamilyApple7)) {
        opts->setLanguageVersion(MTL::LanguageVersion3_0);
    } else {
        opts->setLanguageVersion(MTL::LanguageVersion2_4);
    }
    MTL::Library* lib = dev->newLibrary(src, opts, &err);
    opts->release();
    if (!lib) {
        std::fprintf(stderr, "[MetalDBG] FAIL: newLibrary error: %s\n",
                     err ? err->localizedDescription()->utf8String() : "(null)");
        if (err) err->release();
        q->release(); dev->release(); pool->release(); return empty;
    }
    std::fprintf(stderr, "[MetalDBG] Step 3: library compiled\n");

    // Function/pipeline (NS::String::string is autoreleased; do NOT release)
    NS::String* fnName = NS::String::string("filter_equals", NS::UTF8StringEncoding);
    MTL::Function* fn = lib->newFunction(fnName);
    lib->release();
    if (!fn) {
        std::fprintf(stderr, "[MetalDBG] FAIL: newFunction returned null\n");
        q->release(); dev->release(); pool->release(); return empty;
    }
    std::fprintf(stderr, "[MetalDBG] Step 4: function resolved\n");

    NS::Error* psoErr = nullptr;
    MTL::ComputePipelineState* pso = dev->newComputePipelineState(fn, &psoErr);
    fn->release();
    if (!pso) {
        std::fprintf(stderr, "[MetalDBG] FAIL: newComputePipelineState: %s\n",
                     psoErr ? psoErr->localizedDescription()->utf8String() : "(null)");
        if (psoErr) psoErr->release();
        q->release(); dev->release(); pool->release(); return empty;
    }
    std::fprintf(stderr, "[MetalDBG] Step 5: pipeline ok, maxThreads=%lu\n",
                 (unsigned long)pso->maxTotalThreadsPerThreadgroup());

    const uint32_t n = static_cast<uint32_t>(values.size());
    const size_t valuesBytes = values.size() * sizeof(uint32_t);
    const size_t rowIDsBytes = rowIDs.size() * sizeof(uint32_t);

    // Buffers
    MTL::Buffer* inValues  = dev->newBuffer(valuesBytes, MTL::ResourceStorageModeShared);
    MTL::Buffer* inRowIDs  = dev->newBuffer(rowIDsBytes, MTL::ResourceStorageModeShared);
    MTL::Buffer* outRowIDs = dev->newBuffer(valuesBytes, MTL::ResourceStorageModeShared);
    MTL::Buffer* outCount  = dev->newBuffer(sizeof(uint32_t), MTL::ResourceStorageModeShared);
    if (!inValues || !inRowIDs || !outRowIDs || !outCount) {
        std::fprintf(stderr, "[MetalDBG] FAIL: buffer alloc\n");
        if (outCount) outCount->release();
        if (outRowIDs) outRowIDs->release();
        if (inRowIDs) inRowIDs->release();
        if (inValues) inValues->release();
        pso->release(); q->release(); dev->release(); pool->release(); return empty;
    }
    std::fprintf(stderr, "[MetalDBG] Step 6: buffers ok\n");

    struct Params { uint32_t needle; uint32_t n; } P{needle, n};
    MTL::Buffer* paramsBuf = dev->newBuffer(sizeof(Params), MTL::ResourceStorageModeShared);
    if (!paramsBuf) {
        std::fprintf(stderr, "[MetalDBG] FAIL: params buffer alloc\n");
        outCount->release(); outRowIDs->release(); inRowIDs->release(); inValues->release();
        pso->release(); q->release(); dev->release(); pool->release(); return empty;
    }

    std::memcpy(inValues->contents(), values.data(), valuesBytes);
    std::memcpy(inRowIDs->contents(), rowIDs.data(), rowIDsBytes);
    std::memcpy(paramsBuf->contents(), &P, sizeof(P));
    std::memset(outCount->contents(), 0, sizeof(uint32_t));
    std::fprintf(stderr, "[MetalDBG] Step 7: uploads ok\n");

    // Encode/dispatch
    MTL::CommandBuffer* cb  = q->commandBuffer();            // likely autoreleased
    MTL::ComputeCommandEncoder* enc = cb->computeCommandEncoder(); // likely autoreleased
    if (!cb || !enc) {
        std::fprintf(stderr, "[MetalDBG] FAIL: commandBuffer/encoder null\n");
        paramsBuf->release(); outCount->release(); outRowIDs->release();
        inRowIDs->release(); inValues->release();
        pso->release(); q->release(); dev->release(); pool->release(); return empty;
    }

    enc->setComputePipelineState(pso);
    enc->setBuffer(inValues,  0, 0);
    enc->setBuffer(inRowIDs,  0, 1);
    enc->setBuffer(outRowIDs, 0, 2);
    enc->setBuffer(outCount,  0, 3);
    enc->setBuffer(paramsBuf, 0, 4);

    const uint32_t tgs = std::min<uint32_t>(pso->maxTotalThreadsPerThreadgroup(), 256);
    MTL::Size threadsPerTg = MTL::Size::Make(tgs, 1, 1);
    MTL::Size grid         = MTL::Size::Make(n,   1, 1);
    enc->dispatchThreads(grid, threadsPerTg);
    enc->endEncoding();
    std::fprintf(stderr, "[MetalDBG] Step 8: encoded dispatch (n=%u, tgs=%u)\n", n, tgs);

    cb->commit();
    cb->waitUntilCompleted();
    std::fprintf(stderr, "[MetalDBG] Step 9: GPU completed\n");

    // Read back
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

    // Cleanup
    paramsBuf->release();
    outCount->release();
    outRowIDs->release();
    inRowIDs->release();
    inValues->release();
    pso->release();
    q->release();
    dev->release();
    pool->release();

    return result;
}