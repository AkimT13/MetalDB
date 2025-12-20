// gpu_scan_range.mm
#define NS_PRIVATE_IMPLEMENTATION
#define MTL_PRIVATE_IMPLEMENTATION
#include <../metal-cpp/Metal/Metal.hpp>
#include <../metal-cpp/Foundation/Foundation.hpp>

#include <vector>
#include <cstdint>
#include <cstring>
#include <algorithm>
#include <atomic>
#include <mutex>

static const char* kShaderSrcRange = R"METAL(
#include <metal_stdlib>
using namespace metal;

struct Params {
    uint32_t lo;
    uint32_t hi;
    uint32_t n;
};

kernel void filter_between(device const uint32_t* values    [[buffer(0)]],
                           device const uint32_t* rowIDs    [[buffer(1)]],
                           device uint32_t*       outRowIDs [[buffer(2)]],
                           device atomic_uint*    outCount  [[buffer(3)]],
                           constant Params&       P         [[buffer(4)]],
                           uint gid [[thread_position_in_grid]])
{
    if (gid >= P.n) return;
    uint v = values[gid];
    if (v >= P.lo && v <= P.hi) {
        uint idx = atomic_fetch_add_explicit(outCount, 1, memory_order_relaxed);
        outRowIDs[idx] = rowIDs[gid];
    }
}
)METAL";

// Cache like equals:
namespace {
    static MTL::Device* s_dev = nullptr;
    static MTL::CommandQueue* s_q = nullptr;
    static MTL::ComputePipelineState* s_psoRange = nullptr;
    static std::atomic<bool> s_ready{false};
    static std::mutex s_mu;

    bool ensureRangePipeline() {
        if (s_ready.load(std::memory_order_acquire)) return true;
        std::lock_guard<std::mutex> g(s_mu);
        if (s_ready.load(std::memory_order_relaxed)) return true;

        s_dev = MTL::CreateSystemDefaultDevice();
        if (!s_dev) {
            auto* arr = MTL::CopyAllDevices();
            if (arr && arr->count()) {
                s_dev = static_cast<MTL::Device*>(arr->object(0));
                s_dev->retain();
            }
            if (arr) arr->release();
        }
        if (!s_dev) return false;

        s_q = s_dev->newCommandQueue();
        if (!s_q) return false;

        NS::Error* err = nullptr;
        NS::String* src = NS::String::string(kShaderSrcRange, NS::UTF8StringEncoding);
        auto* opts = MTL::CompileOptions::alloc()->init();
        if (s_dev->supportsFamily(MTL::GPUFamilyApple7)) opts->setLanguageVersion(MTL::LanguageVersion3_0);
        else opts->setLanguageVersion(MTL::LanguageVersion2_4);
        auto* lib = s_dev->newLibrary(src, opts, &err);
        opts->release();
        if (!lib) return false;

        NS::String* fnName = NS::String::string("filter_between", NS::UTF8StringEncoding);
        auto* fn = lib->newFunction(fnName);
        lib->release();
        if (!fn) return false;

        NS::Error* psoErr = nullptr;
        s_psoRange = s_dev->newComputePipelineState(fn, &psoErr);
        fn->release();
        if (!s_psoRange) {
            if (psoErr) psoErr->release();
            return false;
        }

        s_ready.store(true, std::memory_order_release);
        return true;
    }
}

extern "C"
std::vector<uint32_t>
gpuScanBetween(const std::vector<uint32_t>& values,
               const std::vector<uint32_t>& rowIDs,
               uint32_t lo, uint32_t hi)
{
    std::vector<uint32_t> empty;
    if (values.size() != rowIDs.size()) return empty;
    if (!ensureRangePipeline()) return empty;

    using namespace MTL;
    const uint32_t n = static_cast<uint32_t>(values.size());
    const size_t valuesBytes = values.size() * sizeof(uint32_t);
    const size_t rowIDsBytes = rowIDs.size() * sizeof(uint32_t);

    Buffer* inValues  = s_dev->newBuffer(valuesBytes, ResourceStorageModeShared);
    Buffer* inRowIDs  = s_dev->newBuffer(rowIDsBytes, ResourceStorageModeShared);
    Buffer* outRowIDs = s_dev->newBuffer(valuesBytes, ResourceStorageModeShared);
    Buffer* outCount  = s_dev->newBuffer(sizeof(uint32_t), ResourceStorageModeShared);
    struct Params { uint32_t lo, hi, n; } P{lo, hi, n};
    Buffer* paramsBuf = s_dev->newBuffer(sizeof(Params), ResourceStorageModeShared);

    std::memcpy(inValues->contents(), values.data(), valuesBytes);
    std::memcpy(inRowIDs->contents(), rowIDs.data(), rowIDsBytes);
    std::memcpy(paramsBuf->contents(), &P, sizeof(P));
    std::memset(outCount->contents(), 0, sizeof(uint32_t));

    auto* cb  = s_q->commandBuffer();
    auto* enc = cb->computeCommandEncoder();
    enc->setComputePipelineState(s_psoRange);
    enc->setBuffer(inValues,  0, 0);
    enc->setBuffer(inRowIDs,  0, 1);
    enc->setBuffer(outRowIDs, 0, 2);
    enc->setBuffer(outCount,  0, 3);
    enc->setBuffer(paramsBuf, 0, 4);

    const uint32_t tgs = std::min<uint32_t>(s_psoRange->maxTotalThreadsPerThreadgroup(), 256);
    MTL::Size tpg = MTL::Size::Make(tgs, 1, 1);
    MTL::Size grid = MTL::Size::Make(n, 1, 1);
    enc->dispatchThreads(grid, tpg);
    enc->endEncoding();
    cb->commit();
    cb->waitUntilCompleted();

    uint32_t count = *reinterpret_cast<uint32_t*>(outCount->contents());
    if (count > n) count = n;
    std::vector<uint32_t> out(count);
    if (count) std::memcpy(out.data(), outRowIDs->contents(), count * sizeof(uint32_t));

    paramsBuf->release();
    outCount->release();
    outRowIDs->release();
    inRowIDs->release();
    inValues->release();

    return out;
}