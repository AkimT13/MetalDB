// gpu_scan_range.mm
// Range scan [lo, hi] using a precompiled Metal library (gpu_scan_range.metallib).
// Device, CommandQueue, and PSO are cached for the lifetime of the process.
//
// NOTE: NS_PRIVATE_IMPLEMENTATION and MTL_PRIVATE_IMPLEMENTATION are defined
// exactly once here for the entire link unit.
#define NS_PRIVATE_IMPLEMENTATION
#define MTL_PRIVATE_IMPLEMENTATION
#include <../metal-cpp/Metal/Metal.hpp>
#include <../metal-cpp/Foundation/Foundation.hpp>

#include "gpu_utils.h"
#include <vector>
#include <cstdint>
#include <cstring>
#include <algorithm>
#include <atomic>
#include <mutex>
#include <cstdio>

// ── Pipeline cache ────────────────────────────────────────────────────────────
namespace {
    MTL::Device*               s_dev = nullptr;
    MTL::CommandQueue*         s_q   = nullptr;
    MTL::ComputePipelineState* s_pso = nullptr;
    std::atomic<bool>          s_ready{false};
    std::mutex                 s_mu;

    bool ensurePipeline() {
        if (s_ready.load(std::memory_order_acquire)) return true;
        std::lock_guard<std::mutex> g(s_mu);
        if (s_ready.load(std::memory_order_relaxed)) return true;

        s_dev = MTL::CreateSystemDefaultDevice();
        if (!s_dev) {
            auto* arr = MTL::CopyAllDevices();
            if (arr && arr->count() > 0) {
                s_dev = static_cast<MTL::Device*>(arr->object(0));
                s_dev->retain();
            }
            if (arr) arr->release();
        }
        if (!s_dev) return false;

        s_q = s_dev->newCommandQueue();
        if (!s_q) return false;

        auto path = metallibPath("gpu_scan_range.metallib");
        NS::String*   pathStr = NS::String::string(path.c_str(), NS::UTF8StringEncoding);
        NS::URL*      url     = NS::URL::fileURLWithPath(pathStr);
        NS::Error*    err     = nullptr;
        MTL::Library* lib     = s_dev->newLibrary(url, &err);
        if (!lib) {
            std::fprintf(stderr, "[ScanRange] Failed to load metallib '%s': %s\n",
                         path.c_str(),
                         err ? err->localizedDescription()->utf8String() : "(null)");
            return false;
        }

        NS::String*    fnName = NS::String::string("filter_between", NS::UTF8StringEncoding);
        MTL::Function* fn     = lib->newFunction(fnName);
        lib->release();
        if (!fn) return false;

        NS::Error* psoErr = nullptr;
        s_pso = s_dev->newComputePipelineState(fn, &psoErr);
        fn->release();
        if (!s_pso) { if (psoErr) psoErr->release(); return false; }

        s_ready.store(true, std::memory_order_release);
        return true;
    }
} // namespace

// ── gpuScanBetween ────────────────────────────────────────────────────────────
extern "C"
std::vector<uint32_t>
gpuScanBetween(const std::vector<uint32_t>& values,
               const std::vector<uint32_t>& rowIDs,
               uint32_t lo, uint32_t hi)
{
    std::vector<uint32_t> empty;
    if (values.size() != rowIDs.size()) return empty;

    NS::AutoreleasePool* pool = NS::AutoreleasePool::alloc()->init();
    if (!ensurePipeline()) { pool->release(); return empty; }

    const uint32_t n     = static_cast<uint32_t>(values.size());
    const size_t   vbytes = n * sizeof(uint32_t);

    MTL::Buffer* inValues  = s_dev->newBuffer(vbytes, MTL::ResourceStorageModeShared);
    MTL::Buffer* inRowIDs  = s_dev->newBuffer(vbytes, MTL::ResourceStorageModeShared);
    MTL::Buffer* outRowIDs = s_dev->newBuffer(vbytes, MTL::ResourceStorageModeShared);
    MTL::Buffer* outCount  = s_dev->newBuffer(sizeof(uint32_t), MTL::ResourceStorageModeShared);
    struct Params { uint32_t lo, hi, n; } P{lo, hi, n};
    MTL::Buffer* paramsBuf = s_dev->newBuffer(sizeof(Params), MTL::ResourceStorageModeShared);

    std::memcpy(inValues->contents(),  values.data(), vbytes);
    std::memcpy(inRowIDs->contents(),  rowIDs.data(), vbytes);
    std::memcpy(paramsBuf->contents(), &P, sizeof(P));
    std::memset(outCount->contents(),  0, sizeof(uint32_t));

    MTL::CommandBuffer*         cb  = s_q->commandBuffer();
    MTL::ComputeCommandEncoder* enc = cb->computeCommandEncoder();
    enc->setComputePipelineState(s_pso);
    enc->setBuffer(inValues,  0, 0); enc->setBuffer(inRowIDs,  0, 1);
    enc->setBuffer(outRowIDs, 0, 2); enc->setBuffer(outCount,  0, 3);
    enc->setBuffer(paramsBuf, 0, 4);
    const uint32_t tgs = std::min<uint32_t>(s_pso->maxTotalThreadsPerThreadgroup(), 256);
    enc->dispatchThreads(MTL::Size::Make(n, 1, 1), MTL::Size::Make(tgs, 1, 1));
    enc->endEncoding();
    cb->commit();
    cb->waitUntilCompleted();

    uint32_t count = *static_cast<uint32_t*>(outCount->contents());
    if (count > n) count = n;
    std::vector<uint32_t> out(count);
    if (count) std::memcpy(out.data(), outRowIDs->contents(), count * sizeof(uint32_t));

    paramsBuf->release(); outCount->release();
    outRowIDs->release(); inRowIDs->release(); inValues->release();
    pool->release();
    return out;
}
