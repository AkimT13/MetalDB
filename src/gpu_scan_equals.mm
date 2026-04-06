// gpu_scan_equals.mm
// Equality scan using a precompiled Metal library (gpu_scan_equals.metallib).
// Device, CommandQueue, and PSO are cached for the lifetime of the process.

#include <../metal-cpp/Metal/Metal.hpp>
#include <../metal-cpp/Foundation/Foundation.hpp>

#include "gpu_utils.h"
#include <vector>
#include <cstdint>
#include <cstring>
#include <algorithm>
#include <cstdio>
#include <atomic>
#include <mutex>

extern "C" bool metalIsAvailable();
extern "C" void metalPrintDevices();

// ── Pipeline cache ────────────────────────────────────────────────────────────
namespace {
    MTL::Device*               s_dev  = nullptr;
    MTL::CommandQueue*         s_q    = nullptr;
    MTL::ComputePipelineState* s_pso  = nullptr;
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

        auto path = metallibPath("gpu_scan_equals.metallib");
        NS::String*   pathStr = NS::String::string(path.c_str(), NS::UTF8StringEncoding);
        NS::URL*      url     = NS::URL::fileURLWithPath(pathStr);
        NS::Error*    err     = nullptr;
        MTL::Library* lib     = s_dev->newLibrary(url, &err);
        if (!lib) {
            std::fprintf(stderr, "[ScanEquals] Failed to load metallib '%s': %s\n",
                         path.c_str(),
                         err ? err->localizedDescription()->utf8String() : "(null)");
            return false;
        }

        NS::String*    fnName = NS::String::string("filter_equals", NS::UTF8StringEncoding);
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

// ── Public helpers ────────────────────────────────────────────────────────────
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
    if (!arr) { pool->release(); return; }
    for (NS::UInteger i = 0; i < arr->count(); ++i) {
        auto* d = static_cast<MTL::Device*>(arr->object(i));
        std::fprintf(stderr, "  #%lu: %s\n", (unsigned long)i,
                     d->name() ? d->name()->utf8String() : "(null)");
    }
    arr->release();
    pool->release();
}

// ── gpuScanEquals ─────────────────────────────────────────────────────────────
std::vector<uint32_t>
gpuScanEquals(const std::vector<uint32_t>& values,
              const std::vector<uint32_t>& rowIDs,
              uint32_t needle)
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
    struct Params { uint32_t needle, n; } P{needle, n};
    MTL::Buffer* paramsBuf = s_dev->newBuffer(sizeof(Params), MTL::ResourceStorageModeShared);

    if (!inValues || !inRowIDs || !outRowIDs || !outCount || !paramsBuf) {
        if (inValues)  inValues->release();  if (inRowIDs)  inRowIDs->release();
        if (outRowIDs) outRowIDs->release(); if (outCount)  outCount->release();
        if (paramsBuf) paramsBuf->release();
        pool->release(); return empty;
    }

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
    std::vector<uint32_t> result(count);
    if (count) std::memcpy(result.data(), outRowIDs->contents(), count * sizeof(uint32_t));

    paramsBuf->release(); outCount->release();
    outRowIDs->release(); inRowIDs->release(); inValues->release();
    pool->release();
    return result;
}
