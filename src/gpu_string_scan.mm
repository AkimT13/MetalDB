// gpu_string_scan.mm
// GPU string equality scan using a precompiled Metal library (gpu_string_scan.metallib).
// Device, CommandQueue, and PSO are cached for the lifetime of the process.

#include <../metal-cpp/Metal/Metal.hpp>
#include <../metal-cpp/Foundation/Foundation.hpp>

#include "gpu_string_scan.h"
#include "gpu_utils.h"
#include <vector>
#include <string>
#include <cstdint>
#include <cstring>
#include <algorithm>
#include <cstdio>
#include <atomic>
#include <mutex>

extern "C" bool metalIsAvailable();

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

        auto path = metallibPath("gpu_string_scan.metallib");
        NS::String*   pathStr = NS::String::string(path.c_str(), NS::UTF8StringEncoding);
        NS::URL*      url     = NS::URL::fileURLWithPath(pathStr);
        NS::Error*    err     = nullptr;
        MTL::Library* lib     = s_dev->newLibrary(url, &err);
        if (!lib) {
            std::fprintf(stderr, "[StringScan] Failed to load metallib '%s': %s\n",
                         path.c_str(),
                         err ? err->localizedDescription()->utf8String() : "(null)");
            return false;
        }

        NS::String*    fnName = NS::String::string("stringScanEquals", NS::UTF8StringEncoding);
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

// ── gpuStringScanEquals ───────────────────────────────────────────────────────
bool gpuStringScanEquals(
    const std::vector<char>&     chars,
    const std::vector<int32_t>&  offsets,
    const std::vector<uint32_t>& rowIDs,
    const std::string&           needle,
    std::vector<uint32_t>&       outRowIDs)
{
    const uint32_t n = static_cast<uint32_t>(rowIDs.size());
    if (n == 0 || offsets.size() != size_t(n) + 1) return false;

    NS::AutoreleasePool* pool = NS::AutoreleasePool::alloc()->init();
    if (!ensurePipeline()) { pool->release(); return false; }

    // Allocate buffers — use at least 1 byte for chars/needle to avoid zero-size alloc.
    const size_t charsBytes   = std::max(chars.size(), size_t(1));
    const size_t needleBytes  = std::max(needle.size(), size_t(1));
    const size_t offsetsBytes = (n + 1) * sizeof(int32_t);
    const size_t resultsBytes = n * sizeof(uint32_t);

    struct StringScanParams { uint32_t n; int32_t needleLen; };
    StringScanParams P{ n, static_cast<int32_t>(needle.size()) };

    MTL::Buffer* charsBuf   = s_dev->newBuffer(charsBytes,   MTL::ResourceStorageModeShared);
    MTL::Buffer* offsetsBuf = s_dev->newBuffer(offsetsBytes, MTL::ResourceStorageModeShared);
    MTL::Buffer* needleBuf  = s_dev->newBuffer(needleBytes,  MTL::ResourceStorageModeShared);
    MTL::Buffer* paramsBuf  = s_dev->newBuffer(sizeof(P),    MTL::ResourceStorageModeShared);
    MTL::Buffer* resultsBuf = s_dev->newBuffer(resultsBytes, MTL::ResourceStorageModeShared);

    bool allocOk = charsBuf && offsetsBuf && needleBuf && paramsBuf && resultsBuf;
    if (!allocOk) {
        if (charsBuf)   charsBuf->release();
        if (offsetsBuf) offsetsBuf->release();
        if (needleBuf)  needleBuf->release();
        if (paramsBuf)  paramsBuf->release();
        if (resultsBuf) resultsBuf->release();
        pool->release();
        return false;
    }

    if (!chars.empty())
        std::memcpy(charsBuf->contents(),   chars.data(),   chars.size());
    std::memcpy(offsetsBuf->contents(),  offsets.data(), offsetsBytes);
    if (!needle.empty())
        std::memcpy(needleBuf->contents(),  needle.data(),  needle.size());
    std::memcpy(paramsBuf->contents(),   &P, sizeof(P));
    std::memset(resultsBuf->contents(),  0,  resultsBytes);

    MTL::CommandBuffer*         cb  = s_q->commandBuffer();
    MTL::ComputeCommandEncoder* enc = cb->computeCommandEncoder();
    enc->setComputePipelineState(s_pso);
    enc->setBuffer(charsBuf,   0, 0);
    enc->setBuffer(offsetsBuf, 0, 1);
    enc->setBuffer(needleBuf,  0, 2);
    enc->setBuffer(paramsBuf,  0, 3);
    enc->setBuffer(resultsBuf, 0, 4);

    const uint32_t tgs = std::min<uint32_t>(s_pso->maxTotalThreadsPerThreadgroup(), 256);
    enc->dispatchThreads(MTL::Size::Make(n, 1, 1), MTL::Size::Make(tgs, 1, 1));
    enc->endEncoding();
    cb->commit();
    cb->waitUntilCompleted();

    const uint32_t* results = static_cast<const uint32_t*>(resultsBuf->contents());
    outRowIDs.clear();
    for (uint32_t i = 0; i < n; ++i)
        if (results[i]) outRowIDs.push_back(rowIDs[i]);

    charsBuf->release(); offsetsBuf->release();
    needleBuf->release(); paramsBuf->release(); resultsBuf->release();
    pool->release();
    return true;
}
