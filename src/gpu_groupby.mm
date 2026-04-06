// gpu_groupby.mm
// Single-pass GPU group-by (count + sum) using a precompiled metallib.
// Device, CommandQueue, and PSO are cached for the lifetime of the process.
//
// Limitation: bucketSums uses atomic_uint (32-bit). Per-group sums overflow
// if they exceed ~4.29 billion. Metal does not support atomic_fetch_add on
// device atomic_ulong (64-bit) on current Apple hardware.

#include <../metal-cpp/Metal/Metal.hpp>
#include <../metal-cpp/Foundation/Foundation.hpp>

#include "gpu_groupby.h"
#include "gpu_utils.h"
#include <cstring>
#include <cstdio>
#include <algorithm>
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

        if (!s_dev->supportsFamily(MTL::GPUFamilyApple7)) {
            std::fprintf(stderr, "[GroupBy] Device does not support GPUFamilyApple7 — falling back to CPU\n");
            return false;
        }

        s_q = s_dev->newCommandQueue();
        if (!s_q) return false;

        auto path = metallibPath("gpu_groupby.metallib");
        NS::String*   pathStr = NS::String::string(path.c_str(), NS::UTF8StringEncoding);
        NS::URL*      url     = NS::URL::fileURLWithPath(pathStr);
        NS::Error*    err     = nullptr;
        MTL::Library* lib     = s_dev->newLibrary(url, &err);
        if (!lib) {
            std::fprintf(stderr, "[GroupBy] Failed to load metallib '%s': %s\n",
                         path.c_str(),
                         err ? err->localizedDescription()->utf8String() : "(null)");
            return false;
        }

        NS::String*    fnName = NS::String::string("group_by", NS::UTF8StringEncoding);
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

// ── gpuGroupByCountSum ────────────────────────────────────────────────────────
bool gpuGroupByCountSum(
    const std::vector<uint32_t>& keys,
    const std::vector<uint32_t>& vals,
    uint32_t numBuckets,
    std::unordered_map<uint32_t, uint64_t>& outCount,
    std::unordered_map<uint32_t, uint64_t>& outSum)
{
    if (keys.empty()) return true;
    if (!metalIsAvailable()) return false;

    NS::AutoreleasePool* pool = NS::AutoreleasePool::alloc()->init();
    if (!ensurePipeline()) { pool->release(); return false; }

    const uint32_t n  = static_cast<uint32_t>(keys.size());
    const uint32_t nb = numBuckets;

    MTL::Buffer* keyBuf = s_dev->newBuffer(n  * sizeof(uint32_t), MTL::ResourceStorageModeShared);
    MTL::Buffer* valBuf = s_dev->newBuffer(n  * sizeof(uint32_t), MTL::ResourceStorageModeShared);
    MTL::Buffer* kBuf   = s_dev->newBuffer(nb * sizeof(uint32_t), MTL::ResourceStorageModeShared);
    MTL::Buffer* cBuf   = s_dev->newBuffer(nb * sizeof(uint32_t), MTL::ResourceStorageModeShared);
    MTL::Buffer* sBuf   = s_dev->newBuffer(nb * sizeof(uint32_t), MTL::ResourceStorageModeShared);

    if (!keyBuf || !valBuf || !kBuf || !cBuf || !sBuf) {
        if (keyBuf) keyBuf->release(); if (valBuf) valBuf->release();
        if (kBuf)   kBuf->release();   if (cBuf)   cBuf->release();
        if (sBuf)   sBuf->release();
        pool->release(); return false;
    }

    std::memcpy(keyBuf->contents(), keys.data(), n * sizeof(uint32_t));
    std::memcpy(valBuf->contents(), vals.data(), n * sizeof(uint32_t));
    std::memset(kBuf->contents(), 0xFF, nb * sizeof(uint32_t)); // EMPTY_KEY
    std::memset(cBuf->contents(), 0x00, nb * sizeof(uint32_t));
    std::memset(sBuf->contents(), 0x00, nb * sizeof(uint32_t));

    MTL::CommandBuffer*         cb  = s_q->commandBuffer();
    MTL::ComputeCommandEncoder* enc = cb->computeCommandEncoder();
    enc->setComputePipelineState(s_pso);
    enc->setBuffer(keyBuf, 0, 0); enc->setBuffer(valBuf, 0, 1);
    enc->setBytes(&n,  sizeof(n),  2);
    enc->setBuffer(kBuf, 0, 3);   enc->setBuffer(cBuf, 0, 4);
    enc->setBuffer(sBuf, 0, 5);
    enc->setBytes(&nb, sizeof(nb), 6);

    const uint32_t tg = std::min<uint32_t>(
        static_cast<uint32_t>(s_pso->maxTotalThreadsPerThreadgroup()), 256u);
    enc->dispatchThreads(MTL::Size::Make(n, 1, 1), MTL::Size::Make(tg, 1, 1));
    enc->endEncoding();
    cb->commit();
    cb->waitUntilCompleted();

    const uint32_t* rKeys   = static_cast<const uint32_t*>(kBuf->contents());
    const uint32_t* rCounts = static_cast<const uint32_t*>(cBuf->contents());
    const uint32_t* rSums   = static_cast<const uint32_t*>(sBuf->contents());

    for (uint32_t i = 0; i < nb; ++i) {
        if (rKeys[i] != 0xFFFFFFFFu) {
            outCount[rKeys[i]] += rCounts[i];
            outSum  [rKeys[i]] += rSums[i];
        }
    }

    sBuf->release(); cBuf->release(); kBuf->release();
    valBuf->release(); keyBuf->release();
    pool->release();
    return true;
}
