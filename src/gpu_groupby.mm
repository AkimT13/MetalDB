// gpu_groupby.mm
// Single-pass GPU group-by (count + sum) using a precompiled metallib.
// Device, CommandQueue, and PSO are cached for the lifetime of the process.
//
// Per-group sums are 64-bit: the kernel keeps (lo, hi) 32-bit atomic pairs with
// carry propagation, since Metal has no 64-bit atomic add on device memory.
// If the hash table overflows, the pass is retried with more buckets; keys equal
// to the 0xFFFFFFFF empty-slot sentinel are aggregated on the CPU.

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
namespace {

uint32_t nextPow2(uint64_t v) {
    uint64_t b = 1;
    while (b < v && b < (1ull << 31)) b <<= 1;
    return static_cast<uint32_t>(b);
}

// One kernel pass over `keys`/`vals` with `nb` buckets. Returns false on Metal
// failure; sets `overflowed` if some row could not be placed (retry larger).
bool runPass(const std::vector<uint32_t>& keys, const std::vector<uint32_t>& vals, uint32_t nb,
             bool& overflowed,
             std::unordered_map<uint32_t, uint64_t>& outCount,
             std::unordered_map<uint32_t, uint64_t>& outSum)
{
    const uint32_t n = static_cast<uint32_t>(keys.size());
    MTL::Buffer* keyBuf = s_dev->newBuffer(size_t(n)  * sizeof(uint32_t), MTL::ResourceStorageModeShared);
    MTL::Buffer* valBuf = s_dev->newBuffer(size_t(n)  * sizeof(uint32_t), MTL::ResourceStorageModeShared);
    MTL::Buffer* kBuf   = s_dev->newBuffer(size_t(nb) * sizeof(uint32_t), MTL::ResourceStorageModeShared);
    MTL::Buffer* cBuf   = s_dev->newBuffer(size_t(nb) * sizeof(uint32_t), MTL::ResourceStorageModeShared);
    MTL::Buffer* loBuf  = s_dev->newBuffer(size_t(nb) * sizeof(uint32_t), MTL::ResourceStorageModeShared);
    MTL::Buffer* hiBuf  = s_dev->newBuffer(size_t(nb) * sizeof(uint32_t), MTL::ResourceStorageModeShared);
    MTL::Buffer* ovBuf  = s_dev->newBuffer(sizeof(uint32_t), MTL::ResourceStorageModeShared);
    MTL::Buffer* all[] = {keyBuf, valBuf, kBuf, cBuf, loBuf, hiBuf, ovBuf};
    auto releaseAll = [&] { for (auto* b : all) if (b) b->release(); };
    for (auto* b : all) if (!b) { releaseAll(); return false; }

    std::memcpy(keyBuf->contents(), keys.data(), size_t(n) * sizeof(uint32_t));
    std::memcpy(valBuf->contents(), vals.data(), size_t(n) * sizeof(uint32_t));
    std::memset(kBuf->contents(),  0xFF, size_t(nb) * sizeof(uint32_t));  // EMPTY_KEY
    std::memset(cBuf->contents(),  0x00, size_t(nb) * sizeof(uint32_t));
    std::memset(loBuf->contents(), 0x00, size_t(nb) * sizeof(uint32_t));
    std::memset(hiBuf->contents(), 0x00, size_t(nb) * sizeof(uint32_t));
    std::memset(ovBuf->contents(), 0x00, sizeof(uint32_t));

    MTL::CommandBuffer*         cb  = s_q->commandBuffer();
    MTL::ComputeCommandEncoder* enc = cb->computeCommandEncoder();
    enc->setComputePipelineState(s_pso);
    enc->setBuffer(keyBuf, 0, 0);
    enc->setBuffer(valBuf, 0, 1);
    enc->setBytes(&n, sizeof(n), 2);
    enc->setBuffer(kBuf, 0, 3);
    enc->setBuffer(cBuf, 0, 4);
    enc->setBuffer(loBuf, 0, 5);
    enc->setBytes(&nb, sizeof(nb), 6);
    enc->setBuffer(hiBuf, 0, 7);
    enc->setBuffer(ovBuf, 0, 8);

    const uint32_t tg = std::min<uint32_t>(
        static_cast<uint32_t>(s_pso->maxTotalThreadsPerThreadgroup()), 256u);
    enc->dispatchThreads(MTL::Size::Make(n, 1, 1), MTL::Size::Make(tg, 1, 1));
    enc->endEncoding();
    cb->commit();
    cb->waitUntilCompleted();
    const bool ok = cb->status() == MTL::CommandBufferStatusCompleted;

    overflowed = *static_cast<const uint32_t*>(ovBuf->contents()) != 0;
    if (ok && !overflowed) {
        const uint32_t* rKeys = static_cast<const uint32_t*>(kBuf->contents());
        const uint32_t* rCnt  = static_cast<const uint32_t*>(cBuf->contents());
        const uint32_t* rLo   = static_cast<const uint32_t*>(loBuf->contents());
        const uint32_t* rHi   = static_cast<const uint32_t*>(hiBuf->contents());
        for (uint32_t i = 0; i < nb; ++i) {
            if (rKeys[i] == 0xFFFFFFFFu) continue;
            outCount[rKeys[i]] += rCnt[i];
            outSum[rKeys[i]]   += (uint64_t(rHi[i]) << 32) | rLo[i];
        }
    }
    releaseAll();
    return ok;
}

}  // namespace

bool gpuGroupByCountSum(
    const std::vector<uint32_t>& keysIn,
    const std::vector<uint32_t>& valsIn,
    uint32_t numBuckets,
    std::unordered_map<uint32_t, uint64_t>& outCount,
    std::unordered_map<uint32_t, uint64_t>& outSum)
{
    if (keysIn.empty()) return true;
    if (keysIn.size() != valsIn.size() || keysIn.size() > 0xFFFFFFFFull) return false;
    if (!metalIsAvailable()) return false;

    NS::AutoreleasePool* pool = NS::AutoreleasePool::alloc()->init();
    if (!ensurePipeline()) { pool->release(); return false; }

    // 0xFFFFFFFF is the kernel's empty-slot sentinel: aggregate those rows on the
    // CPU and give the GPU everything else (copying only when such keys exist).
    const std::vector<uint32_t>* keys = &keysIn;
    const std::vector<uint32_t>* vals = &valsIn;
    std::vector<uint32_t> fk, fv;
    uint64_t sentinelCount = 0, sentinelSum = 0;
    if (std::find(keysIn.begin(), keysIn.end(), 0xFFFFFFFFu) != keysIn.end()) {
        fk.reserve(keysIn.size());
        fv.reserve(valsIn.size());
        for (size_t i = 0; i < keysIn.size(); ++i) {
            if (keysIn[i] == 0xFFFFFFFFu) {
                ++sentinelCount;
                sentinelSum += valsIn[i];
            } else {
                fk.push_back(keysIn[i]);
                fv.push_back(valsIn[i]);
            }
        }
        keys = &fk;
        vals = &fv;
    }

    std::unordered_map<uint32_t, uint64_t> cnt, sum;
    bool ok = true;
    if (!keys->empty()) {
        // Start small (cheap for low-cardinality keys) and grow 8x on overflow, up
        // to 2x the row count, which always fits every distinct key.
        const uint32_t maxBuckets = nextPow2(std::max<uint64_t>(2 * uint64_t(keys->size()), 1024));
        uint32_t nb = std::min(nextPow2(std::max<uint32_t>(numBuckets, 1024)), maxBuckets);
        nb = std::min(nb, nextPow2(1u << 16));
        while (true) {
            bool overflowed = false;
            cnt.clear();
            sum.clear();
            ok = runPass(*keys, *vals, nb, overflowed, cnt, sum);
            if (!ok || !overflowed) break;
            if (nb >= maxBuckets) { ok = false; break; }  // cannot happen at load factor <= 0.5
            nb = std::min<uint64_t>(uint64_t(nb) * 8, maxBuckets);
        }
    }
    if (ok) {
        for (const auto& [k, c] : cnt) outCount[k] += c;
        for (const auto& [k, s] : sum) outSum[k] += s;
        if (sentinelCount) {
            outCount[0xFFFFFFFFu] += sentinelCount;
            outSum[0xFFFFFFFFu]   += sentinelSum;
        }
    }
    pool->release();
    return ok;
}
