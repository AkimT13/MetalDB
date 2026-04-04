// gpu_groupby.mm
// Single-pass GPU group-by (count + sum) using device-space atomics.
// Requires Metal 3.0 (GPUFamilyApple7+, i.e. M1 and later).
// On older GPUs the function returns false and the caller falls back to CPU.

#include <../metal-cpp/Metal/Metal.hpp>
#include <../metal-cpp/Foundation/Foundation.hpp>

#include "gpu_groupby.h"
#include <cstring>
#include <cstdio>
#include <algorithm>

#ifndef GBDBG
#define GBDBG 0
#endif
#define GBLOG(...) do { if (GBDBG) std::fprintf(stderr, "[GroupByGPU] " __VA_ARGS__); } while(0)

// Note: device atomic_ulong fetch_add is not available in all Metal versions.
// We use atomic_uint (32-bit) for both counts and sums.
// Sums overflow if per-group sum exceeds ~4.29 billion — caller should be
// aware of this limitation and fall back to CPU for large-value columns.
static const char* kGroupByKernel = R"METAL(
#include <metal_stdlib>
using namespace metal;

constant uint32_t EMPTY_KEY = 0xFFFFFFFFu;

kernel void group_by(
    device const uint32_t*   keys        [[buffer(0)]],
    device const uint32_t*   vals        [[buffer(1)]],
    constant uint32_t&       n           [[buffer(2)]],
    device atomic_uint*      bucketKeys  [[buffer(3)]],
    device atomic_uint*      bucketCnts  [[buffer(4)]],
    device atomic_uint*      bucketSums  [[buffer(5)]],  // 32-bit partial sums
    constant uint32_t&       numBuckets  [[buffer(6)]],
    uint gid [[thread_position_in_grid]])
{
    if (gid >= n) return;
    uint32_t key = keys[gid];
    uint32_t val = vals[gid];
    uint32_t slot = (key * 2654435761u) % numBuckets;

    for (uint32_t probe = 0; probe < numBuckets; ++probe) {
        uint32_t cur = atomic_load_explicit(&bucketKeys[slot], memory_order_relaxed);
        if (cur == key) {
            atomic_fetch_add_explicit(&bucketCnts[slot], 1u, memory_order_relaxed);
            atomic_fetch_add_explicit(&bucketSums[slot], val, memory_order_relaxed);
            return;
        }
        if (cur == EMPTY_KEY) {
            uint32_t expected = EMPTY_KEY;
            bool claimed = atomic_compare_exchange_weak_explicit(
                &bucketKeys[slot], &expected, key,
                memory_order_relaxed, memory_order_relaxed);
            if (claimed || expected == key) {
                atomic_fetch_add_explicit(&bucketCnts[slot], 1u, memory_order_relaxed);
                atomic_fetch_add_explicit(&bucketSums[slot], val, memory_order_relaxed);
                return;
            }
        }
        slot = (slot + 1u) % numBuckets;
    }
}
)METAL";

extern "C" bool metalIsAvailable();

bool gpuGroupByCountSum(
    const std::vector<uint32_t>& keys,
    const std::vector<uint32_t>& vals,
    uint32_t numBuckets,
    std::unordered_map<uint32_t, uint64_t>& outCount,
    std::unordered_map<uint32_t, uint64_t>& outSum)
{
    using namespace MTL;
    using namespace NS;

    if (keys.empty()) return true;
    if (!metalIsAvailable()) return false;

    AutoreleasePool* pool = AutoreleasePool::alloc()->init();

    Device* dev = CreateSystemDefaultDevice();
    if (!dev) {
        auto* arr = CopyAllDevices();
        if (arr && arr->count() > 0) {
            dev = static_cast<Device*>(arr->object(0)); dev->retain();
        }
        if (arr) arr->release();
    }
    if (!dev) { pool->release(); return false; }

    // Require Metal 3.0 (device atomics with atomic_ulong)
    if (!dev->supportsFamily(GPUFamilyApple7)) {
        GBLOG("Device does not support GPUFamilyApple7 — falling back to CPU\n");
        dev->release(); pool->release(); return false;
    }

    CommandQueue* q = dev->newCommandQueue();
    if (!q) { dev->release(); pool->release(); return false; }

    Error* err = nullptr;
    String* src = String::string(kGroupByKernel, UTF8StringEncoding);
    CompileOptions* opts = CompileOptions::alloc()->init();
    opts->setLanguageVersion(LanguageVersion3_0);
    Library* lib = dev->newLibrary(src, opts, &err);
    opts->release();
    if (!lib) {
        GBLOG("Compile failed: %s\n",
              err ? err->localizedDescription()->utf8String() : "(null)");
        if (err) err->release();
        q->release(); dev->release(); pool->release(); return false;
    }

    String* fname = String::string("group_by", UTF8StringEncoding);
    Function* fn = lib->newFunction(fname);
    lib->release();
    if (!fn) { q->release(); dev->release(); pool->release(); return false; }

    Error* pErr = nullptr;
    ComputePipelineState* pso = dev->newComputePipelineState(fn, &pErr);
    fn->release();
    if (!pso) {
        if (pErr) pErr->release();
        q->release(); dev->release(); pool->release(); return false;
    }

    const uint32_t n = static_cast<uint32_t>(keys.size());

    // Input buffers
    Buffer* keyBuf = dev->newBuffer(n * sizeof(uint32_t), ResourceStorageModeShared);
    Buffer* valBuf = dev->newBuffer(n * sizeof(uint32_t), ResourceStorageModeShared);
    if (!keyBuf || !valBuf) {
        if (keyBuf) keyBuf->release();
        if (valBuf) valBuf->release();
        pso->release(); q->release(); dev->release(); pool->release(); return false;
    }
    std::memcpy(keyBuf->contents(), keys.data(), n * sizeof(uint32_t));
    std::memcpy(valBuf->contents(), vals.data(), n * sizeof(uint32_t));

    // Hash-table output buffers (initialised to empty / zero)
    const uint32_t nb = numBuckets;
    Buffer* kBuf = dev->newBuffer(nb * sizeof(uint32_t), ResourceStorageModeShared);
    Buffer* cBuf = dev->newBuffer(nb * sizeof(uint32_t), ResourceStorageModeShared);
    Buffer* sBuf = dev->newBuffer(nb * sizeof(uint32_t), ResourceStorageModeShared);
    if (!kBuf || !cBuf || !sBuf) {
        if (kBuf) kBuf->release(); if (cBuf) cBuf->release(); if (sBuf) sBuf->release();
        valBuf->release(); keyBuf->release();
        pso->release(); q->release(); dev->release(); pool->release(); return false;
    }
    // Fill bucket keys with EMPTY_KEY (0xFFFFFFFF)
    std::memset(kBuf->contents(), 0xFF, nb * sizeof(uint32_t));
    std::memset(cBuf->contents(), 0x00, nb * sizeof(uint32_t));
    std::memset(sBuf->contents(), 0x00, nb * sizeof(uint32_t));

    // Dispatch
    {
        CommandBuffer* cb  = q->commandBuffer();
        ComputeCommandEncoder* enc = cb->computeCommandEncoder();

        enc->setComputePipelineState(pso);
        enc->setBuffer(keyBuf, 0, 0);
        enc->setBuffer(valBuf, 0, 1);
        enc->setBytes(&n,  sizeof(n),  2);
        enc->setBuffer(kBuf, 0, 3);
        enc->setBuffer(cBuf, 0, 4);
        enc->setBuffer(sBuf, 0, 5);
        enc->setBytes(&nb, sizeof(nb), 6);

        const uint32_t tg = std::min<uint32_t>(
            static_cast<uint32_t>(pso->maxTotalThreadsPerThreadgroup()), 256u);
        MTL::Size grid = MTL::Size::Make(n, 1, 1);
        MTL::Size tpg  = MTL::Size::Make(tg, 1, 1);
        enc->dispatchThreads(grid, tpg);
        enc->endEncoding();
        cb->commit();
        cb->waitUntilCompleted();
    }

    // Read back results
    const uint32_t* rKeys   = static_cast<const uint32_t*>(kBuf->contents());
    const uint32_t* rCounts = static_cast<const uint32_t*>(cBuf->contents());
    const uint32_t* rSums   = static_cast<const uint32_t*>(sBuf->contents());

    for (uint32_t i = 0; i < nb; ++i) {
        if (rKeys[i] != 0xFFFFFFFFu) {
            outCount[rKeys[i]] += rCounts[i];
            outSum  [rKeys[i]] += rSums[i];
        }
    }

    // Cleanup
    sBuf->release(); cBuf->release(); kBuf->release();
    valBuf->release(); keyBuf->release();
    pso->release(); q->release(); dev->release();
    pool->release();
    return true;
}
