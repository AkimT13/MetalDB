// gpu_sum.mm
// GPU reduction (sum of uint32 values). Two-pass: block partials, then reduce.
// Debug-safe version with checkpoints and guards. No NS_/MTL_ PRIVATE macros here.

#include <../metal-cpp/Metal/Metal.hpp>
#include <../metal-cpp/Foundation/Foundation.hpp>

#include <vector>
#include <cstdint>
#include <cstring>
#include <algorithm>
#include <cstdio>

#ifndef SUMDBG
#define SUMDBG 1   // set to 0 to silence
#endif

static const char* kSumKernels = R"METAL(
#include <metal_stdlib>
using namespace metal;

kernel void sum_partials(device const uint32_t* values   [[buffer(0)]],
                         constant uint32_t&     n        [[buffer(1)]],
                         device uint64_t*       partials [[buffer(2)]],
                         uint gid   [[thread_position_in_grid]],
                         uint lane  [[thread_index_in_threadgroup]],
                         uint tgSz  [[threads_per_threadgroup]],
                         threadgroup uint64_t* shmem)
{
    uint64_t v = (gid < n) ? (uint64_t)values[gid] : 0;
    shmem[lane] = v;
    threadgroup_barrier(mem_flags::mem_threadgroup);

    for (uint stride = tgSz >> 1; stride > 0; stride >>= 1) {
        if (lane < stride) {
            shmem[lane] += shmem[lane + stride];
        }
        threadgroup_barrier(mem_flags::mem_threadgroup);
    }

    if (lane == 0) {
        uint groupIdx = gid / tgSz;
        partials[groupIdx] = shmem[0];
    }
}

kernel void sum_reduce_partials(device const uint64_t* partials [[buffer(0)]],
                                constant uint32_t&     pCount   [[buffer(1)]],
                                device uint64_t*       out      [[buffer(2)]],
                                uint gid [[thread_position_in_grid]])
{
    if (gid == 0) {
        uint64_t s = 0;
        for (uint i = 0; i < pCount; ++i) s += partials[i];
        out[0] = s;
    }
}
)METAL";

// metalIsAvailable is implemented in gpu_scan_equals.mm
extern "C" bool metalIsAvailable();

uint64_t gpuSumU32(const std::vector<uint32_t>& values) {
    using namespace MTL;
    using namespace NS;

    if (values.empty()) {
    #if SUMDBG
        std::fprintf(stderr, "[SumDBG] empty input → 0\n");
    #endif
        return 0;
    }
    if (!metalIsAvailable()) {
    #if SUMDBG
        std::fprintf(stderr, "[SumDBG] Metal unavailable → 0\n");
    #endif
        return 0;
    }

    AutoreleasePool* pool = AutoreleasePool::alloc()->init();
    std::fprintf(stderr, "[SumDBG] Step 0: begin, n=%zu\n", values.size());

    // Device
    Device* dev = CreateSystemDefaultDevice();
    if (!dev) {
        auto* arr = CopyAllDevices();
        if (arr && arr->count() > 0) { dev = static_cast<Device*>(arr->object(0)); dev->retain(); }
        if (arr) arr->release();
    }
    if (!dev) {
        std::fprintf(stderr, "[SumDBG] FAIL: no device\n");
        pool->release(); return 0;
    }
    std::fprintf(stderr, "[SumDBG] Step 1: device ok\n");

    CommandQueue* q = dev->newCommandQueue();
    if (!q) {
        std::fprintf(stderr, "[SumDBG] FAIL: newCommandQueue null\n");
        dev->release(); pool->release(); return 0;
    }
    std::fprintf(stderr, "[SumDBG] Step 2: queue ok\n");

    // Compile kernels
    Error* err = nullptr;
    String* src = String::string(kSumKernels, UTF8StringEncoding); // autoreleased
    CompileOptions* opts = CompileOptions::alloc()->init();
    if (dev->supportsFamily(GPUFamilyApple7)) opts->setLanguageVersion(LanguageVersion3_0);
    else                                      opts->setLanguageVersion(LanguageVersion2_4);
    Library* lib = dev->newLibrary(src, opts, &err);
    opts->release();
    if (!lib) {
        std::fprintf(stderr, "[SumDBG] FAIL: newLibrary: %s\n",
                     err ? err->localizedDescription()->utf8String() : "(null)");
        if (err) err->release();
        q->release(); dev->release(); pool->release(); return 0;
    }
    std::fprintf(stderr, "[SumDBG] Step 3: library ok\n");

    String* f1n = String::string("sum_partials", UTF8StringEncoding);          // autoreleased
    String* f2n = String::string("sum_reduce_partials", UTF8StringEncoding);   // autoreleased
    Function* f1 = lib->newFunction(f1n);
    Function* f2 = lib->newFunction(f2n);
    lib->release();
    if (!f1 || !f2) {
        std::fprintf(stderr, "[SumDBG] FAIL: newFunction null (f1=%p f2=%p)\n", (void*)f1, (void*)f2);
        if (f1) f1->release();
        if (f2) f2->release();
        q->release(); dev->release(); pool->release(); return 0;
    }
    std::fprintf(stderr, "[SumDBG] Step 4: functions ok\n");

    Error* pErr = nullptr;
    ComputePipelineState* pso1 = dev->newComputePipelineState(f1, &pErr);
    if (!pso1) {
        std::fprintf(stderr, "[SumDBG] FAIL: pso1: %s\n", pErr ? pErr->localizedDescription()->utf8String() : "(null)");
        if (pErr) pErr->release();
        f1->release(); f2->release(); q->release(); dev->release(); pool->release(); return 0;
    }
    pErr = nullptr;
    ComputePipelineState* pso2 = dev->newComputePipelineState(f2, &pErr);
    if (!pso2) {
        std::fprintf(stderr, "[SumDBG] FAIL: pso2: %s\n", pErr ? pErr->localizedDescription()->utf8String() : "(null)");
        if (pErr) pErr->release();
        pso1->release(); f1->release(); f2->release(); q->release(); dev->release(); pool->release(); return 0;
    }
    f1->release(); f2->release();
    std::fprintf(stderr, "[SumDBG] Step 5: pipelines ok (maxThreads=%lu)\n",
                 (unsigned long)pso1->maxTotalThreadsPerThreadgroup());

    const uint32_t n = static_cast<uint32_t>(values.size());
    const size_t bytes = size_t(n) * sizeof(uint32_t);

    Buffer* in = dev->newBuffer(bytes, ResourceStorageModeShared);
    if (!in) {
        std::fprintf(stderr, "[SumDBG] FAIL: in buffer alloc\n");
        pso2->release(); pso1->release(); q->release(); dev->release(); pool->release(); return 0;
    }
    std::memcpy(in->contents(), values.data(), bytes);

    const uint32_t tg     = std::min<uint32_t>(pso1->maxTotalThreadsPerThreadgroup(), 256);
    const uint32_t groups = (n + tg - 1) / tg;

    Buffer* partials = dev->newBuffer(std::max<uint32_t>(groups,1) * sizeof(uint64_t), ResourceStorageModeShared);
    Buffer* out      = dev->newBuffer(sizeof(uint64_t), ResourceStorageModeShared);
    if (!partials || !out) {
        std::fprintf(stderr, "[SumDBG] FAIL: partials/out buffer alloc\n");
        if (out) out->release();
        if (partials) partials->release();
        in->release(); pso2->release(); pso1->release(); q->release(); dev->release(); pool->release(); return 0;
    }
    std::memset(out->contents(), 0, sizeof(uint64_t));
    std::fprintf(stderr, "[SumDBG] Step 6: buffers ok (n=%u, tg=%u, groups=%u)\n", n, tg, groups);

    // Pass 1
    {
        CommandBuffer* cb  = q->commandBuffer();
        if (!cb) {
            std::fprintf(stderr, "[SumDBG] FAIL: commandBuffer null (pass1)\n");
            out->release(); partials->release(); in->release();
            pso2->release(); pso1->release(); q->release(); dev->release(); pool->release(); return 0;
        }
        ComputeCommandEncoder* enc = cb->computeCommandEncoder();
        if (!enc) {
            std::fprintf(stderr, "[SumDBG] FAIL: computeCommandEncoder null (pass1)\n");
            cb->release();
            out->release(); partials->release(); in->release();
            pso2->release(); pso1->release(); q->release(); dev->release(); pool->release(); return 0;
        }
        enc->setComputePipelineState(pso1);
        enc->setBuffer(in, 0, 0);
        enc->setBytes(&n, sizeof(uint32_t), 1);
        enc->setBuffer(partials, 0, 2);
        enc->setThreadgroupMemoryLength(tg * sizeof(uint64_t), 0);

        MTL::Size grid = MTL::Size::Make(groups * tg, 1, 1);
        MTL::Size tpg  = MTL::Size::Make(tg, 1, 1);
        enc->dispatchThreads(grid, tpg);
        enc->endEncoding();
        cb->commit();
        cb->waitUntilCompleted();
        std::fprintf(stderr, "[SumDBG] Step 7: pass1 done\n");
    }

    // Pass 2
    if (groups > 1) {
        uint32_t pCount = groups;
        CommandBuffer* cb  = q->commandBuffer();
        if (!cb) {
            std::fprintf(stderr, "[SumDBG] FAIL: commandBuffer null (pass2)\n");
            out->release(); partials->release(); in->release();
            pso2->release(); pso1->release(); q->release(); dev->release(); pool->release(); return 0;
        }
        ComputeCommandEncoder* enc = cb->computeCommandEncoder();
        if (!enc) {
            std::fprintf(stderr, "[SumDBG] FAIL: computeCommandEncoder null (pass2)\n");
            cb->release();
            out->release(); partials->release(); in->release();
            pso2->release(); pso1->release(); q->release(); dev->release(); pool->release(); return 0;
        }
        enc->setComputePipelineState(pso2);
        enc->setBuffer(partials, 0, 0);
        enc->setBytes(&pCount, sizeof(uint32_t), 1);
        enc->setBuffer(out, 0, 2);

        MTL::Size grid = MTL::Size::Make(1, 1, 1);
        MTL::Size tpg  = MTL::Size::Make(1, 1, 1);
        enc->dispatchThreads(grid, tpg);
        enc->endEncoding();
        cb->commit();
        cb->waitUntilCompleted();
        std::fprintf(stderr, "[SumDBG] Step 8: pass2 done\n");
    } else {
        *reinterpret_cast<uint64_t*>(out->contents()) =
            *reinterpret_cast<const uint64_t*>(partials->contents());
        std::fprintf(stderr, "[SumDBG] Step 8: single-group fast path\n");
    }

    uint64_t sum = *reinterpret_cast<const uint64_t*>(out->contents());
    std::fprintf(stderr, "[SumDBG] Step 9: readback sum=%llu\n", (unsigned long long)sum);

    // Cleanup
    out->release();
    partials->release();
    in->release();
    pso2->release();
    pso1->release();
    q->release();
    dev->release();
    pool->release();

    return sum;
}