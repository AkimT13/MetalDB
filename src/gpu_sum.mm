// gpu_sum.mm
// Two-pass GPU reduction (sum of uint32 values) using a precompiled metallib.
// Device, CommandQueue, and both PSOs are cached for the lifetime of the process.

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

// ── Pipeline cache ────────────────────────────────────────────────────────────
namespace {
    MTL::Device*               s_dev  = nullptr;
    MTL::CommandQueue*         s_q    = nullptr;
    MTL::ComputePipelineState* s_pso1 = nullptr; // sum_partials
    MTL::ComputePipelineState* s_pso2 = nullptr; // sum_reduce_partials
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

        auto path = metallibPath("gpu_sum.metallib");
        NS::String*   pathStr = NS::String::string(path.c_str(), NS::UTF8StringEncoding);
        NS::URL*      url     = NS::URL::fileURLWithPath(pathStr);
        NS::Error*    err     = nullptr;
        MTL::Library* lib     = s_dev->newLibrary(url, &err);
        if (!lib) {
            std::fprintf(stderr, "[GpuSum] Failed to load metallib '%s': %s\n",
                         path.c_str(),
                         err ? err->localizedDescription()->utf8String() : "(null)");
            return false;
        }

        auto* n1 = NS::String::string("sum_partials",         NS::UTF8StringEncoding);
        auto* n2 = NS::String::string("sum_reduce_partials",  NS::UTF8StringEncoding);
        MTL::Function* f1 = lib->newFunction(n1);
        MTL::Function* f2 = lib->newFunction(n2);
        lib->release();
        if (!f1 || !f2) { if (f1) f1->release(); if (f2) f2->release(); return false; }

        NS::Error* e = nullptr;
        s_pso1 = s_dev->newComputePipelineState(f1, &e); f1->release();
        if (!s_pso1) { if (e) e->release(); f2->release(); return false; }
        s_pso2 = s_dev->newComputePipelineState(f2, &e); f2->release();
        if (!s_pso2) { if (e) e->release(); s_pso1->release(); s_pso1 = nullptr; return false; }

        s_ready.store(true, std::memory_order_release);
        return true;
    }
} // namespace

// ── gpuSumU32 ─────────────────────────────────────────────────────────────────
uint64_t gpuSumU32(const std::vector<uint32_t>& values) {
    if (values.empty() || !metalIsAvailable()) return 0;

    NS::AutoreleasePool* pool = NS::AutoreleasePool::alloc()->init();
    if (!ensurePipeline()) { pool->release(); return 0; }

    const uint32_t n     = static_cast<uint32_t>(values.size());
    const uint32_t tg    = std::min<uint32_t>(s_pso1->maxTotalThreadsPerThreadgroup(), 256);
    const uint32_t groups = (n + tg - 1) / tg;

    MTL::Buffer* in       = s_dev->newBuffer(n * sizeof(uint32_t), MTL::ResourceStorageModeShared);
    MTL::Buffer* partials = s_dev->newBuffer(std::max(groups, 1u) * sizeof(uint64_t), MTL::ResourceStorageModeShared);
    MTL::Buffer* out      = s_dev->newBuffer(sizeof(uint64_t), MTL::ResourceStorageModeShared);
    if (!in || !partials || !out) {
        if (in) in->release(); if (partials) partials->release(); if (out) out->release();
        pool->release(); return 0;
    }

    std::memcpy(in->contents(), values.data(), n * sizeof(uint32_t));
    std::memset(out->contents(), 0, sizeof(uint64_t));

    // Pass 1 — per-threadgroup partial sums
    {
        MTL::CommandBuffer*         cb  = s_q->commandBuffer();
        MTL::ComputeCommandEncoder* enc = cb->computeCommandEncoder();
        enc->setComputePipelineState(s_pso1);
        enc->setBuffer(in, 0, 0);
        enc->setBytes(&n, sizeof(uint32_t), 1);
        enc->setBuffer(partials, 0, 2);
        enc->setThreadgroupMemoryLength(tg * sizeof(uint64_t), 0);
        enc->dispatchThreads(MTL::Size::Make(groups * tg, 1, 1), MTL::Size::Make(tg, 1, 1));
        enc->endEncoding();
        cb->commit();
        cb->waitUntilCompleted();
    }

    // Pass 2 — reduce partials to final sum (skip if only one group)
    if (groups > 1) {
        MTL::CommandBuffer*         cb  = s_q->commandBuffer();
        MTL::ComputeCommandEncoder* enc = cb->computeCommandEncoder();
        enc->setComputePipelineState(s_pso2);
        enc->setBuffer(partials, 0, 0);
        enc->setBytes(&groups, sizeof(uint32_t), 1);
        enc->setBuffer(out, 0, 2);
        enc->dispatchThreads(MTL::Size::Make(1, 1, 1), MTL::Size::Make(1, 1, 1));
        enc->endEncoding();
        cb->commit();
        cb->waitUntilCompleted();
    } else {
        *static_cast<uint64_t*>(out->contents()) =
            *static_cast<const uint64_t*>(partials->contents());
    }

    uint64_t sum = *static_cast<const uint64_t*>(out->contents());
    out->release(); partials->release(); in->release();
    pool->release();
    return sum;
}
