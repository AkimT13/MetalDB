#include <metal_stdlib>
using namespace metal;

kernel void sum_partials(device const uint32_t* values   [[buffer(0)]],
                         constant uint32_t&     n        [[buffer(1)]],
                         device uint64_t*       partials [[buffer(2)]],
                         uint gid   [[thread_position_in_grid]],
                         uint lane  [[thread_index_in_threadgroup]],
                         uint tgSz  [[threads_per_threadgroup]],
                         threadgroup uint64_t*  shmem    [[threadgroup(0)]])
{
    uint64_t v = (gid < n) ? (uint64_t)values[gid] : 0;
    shmem[lane] = v;
    threadgroup_barrier(mem_flags::mem_threadgroup);

    for (uint stride = tgSz >> 1; stride > 0; stride >>= 1) {
        if (lane < stride) shmem[lane] += shmem[lane + stride];
        threadgroup_barrier(mem_flags::mem_threadgroup);
    }

    if (lane == 0) partials[gid / tgSz] = shmem[0];
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
