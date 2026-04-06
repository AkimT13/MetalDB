#include <metal_stdlib>
using namespace metal;

struct ScanRangeParams {
    uint32_t lo;
    uint32_t hi;
    uint32_t n;
};

kernel void filter_between(device const uint32_t* values    [[buffer(0)]],
                           device const uint32_t* rowIDs    [[buffer(1)]],
                           device uint32_t*       outRowIDs [[buffer(2)]],
                           device atomic_uint*    outCount  [[buffer(3)]],
                           constant ScanRangeParams& P      [[buffer(4)]],
                           uint gid [[thread_position_in_grid]])
{
    if (gid >= P.n) return;
    uint v = values[gid];
    if (v >= P.lo && v <= P.hi) {
        uint idx = atomic_fetch_add_explicit(outCount, 1, memory_order_relaxed);
        outRowIDs[idx] = rowIDs[gid];
    }
}
