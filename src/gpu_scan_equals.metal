#include <metal_stdlib>
using namespace metal;

struct ScanEqualsParams {
    uint32_t needle;
    uint32_t n;
};

kernel void filter_equals(device const uint32_t* values    [[buffer(0)]],
                          device const uint32_t* rowIDs    [[buffer(1)]],
                          device uint32_t*       outRowIDs [[buffer(2)]],
                          device atomic_uint*    outCount  [[buffer(3)]],
                          constant ScanEqualsParams& P     [[buffer(4)]],
                          uint gid [[thread_position_in_grid]])
{
    if (gid >= P.n) return;
    if (values[gid] == P.needle) {
        uint idx = atomic_fetch_add_explicit(outCount, 1, memory_order_relaxed);
        outRowIDs[idx] = rowIDs[gid];
    }
}
