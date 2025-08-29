#include <metal_stdlib>
using namespace metal;

// values[i] corresponds to rowIDs[i]. If values[i] == needle,
// append rowIDs[i] to outRowIDs using an atomic counter.
kernel void filter_equals(device const uint32_t* values      [[buffer(0)]],
                          device const uint32_t* rowIDs      [[buffer(1)]],
                          device uint32_t*       outRowIDs   [[buffer(2)]],
                          device atomic_uint*    outCount    [[buffer(3)]],
                          constant uint32_t&     needle      [[buffer(4)]],
                          constant uint32_t&     n           [[buffer(5)]],
                          uint gid [[thread_position_in_grid]])
{
    if (gid >= n) return;
    uint v = values[gid];
    if (v == needle) {
        uint idx = atomic_fetch_add_explicit(outCount, 1, memory_order_relaxed);
        outRowIDs[idx] = rowIDs[gid];
    }
}
