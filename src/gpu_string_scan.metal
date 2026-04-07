#include <metal_stdlib>
using namespace metal;

struct StringScanParams {
    uint32_t n;          // number of rows
    int32_t  needleLen;  // byte length of needle
};

// One thread per row. Compares chars[offsets[id]..offsets[id+1]] against needle.
// Writes 1 to results[id] on match, 0 otherwise.
kernel void stringScanEquals(
    device const char*         chars    [[buffer(0)]],
    device const int32_t*      offsets  [[buffer(1)]],
    device const char*         needle   [[buffer(2)]],
    constant StringScanParams& P        [[buffer(3)]],
    device uint32_t*           results  [[buffer(4)]],
    uint id [[thread_position_in_grid]])
{
    if (id >= P.n) { results[id] = 0; return; }

    int32_t start = offsets[id];
    int32_t len   = offsets[id + 1] - start;

    if (len != P.needleLen) { results[id] = 0; return; }

    for (int32_t i = 0; i < len; i++) {
        if (chars[start + i] != needle[i]) { results[id] = 0; return; }
    }
    results[id] = 1;
}
