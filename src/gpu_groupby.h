#pragma once
#include <vector>
#include <cstdint>
#include <unordered_map>

// GPU hash-based group-by (count + sum) for uint32_t key/value columns.
// Uses device-space atomics (Metal 3.0 / GPUFamilyApple7+).
// Falls back to returning empty maps when Metal 3.0 is unavailable —
// the caller must handle the CPU fallback path.
//
// numBuckets must be a power-of-two >= number of distinct keys.
bool gpuGroupByCountSum(
    const std::vector<uint32_t>& keys,
    const std::vector<uint32_t>& vals,
    uint32_t numBuckets,
    std::unordered_map<uint32_t, uint64_t>& outCount,
    std::unordered_map<uint32_t, uint64_t>& outSum);
