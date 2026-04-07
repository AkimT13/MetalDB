#pragma once
#include <vector>
#include <string>
#include <cstdint>

// GPU string equality scan (Arrow-style column layout).
//
// chars:    concatenated UTF-8 bytes of all strings for live rows.
// offsets:  n+1 int32_t offsets; string i = chars[offsets[i]..offsets[i+1]].
// rowIDs:   n rowIDs corresponding to each string (in the same order).
// needle:   the string to search for.
// outRowIDs: receives rowIDs of matching rows.
//
// Returns true if the GPU kernel ran successfully.
// Returns false on pipeline failure — caller must fall back to CPU.
bool gpuStringScanEquals(
    const std::vector<char>&     chars,
    const std::vector<int32_t>&  offsets,
    const std::vector<uint32_t>& rowIDs,
    const std::string&           needle,
    std::vector<uint32_t>&       outRowIDs
);
