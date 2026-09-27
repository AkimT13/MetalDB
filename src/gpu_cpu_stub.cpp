// CPU-only stand-ins for the Metal entry points.
//
// Linked instead of the *.mm GPU sources by the portable build (`make cpu`) so the
// engine builds and tests on Linux / CI / machines without Metal. metalIsAvailable()
// returns false, so every hybrid path in Table / GroupBy takes its CPU branch and
// the functions below are never reached in practice; they return "not handled"
// values defensively.
#include <cstdint>
#include <string>
#include <unordered_map>
#include <vector>

extern "C" bool metalIsAvailable() { return false; }
extern "C" void metalPrintDevices() {}

std::vector<uint32_t> gpuScanEquals(const std::vector<uint32_t>&, const std::vector<uint32_t>&, uint32_t) {
    return {};
}

extern "C" std::vector<uint32_t> gpuScanBetween(const std::vector<uint32_t>&, const std::vector<uint32_t>&,
                                                uint32_t, uint32_t) {
    return {};
}

bool gpuSumU32Checked(const std::vector<uint32_t>&, uint64_t&) { return false; }

uint64_t gpuSumU32(const std::vector<uint32_t>& values) {
    uint64_t acc = 0;
    for (uint32_t v : values) acc += v;
    return acc;
}

bool gpuGroupByCountSum(const std::vector<uint32_t>&, const std::vector<uint32_t>&, uint32_t,
                        std::unordered_map<uint32_t, uint64_t>&, std::unordered_map<uint32_t, uint64_t>&) {
    return false;
}

bool gpuStringScanEquals(const std::vector<char>&, const std::vector<int32_t>&, const std::vector<uint32_t>&,
                         const std::string&, std::vector<uint32_t>&) {
    return false;
}
