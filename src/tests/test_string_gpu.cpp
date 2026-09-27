// GPU string equality scan vs. CPU reference.
//
// On a Mac with Metal the large-table scans below take the GPU path
// (gpu_string_scan.mm); elsewhere they fall back to CPU. Either way the results
// must match the CPU-only reference exactly, including edge cases the Arrow-style
// packing has to get right: empty strings, needles that are prefixes of stored
// values, multi-byte UTF-8, and deleted rows.
#include "../Engine.hpp"

#include <cassert>
#include <cstdio>
#include <string>
#include <vector>

extern "C" bool metalIsAvailable();

static const char* kPath = "/tmp/str_gpu_tbl";

static void cleanup() {
    for (const char* ext : {".mdb", ".mdb.idx", ".mdb.wal", ".mdb.1.str"}) {
        std::string p = std::string(kPath) + ext;
        std::remove(p.c_str());
    }
}

static std::string valueFor(uint32_t i) {
    switch (i % 6) {
        case 0: return "alpha";
        case 1: return "alphabet";      // needle "alpha" is a prefix of this
        case 2: return "";              // empty string
        case 3: return "caf\xC3\xA9";   // "café" (multi-byte UTF-8)
        case 4: return "beta-" + std::to_string(i % 50);
        default: return "a";
    }
}

int main() {
    cleanup();
    const uint32_t kRows = 60'000;  // above GPU_STRING_THRESHOLD guidance (50k)

    Engine e;
    Table& t = e.createTypedTable(kPath, {ColType::UINT32, ColType::STRING});
    for (uint32_t i = 0; i < kRows; ++i)
        t.insertTypedRow({ColValue(i), ColValue(valueFor(i))});
    for (uint32_t i = 0; i < kRows; i += 7) t.deleteRow(i);

    const std::vector<std::string> needles = {
        "alpha", "alphabet", "", "caf\xC3\xA9", "beta-7", "a", "missing", "alph"};

    for (const auto& needle : needles) {
        t.setUseGPU(false);
        const auto cpu = t.scanEqualsString(1, needle);

        t.setUseGPU(true);
        t.setGPUThreshold(0);
        const auto gpu = t.scanEqualsString(1, needle);

        if (cpu != gpu) {
            std::fprintf(stderr, "mismatch for needle '%s': cpu=%zu gpu=%zu\n",
                         needle.c_str(), cpu.size(), gpu.size());
            return 1;
        }
        for (uint32_t rowID : cpu) {
            assert(rowID % 7 != 0);  // deleted rows never match
            assert(valueFor(rowID) == needle);
        }
    }

    // Spot-check expected cardinalities from the generator.
    t.setUseGPU(false);
    size_t expectedAlpha = 0;
    for (uint32_t i = 0; i < kRows; ++i)
        if (i % 7 != 0 && i % 6 == 0) ++expectedAlpha;
    assert(t.scanEqualsString(1, "alpha").size() == expectedAlpha);
    assert(t.scanEqualsString(1, "missing").empty());

    cleanup();
    std::printf("test_string_gpu: passed (%s path exercised)\n",
                metalIsAvailable() ? "GPU" : "CPU-only");
    return 0;
}
