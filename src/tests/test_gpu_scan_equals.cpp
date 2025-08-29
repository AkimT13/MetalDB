// tests/test_gpu_scan_equals.cpp
#include "../Table.hpp"
#include "../ValueTypes.hpp"

#include <cassert>
#include <iostream>
#include <vector>
#include <algorithm>
#include <unistd.h>

// from gpu_scan_equals.mm
extern "C" bool metalIsAvailable();
extern "C" void metalPrintDevices();

std::vector<uint32_t>
gpuScanEquals(const std::vector<uint32_t>& values,
              const std::vector<uint32_t>& rowIDs,
              uint32_t needle);

// --- helpers ---
static void printVec(const char* tag, const std::vector<uint32_t>& v) {
    std::cerr << tag << " (" << v.size() << "): ";
    for (auto x : v) std::cerr << x << " ";
    std::cerr << "\n";
}

static void printDiff(const char* tagA, const std::vector<uint32_t>& a,
                      const char* tagB, const std::vector<uint32_t>& b) {
    std::vector<uint32_t> A=a, B=b, onlyA;
    std::sort(A.begin(), A.end());
    std::sort(B.begin(), B.end());
    std::set_difference(A.begin(), A.end(), B.begin(), B.end(),
                        std::back_inserter(onlyA));
    if (!onlyA.empty()) {
        std::cerr << "Only in " << tagA << ": ";
        for (auto x : onlyA) std::cerr << x << " ";
        std::cerr << "\n";
    }
}

int main() {
    char tmpl[] = "/tmp/table_gpu_scanXXXXXX";
    int fd = mkstemp(tmpl);
    assert(fd >= 0);
    close(fd);

    std::string idxPath = std::string(tmpl) + ".idx";

    std::cerr << "[TEST] temp base: " << tmpl << "\n";

    std::vector<uint32_t> cpuRows, gpuRows;

    {
        std::cerr << "[TEST] constructing table\n";
        Table t(tmpl, /*pageSize*/4096, /*numColumns*/2);

        std::cerr << "[TEST] inserting rows\n";
        t.insertRow({2, 10});
        t.insertRow({3, 20});
        t.insertRow({2, 30});
        t.insertRow({5, 40});
        t.insertRow({2, 50});
        t.insertRow({7, 60});

        std::cerr << "[TEST] cpu scan\n";
        cpuRows = t.scanEquals(0, 2);
        std::vector<uint32_t> expect = {0,2,4};
        auto cpuChk = cpuRows; std::sort(cpuChk.begin(), cpuChk.end());
        std::sort(expect.begin(), expect.end());
        if (cpuChk != expect) {
            printVec("CPU (unexpected)", cpuChk);
            printVec("Expect", expect);
            assert(false && "CPU baseline not as expected");
        }

        if (!metalIsAvailable()) {
            std::cerr << "[TEST] Metal unavailable; devices:\n";
            metalPrintDevices();
            std::cout << "test_gpu_scan_equals: skipped (no Metal device)\n";
            // Let t destruct here (before unlink)
            goto CLEANUP;
        }

        std::cerr << "[TEST] materialize col 0\n";
        auto mat = t.materializeColumnWithRowIDs(0);
        if (mat.values.size() != mat.rowIDs.size()) {
            std::cerr << "[TEST] MISMATCH sizes: values=" << mat.values.size()
                      << " rowIDs=" << mat.rowIDs.size() << "\n";
            assert(false && "materialize produced misaligned vectors");
        }

        std::cerr << "[TEST] GPU scan\n";
        gpuRows = gpuScanEquals(mat.values, mat.rowIDs, /*needle*/ 2);
        std::cerr << "[TEST] GPU returned " << gpuRows.size() << " rows\n";
    } // Table t is destroyed here BEFORE we unlink files

    std::cerr << "[TEST] sort + compare\n";
    std::sort(cpuRows.begin(), cpuRows.end());
    std::sort(gpuRows.begin(), gpuRows.end());

    if (cpuRows != gpuRows) {
        printVec("CPU", cpuRows);
        printVec("GPU", gpuRows);
        printDiff("CPU", cpuRows, "GPU", gpuRows);
        printDiff("GPU", gpuRows, "CPU", cpuRows);
        std::cerr << "[TEST] sets differ\n";
        assert(false && "CPU and GPU results differ");
    }

    std::cout << "test_gpu_scan_equals: passed (GPU==CPU)\n";

CLEANUP:
    std::cerr << "[TEST] unlink files\n";
    unlink(tmpl);
    unlink(idxPath.c_str());
    std::cerr << "[TEST] done\n";
    return 0;
}