
#include "Table.hpp"
#include "ValueTypes.hpp"

#include <cstdio>
#include <cstdlib>
#include <cstdint>
#include <cstring>
#include <string>
#include <vector>
#include <optional>
#include <iostream>

static void usage(const char* argv0) {
    std::fprintf(stderr,
        "Usage:\n"
        "  %s create <file> <pageSize> <numCols>\n"
        "  %s insert <file> <v0> [v1 ...]\n"
        "  %s select-eq <file> <col> <val>\n"
        "  %s select-between <file> <col> <lo> <hi>\n"
        "  %s sum <file> <col>\n",
        argv0, argv0, argv0, argv0, argv0);
}

static bool parseU16(const char* s, uint16_t& out) {
    char* end = nullptr;
    unsigned long v = std::strtoul(s, &end, 10);
    if (!s[0] || (end && *end) || v > 0xFFFFul) return false;
    out = static_cast<uint16_t>(v);
    return true;
}

static bool parseU32(const char* s, uint32_t& out) {
    char* end = nullptr;
    unsigned long v = std::strtoul(s, &end, 10);
    if (!s[0] || (end && *end) || v > 0xFFFFFFFFul) return false;
    out = static_cast<uint32_t>(v);
    return true;
}

int main(int argc, char** argv) {
    if (argc < 2) { usage(argv[0]); return 1; }
    std::string cmd = argv[1];

    if (cmd == "create") {
        if (argc != 5) { usage(argv[0]); return 1; }
        const char* path = argv[2];
        uint16_t pageSize = 0, numCols = 0;
        if (!parseU16(argv[3], pageSize) || !parseU16(argv[4], numCols) || pageSize == 0 || numCols == 0) {
            std::fprintf(stderr, "Invalid pageSize or numCols\n");
            return 1;
        }
        Table t(path, pageSize, numCols);
        std::printf("created %s (pageSize=%u, numCols=%u)\n", path, pageSize, numCols);
        return 0;
    }

    if (cmd == "insert") {
        if (argc < 4) { usage(argv[0]); return 1; }
        const char* path = argv[2];
        Table t(path); // open existing
        std::vector<ValueType> row;
        row.reserve(argc - 3);
        for (int i = 3; i < argc; ++i) {
            uint32_t v = 0;
            if (!parseU32(argv[i], v)) {
                std::fprintf(stderr, "Bad value: %s\n", argv[i]);
                return 1;
            }
            row.push_back(static_cast<ValueType>(v));
        }
        if (row.size() != t.numColumns()) {
            std::fprintf(stderr, "Expected %u values, got %zu\n",
                         t.numColumns(), row.size());
            return 1;
        }
        uint32_t rid = t.insertRow(row);
        std::printf("rowID=%u\n", rid);
        return 0;
    }

    if (cmd == "select-eq") {
        if (argc != 5) { usage(argv[0]); return 1; }
        const char* path = argv[2];
        uint16_t col = 0;
        uint32_t val = 0;
        if (!parseU16(argv[3], col) || !parseU32(argv[4], val)) {
            std::fprintf(stderr, "Bad col or val\n"); return 1;
        }
        Table t(path);
        if (col >= t.numColumns()) {
            std::fprintf(stderr, "col out of range\n"); return 1;
        }
        auto rowIDs = t.scanEquals(col, static_cast<ValueType>(val));
        for (auto r : rowIDs) std::printf("%u\n", r);
        return 0;
    }

    if (cmd == "select-between") {
        if (argc != 6) { usage(argv[0]); return 1; }
        const char* path = argv[2];
        uint16_t col = 0;
        uint32_t lo = 0, hi = 0;
        if (!parseU16(argv[3], col) || !parseU32(argv[4], lo) || !parseU32(argv[5], hi)) {
            std::fprintf(stderr, "Bad arguments\n"); return 1;
        }
        if (lo > hi) std::swap(lo, hi);
        Table t(path);
        if (col >= t.numColumns()) {
            std::fprintf(stderr, "col out of range\n"); return 1;
        }
        auto rowIDs = t.whereBetween(col, static_cast<ValueType>(lo), static_cast<ValueType>(hi));
        for (auto r : rowIDs) std::printf("%u\n", r);
        return 0;
    }

    if (cmd == "sum") {
        if (argc != 4) { usage(argv[0]); return 1; }
        const char* path = argv[2];
        uint16_t col = 0;
        if (!parseU16(argv[3], col)) {
            std::fprintf(stderr, "Bad column index\n"); return 1;
        }
        Table t(path);
        if (col >= t.numColumns()) {
            std::fprintf(stderr, "col out of range\n"); return 1;
        }
        ValueType s = t.sumColumnHybrid(col);
        std::printf("%u\n", static_cast<uint32_t>(s));
        return 0;
    }

    usage(argv[0]);
    return 1;
}