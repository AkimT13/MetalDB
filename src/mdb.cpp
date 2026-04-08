#include "Engine.hpp"
#include "MiniSQL.hpp"
#include "QuerySession.hpp"
#include "Server.hpp"
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
#include <cctype>
#include <unistd.h>

static void usage(const char* argv0) {
    std::fprintf(stderr,
        "Usage:\n"
        "  %s create <file> <pageSize> <numCols>\n"
        "  %s insert <file> <v0> [v1 ...]\n"
        "  %s select-eq <file> <col> <val>\n"
        "  %s select-between <file> <col> <lo> <hi>\n"
        "  %s query <sql>\n"
        "  %s repl\n"
        "  %s serve <port>\n"
        "  %s flush <table>\n"
        "  %s sum <file> <col>\n",
        argv0, argv0, argv0, argv0, argv0, argv0, argv0, argv0, argv0);
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

static std::string toBaseTableName(const std::string& name) {
    if (name.size() >= 4 && name.substr(name.size() - 4) == ".mdb")
        return name.substr(0, name.size() - 4);
    return name;
}

static std::string trim(const std::string& input) {
    size_t start = 0;
    while (start < input.size() &&
           std::isspace(static_cast<unsigned char>(input[start]))) ++start;
    size_t end = input.size();
    while (end > start &&
           std::isspace(static_cast<unsigned char>(input[end - 1]))) --end;
    return input.substr(start, end - start);
}

static void printReplHelp() {
    std::puts("Mini-SQL REPL");
    std::puts(".help           show this help");
    std::puts(".quit           exit the REPL");
    std::puts("Queries must end with ';' and use the same syntax as `mdb query`.");
}

static bool findStatementTerminator(const std::string& input, size_t& pos) {
    bool inString = false;
    for (size_t i = 0; i < input.size(); ++i) {
        const char ch = input[i];
        if (ch == '\'') inString = !inString;
        if (!inString && ch == ';') {
            pos = i;
            return true;
        }
    }
    return false;
}

static int runRepl() {
    Engine engine;
    std::string pending;
    std::string line;
    printReplHelp();
    while (true) {
        std::printf("%s", pending.empty() ? "mdb> " : "...> ");
        std::fflush(stdout);
        if (!std::getline(std::cin, line)) {
            if (!pending.empty())
                std::fprintf(stderr, "repl error: unterminated statement before EOF\n");
            std::printf("\n");
            return pending.empty() ? 0 : 1;
        }

        const std::string stripped = trim(line);
        if (pending.empty() && stripped.empty()) continue;
        if (pending.empty() && !stripped.empty() && stripped[0] == '.') {
            if (stripped == ".quit") return 0;
            if (stripped == ".help") {
                printReplHelp();
                continue;
            }
            std::fprintf(stderr, "repl error: unknown command: %s\n", stripped.c_str());
            continue;
        }

        if (!pending.empty()) pending += '\n';
        pending += line;

        size_t termPos = 0;
        while (findStatementTerminator(pending, termPos)) {
            const std::string statement = trim(pending.substr(0, termPos));
            pending.erase(0, termPos + 1);
            pending = trim(pending);
            if (!statement.empty()) (void)executeMiniSQLToStream(engine, statement, "repl");
        }
    }
}

int main(int argc, char** argv) {
    if (argc < 2) { usage(argv[0]); return 1; }
    std::string cmd = argv[1];

    if (cmd == "query") {
        if (argc < 3) { usage(argv[0]); return 1; }
        std::string sql = argv[2];
        for (int i = 3; i < argc; ++i) {
            sql += ' ';
            sql += argv[i];
        }
        Engine engine;
        return executeMiniSQLToStream(engine, sql, "query") ? 0 : 1;
    }

    if (cmd == "repl") {
        if (argc != 2) { usage(argv[0]); return 1; }
        return runRepl();
    }

    if (cmd == "serve") {
        if (argc != 3) { usage(argv[0]); return 1; }
        uint16_t port = 0;
        if (!parseU16(argv[2], port) || port == 0) {
            std::fprintf(stderr, "Bad port\n");
            return 1;
        }
        return runServer(port);
    }

    if (cmd == "flush") {
        if (argc != 3) { usage(argv[0]); return 1; }
        const std::string baseName = toBaseTableName(argv[2]);
        const std::string path = baseName + ".mdb";
        if (::access(path.c_str(), F_OK) != 0) {
            std::fprintf(stderr, "flush error: table file does not exist\n");
            return 1;
        }
        try {
            Engine engine;
            engine.flush(baseName);
            std::printf("flushed %s\n", baseName.c_str());
            return 0;
        } catch (const std::exception& ex) {
            std::fprintf(stderr, "flush error: %s\n", ex.what());
            return 1;
        }
    }

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
            std::fprintf(stderr, "Expected %zu values, got %zu\n",
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
