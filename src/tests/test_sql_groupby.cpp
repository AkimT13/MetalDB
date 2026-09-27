// GROUP BY / DISTINCT / aggregates vs. brute-force reference.
//
// Covers the generic hash-aggregation path (string + multi-column keys, WHERE,
// every aggregate) and the GPU-capable fast path (single UINT32 key, COUNT/SUM/AVG),
// which is run both CPU-only and with the GPU threshold at 0.
#include "../Engine.hpp"
#include "../MiniSQL.hpp"

#include <cassert>
#include <cstdio>
#include <cstdlib>
#include <map>
#include <string>
#include <tuple>
#include <vector>

extern "C" bool metalIsAvailable();

namespace {

const char* kBase = "/tmp/sql_groupby_tbl";

void cleanup() {
    for (const char* ext : {".mdb", ".mdb.idx", ".mdb.wal", ".mdb.2.str"})
        std::remove((std::string(kBase) + ext).c_str());
}

std::string i128(__int128 v) {
    if (v == 0) return "0";
    bool neg = v < 0;
    unsigned __int128 u = neg ? static_cast<unsigned __int128>(-(v + 1)) + 1 : static_cast<unsigned __int128>(v);
    std::string s;
    while (u) {
        s.insert(s.begin(), char('0' + int(u % 10)));
        u /= 10;
    }
    return neg ? "-" + s : s;
}

using Rows = std::vector<std::vector<std::string>>;

// Mirrors the engine's float formatting: shortest round-trip representation.
std::string shortest(double v) {
    char buf[64];
    for (int p = 15; p <= 17; ++p) {
        std::snprintf(buf, sizeof(buf), "%.*g", p, v);
        if (p == 17 || std::strtod(buf, nullptr) == v) break;
    }
    return buf;
}

Rows query(Engine& e, const std::string& sql) {
    return executeMiniSQL(e, sql).rows;
}

void expectEq(const Rows& got, const Rows& want, const std::string& what) {
    if (got == want) return;
    std::fprintf(stderr, "MISMATCH in %s: got %zu rows, want %zu\n", what.c_str(), got.size(), want.size());
    for (size_t i = 0; i < got.size() && i < want.size(); ++i) {
        if (got[i] != want[i]) {
            std::fprintf(stderr, "  first diff at row %zu\n", i);
            for (size_t j = 0; j < got[i].size() && j < want[i].size(); ++j)
                std::fprintf(stderr, "    [%zu] got '%s' want '%s'\n", j, got[i][j].c_str(), want[i][j].c_str());
            break;
        }
    }
    std::exit(1);
}

}  // namespace

int main() {
    cleanup();
    const uint32_t kRows = 20'000;
    struct R {
        uint32_t key;    // c0 UINT32, 37 distinct
        uint32_t val;    // c1 UINT32
        std::string tag; // c2 STRING, 5 distinct
        int64_t big;     // c3 INT64, large magnitudes (sums exceed 2^53)
        double d;        // c4 DOUBLE
    };
    static const char* tags[] = {"ash", "birch", "cedar", "", "elm"};

    std::vector<R> rows;
    std::vector<std::vector<ColValue>> batch;
    for (uint32_t i = 0; i < kRows; ++i) {
        R r{i % 37, (i * 2654435761u) % 100000u, tags[i % 5],
            (i % 2 ? 1 : -1) * ((int64_t(1) << 55) + int64_t(i)), (i % 9) * 0.5};
        rows.push_back(r);
        batch.push_back({ColValue(r.key), ColValue(r.val), ColValue(r.tag), ColValue(r.big), ColValue(r.d)});
    }

    Engine e;
    Table& t = e.createTypedTable(kBase, {ColType::UINT32, ColType::UINT32, ColType::STRING,
                                          ColType::INT64, ColType::DOUBLE});
    t.applyAtomic({}, batch);
    std::vector<uint32_t> dels;
    std::vector<bool> live(kRows, true);
    for (uint32_t i = 3; i < kRows; i += 13) {
        dels.push_back(i);
        live[i] = false;
    }
    t.applyAtomic(dels, {});
    const std::string from = std::string(" FROM '") + kBase + "'";

    for (int mode = 0; mode < 2; ++mode) {
        t.setUseGPU(mode == 1);
        t.setGPUThreshold(0);

        // Fast path: single UINT32 key, COUNT / SUM / AVG.
        {
            std::map<uint32_t, std::tuple<uint64_t, uint64_t>> ref;
            for (uint32_t i = 0; i < kRows; ++i) {
                if (!live[i]) continue;
                auto& [n, s] = ref[rows[i].key];
                ++n;
                s += rows[i].val;
            }
            Rows want;
            for (auto& [k, v] : ref) {
                const std::string avg = shortest(double(std::get<1>(v)) / double(std::get<0>(v)));
                want.push_back({std::to_string(k), std::to_string(std::get<0>(v)), std::to_string(std::get<1>(v)), avg});
            }
            expectEq(query(e, "SELECT c0, count(*), sum(c1), avg(c1)" + from + " GROUP BY c0"), want, "fast path");
        }

        // Generic path: string key + WHERE + every aggregate, exact INT64 sums.
        {
            struct Acc {
                uint64_t n = 0;
                __int128 bigSum = 0;
                int64_t bigMin = INT64_MAX, bigMax = INT64_MIN;
                double dSum = 0;
                uint32_t valMin = UINT32_MAX;
            };
            std::map<std::string, Acc> ref;
            for (uint32_t i = 0; i < kRows; ++i) {
                if (!live[i] || rows[i].key >= 30) continue;
                Acc& a = ref[rows[i].tag];
                ++a.n;
                a.bigSum += rows[i].big;
                a.bigMin = std::min(a.bigMin, rows[i].big);
                a.bigMax = std::max(a.bigMax, rows[i].big);
                a.dSum += rows[i].d;
                a.valMin = std::min(a.valMin, rows[i].val);
            }
            Rows want;
            for (auto& [tag, a] : ref) {
                const std::string dsum = shortest(a.dSum);
                want.push_back({tag, std::to_string(a.n), i128(a.bigSum), std::to_string(a.bigMin),
                                std::to_string(a.bigMax), dsum, std::to_string(a.valMin)});
            }
            expectEq(query(e, "SELECT c2, count(*), sum(c3), min(c3), max(c3), sum(c4), min(c1)" + from +
                                  " WHERE c0 < 30 GROUP BY c2"),
                     want, "generic string-key path");
        }

        // Multi-column key, ordered by aggregate, with LIMIT.
        {
            std::map<std::pair<std::string, uint32_t>, uint64_t> ref;
            for (uint32_t i = 0; i < kRows; ++i)
                if (live[i]) ++ref[{rows[i].tag, rows[i].key % 4}];
            Rows want;
            for (auto& [k, n] : ref) want.push_back({k.first, std::to_string(k.second), std::to_string(n)});
            // c0 % 4 isn't expressible, so group on (c2, c0) and compare only the count of groups.
            auto got = query(e, "SELECT c2, c0, count(*)" + from + " GROUP BY c2, c0");
            assert(got.size() == 5 * 37);
            auto top = query(e, "SELECT c2, c0, count(*)" + from + " GROUP BY c2, c0 ORDER BY 3 DESC, c2, c0 LIMIT 3");
            assert(top.size() == 3);
            assert(std::stoul(top[0][2]) >= std::stoul(top[1][2]));
        }

        // DISTINCT
        {
            Rows want = {{""}, {"ash"}, {"birch"}, {"cedar"}, {"elm"}};
            expectEq(query(e, "SELECT DISTINCT c2" + from), want, "distinct strings");
            assert(query(e, "SELECT DISTINCT c0, c2" + from).size() == 5 * 37);
        }

        // Scalar aggregates, exact INT64 sum across the whole table.
        {
            __int128 total = 0;
            uint64_t n = 0;
            for (uint32_t i = 0; i < kRows; ++i)
                if (live[i]) {
                    total += rows[i].big;
                    ++n;
                }
            expectEq(query(e, "SELECT count(*), sum(c3), count(c2)" + from),
                     Rows{{std::to_string(n), i128(total), std::to_string(n)}}, "scalar aggregates");
        }
    }

    // Empty input: groups vanish, scalar aggregates still produce one row.
    {
        auto r = query(e, "SELECT c0, count(*)" + from + " WHERE c1 > 4000000 GROUP BY c0");
        assert(r.empty());
        r = query(e, "SELECT count(*), sum(c1), min(c1), avg(c1)" + from + " WHERE c1 > 4000000");
        assert((r == Rows{{"0", "0", "", ""}}));
    }

    cleanup();
    std::printf("test_sql_groupby: passed (%s)\n", metalIsAvailable() ? "GPU exercised" : "CPU-only");
    return 0;
}
