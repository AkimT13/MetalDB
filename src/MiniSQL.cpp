// Mini-SQL executor. Parsing lives in SqlParser, WHERE evaluation in WhereEval;
// this file validates statement shape against the table schema and executes.
#include "MiniSQL.hpp"

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <unordered_map>
#include <sstream>
#include <stdexcept>
#include <string>
#include <unistd.h>
#include <utility>
#include <vector>

#include "Engine.hpp"
#include "SqlParser.hpp"
#include "Table.hpp"
#include "ValueTypes.hpp"
#include "WhereEval.hpp"

extern "C" bool metalIsAvailable();

using namespace sql;

namespace {

// ── EXPLAIN tracing ──────────────────────────────────────────────────────────
// While an EXPLAIN runs, g_trace collects plan lines from the stages below.
// thread_local so concurrent sessions (server threads) never share a trace.
struct Trace {
    std::vector<std::string> lines;
    void add(const std::string& line) { lines.push_back(line); }
};
thread_local Trace* g_trace = nullptr;

void trace(const std::string& line) {
    if (g_trace) g_trace->add(line);
}

double msSince(std::chrono::steady_clock::time_point start) {
    return std::chrono::duration<double, std::milli>(std::chrono::steady_clock::now() - start).count();
}

std::string fmtMs(double ms) {
    char buf[32];
    std::snprintf(buf, sizeof(buf), "%.3f ms", ms);
    return buf;
}

// ── value formatting ─────────────────────────────────────────────────────────

std::string formatDouble(double v) {
    char buf[64];
    std::snprintf(buf, sizeof(buf), "%.15g", v);
    return buf;
}

std::string formatInt128(__int128 v) {
    if (v == 0) return "0";
    const bool neg = v < 0;
    unsigned __int128 u = neg ? static_cast<unsigned __int128>(-(v + 1)) + 1 : static_cast<unsigned __int128>(v);
    std::string out;
    while (u > 0) {
        out.push_back(static_cast<char>('0' + static_cast<int>(u % 10)));
        u /= 10;
    }
    if (neg) out.push_back('-');
    std::reverse(out.begin(), out.end());
    return out;
}

std::string formatColValue(const ColValue& value) {
    char buf[64];
    switch (value.type) {
        case ColType::UINT32: return std::to_string(value.u32);
        case ColType::INT64: return std::to_string(value.i64);
        case ColType::FLOAT:
            std::snprintf(buf, sizeof(buf), "%.7g", static_cast<double>(value.f32));
            return buf;
        case ColType::DOUBLE: return formatDouble(value.f64);
        case ColType::STRING: return value.str;
    }
    return "";
}

// ── aggregates ───────────────────────────────────────────────────────────────

struct AggregateState {
    uint64_t count = 0;
    __int128 intSum = 0;      // exact sum for UINT32 / INT64 inputs
    long double floatSum = 0; // FLOAT / DOUBLE inputs
    ColValue min;
    ColValue max;
    bool hasValue = false;

    void add(const ColValue& v) {
        if (!hasValue) {
            min = v;
            max = v;
            hasValue = true;
        } else {
            if (v < min) min = v;
            if (v > max) max = v;
        }
        ++count;
        switch (v.type) {
            case ColType::UINT32: intSum += v.u32; break;
            case ColType::INT64: intSum += v.i64; break;
            case ColType::FLOAT: floatSum += v.f32; break;
            case ColType::DOUBLE: floatSum += v.f64; break;
            case ColType::STRING: break;
        }
    }
};

bool isIntegral(ColType t) { return t == ColType::UINT32 || t == ColType::INT64; }

ColType aggregateType(const Table& table, const SelectItem& item) {
    switch (item.kind) {
        case SelectItem::Kind::CountStar:
        case SelectItem::Kind::Count:
            return ColType::INT64;
        case SelectItem::Kind::Sum:
            return isIntegral(table.columnFile(item.column.index).colType()) ? ColType::INT64 : ColType::DOUBLE;
        case SelectItem::Kind::Avg:
            return ColType::DOUBLE;
        default:
            return table.columnFile(item.column.index).colType();
    }
}

std::string formatAggregate(const Table& table, const SelectItem& item, const AggregateState& state) {
    switch (item.kind) {
        case SelectItem::Kind::CountStar:
        case SelectItem::Kind::Count:
            return std::to_string(state.count);
        case SelectItem::Kind::Sum:
            if (isIntegral(table.columnFile(item.column.index).colType())) return formatInt128(state.intSum);
            return formatDouble(static_cast<double>(state.floatSum));
        case SelectItem::Kind::Min:
            return state.hasValue ? formatColValue(state.min) : "";
        case SelectItem::Kind::Max:
            return state.hasValue ? formatColValue(state.max) : "";
        case SelectItem::Kind::Avg: {
            if (state.count == 0) return "";
            const long double total = static_cast<long double>(state.intSum) + state.floatSum;
            return formatDouble(static_cast<double>(total / static_cast<long double>(state.count)));
        }
        default:
            throw std::invalid_argument("not an aggregate");
    }
}

// ── validation ───────────────────────────────────────────────────────────────

void validateColumnRef(const Table& table, uint16_t colIdx) {
    if (colIdx >= table.numColumns())
        throw std::invalid_argument("column index out of bounds");
}

void validateQueryShape(const Table& table, const ParsedQuery& query) {
    bool hasStar = false;
    bool hasColumn = false;
    int aggregateCount = 0;

    for (const auto& item : query.selectItems) {
        switch (item.kind) {
            case SelectItem::Kind::Star:
                hasStar = true;
                break;
            case SelectItem::Kind::Column:
                hasColumn = true;
                validateColumnRef(table, item.column.index);
                break;
            case SelectItem::Kind::CountStar:
                ++aggregateCount;
                break;
            case SelectItem::Kind::Count:
            case SelectItem::Kind::Min:
            case SelectItem::Kind::Max:
                ++aggregateCount;
                validateColumnRef(table, item.column.index);
                break;
            case SelectItem::Kind::Sum:
            case SelectItem::Kind::Avg:
                ++aggregateCount;
                validateColumnRef(table, item.column.index);
                if (table.columnFile(item.column.index).colType() == ColType::STRING)
                    throw std::invalid_argument(item.header() + " requires a numeric column");
                break;
        }
    }

    if (hasStar && query.selectItems.size() != 1)
        throw std::invalid_argument("SELECT * cannot be combined with other projections");
    if (query.where) validateWhere(table, *query.where);

    if (query.distinct) {
        if (aggregateCount > 0 || !query.groupBy.empty())
            throw std::invalid_argument("SELECT DISTINCT cannot be combined with aggregates or GROUP BY");
        return;
    }

    if (!query.groupBy.empty()) {
        if (hasStar) throw std::invalid_argument("SELECT * with GROUP BY is not supported");
        for (const auto& key : query.groupBy) validateColumnRef(table, key.index);
        for (const auto& item : query.selectItems) {
            if (item.kind != SelectItem::Kind::Column) continue;
            const bool grouped = std::any_of(query.groupBy.begin(), query.groupBy.end(),
                                             [&](const ColumnRef& k) { return k.index == item.column.index; });
            if (!grouped)
                throw std::invalid_argument("column " + item.column.text +
                                            " must appear in GROUP BY or be used in an aggregate");
        }
        return;
    }

    if (aggregateCount > 0 && (hasColumn || hasStar))
        throw std::invalid_argument("column " + std::string(hasStar ? "*" : "references") +
                                    " cannot be mixed with aggregates without GROUP BY");
}

// ── SELECT execution ─────────────────────────────────────────────────────────

std::vector<uint32_t> executeWhere(Table& table, const ParsedQuery& query) {
    const auto start = std::chrono::steady_clock::now();
    if (!query.where) {
        auto ids = allLiveRowIDs(table);
        trace("Filter: none (all " + std::to_string(ids.size()) + " live rows)");
        return ids;
    }
    std::vector<std::string> leaves;
    auto ids = evaluateWhere(table, *query.where, g_trace ? &leaves : nullptr);
    if (g_trace) {
        trace("Filter: " + whereToString(*query.where));
        for (const auto& leaf : leaves) trace("  -> " + leaf);
        trace("  matched " + std::to_string(ids.size()) + " rows in " + fmtMs(msSince(start)));
    }
    return ids;
}

MiniSQLResult executeProjectionQuery(Table& table, const ParsedQuery& query) {
    std::vector<uint16_t> cols;
    MiniSQLResult result;

    if (query.selectItems.size() == 1 && query.selectItems[0].kind == SelectItem::Kind::Star) {
        for (uint16_t c = 0; c < table.numColumns(); ++c) cols.push_back(c);
    } else {
        for (const auto& item : query.selectItems) cols.push_back(item.column.index);
    }
    for (uint16_t c : cols) {
        result.headers.push_back("c" + std::to_string(c));
        result.types.push_back(table.columnFile(c).colType());
    }

    // Without ORDER BY, LIMIT/OFFSET can stop materializing early.
    const auto rowIDs = executeWhere(table, query);
    size_t begin = 0;
    size_t end = rowIDs.size();
    if (query.orderBy.empty()) {
        begin = static_cast<size_t>(std::min<uint64_t>(query.offset, rowIDs.size()));
        if (query.hasLimit) end = static_cast<size_t>(std::min<uint64_t>(begin + query.limit, end));
    }

    trace("Project: " + std::to_string(cols.size()) + " column(s), materializing " +
          std::to_string(end - begin) + " of " + std::to_string(rowIDs.size()) + " rows" +
          (query.orderBy.empty() && query.hasLimit ? " (LIMIT pushed down)" : ""));
    result.rows.reserve(end - begin);
    for (size_t i = begin; i < end; ++i) {
        std::vector<std::string> outRow;
        outRow.reserve(cols.size());
        bool ok = true;
        for (uint16_t c : cols) {
            auto v = table.fetchTypedValue(rowIDs[i], c);
            if (!v) {
                ok = false;
                break;
            }
            outRow.push_back(formatColValue(*v));
        }
        if (ok) result.rows.push_back(std::move(outRow));
    }
    return result;
}

MiniSQLResult executeScalarAggregateQuery(Table& table, const ParsedQuery& query) {
    const auto rowIDs = executeWhere(table, query);
    trace("Aggregate: scalar over " + std::to_string(rowIDs.size()) + " rows");
    MiniSQLResult result;
    std::vector<std::string> row;
    for (const auto& item : query.selectItems) {
        AggregateState state;
        if (item.kind == SelectItem::Kind::CountStar) {
            state.count = rowIDs.size();
        } else {
            for (uint32_t rowID : rowIDs)
                if (auto cell = table.fetchTypedValue(rowID, item.column.index)) state.add(*cell);
        }
        result.headers.push_back(item.header());
        result.types.push_back(aggregateType(table, item));
        row.push_back(formatAggregate(table, item, state));
    }
    result.rows.push_back(std::move(row));
    return result;
}

void setGroupedHeaders(const Table& table, const ParsedQuery& query, MiniSQLResult& result) {
    for (const auto& item : query.selectItems) {
        result.headers.push_back(item.header());
        result.types.push_back(item.isAggregate() ? aggregateType(table, item)
                                                  : table.columnFile(item.column.index).colType());
    }
}

// GPU-capable fast path: one UINT32 key, no WHERE, only COUNT / SUM / AVG over
// UINT32 columns. Routes through GroupBy::countByKey / sumByKey, which dispatch to
// the Metal group-by kernel for large tables.
bool tryFastGroupBy(Engine& engine, Table& table, const ParsedQuery& query,
                    const std::vector<ColumnRef>& keys, MiniSQLResult& result) {
    if (keys.size() != 1 || query.where) return false;
    const uint16_t keyCol = keys[0].index;
    if (table.columnFile(keyCol).colType() != ColType::UINT32) return false;
    for (const auto& item : query.selectItems) {
        switch (item.kind) {
            case SelectItem::Kind::Column:
            case SelectItem::Kind::CountStar:
            case SelectItem::Kind::Count:
                break;
            case SelectItem::Kind::Sum:
            case SelectItem::Kind::Avg:
                if (table.columnFile(item.column.index).colType() != ColType::UINT32) return false;
                break;
            default:
                return false;
        }
    }

    trace("Aggregate: GPU-capable group-by on " + keys[0].text + " (GroupBy::countByKey / sumByKey; " +
          (table.useGPU() && metalIsAvailable() && table.rowCount() >= table.gpuThreshold() ? "GPU" : "CPU") +
          " for " + std::to_string(table.rowCount()) + " rows)");
    const auto counts = engine.groupCount(query.tableName, keyCol);
    std::unordered_map<uint16_t, std::unordered_map<ValueType, uint64_t>> sums;
    for (const auto& item : query.selectItems)
        if ((item.kind == SelectItem::Kind::Sum || item.kind == SelectItem::Kind::Avg) && !sums.count(item.column.index))
            sums[item.column.index] = engine.groupSum(query.tableName, keyCol, item.column.index);

    std::vector<ValueType> orderedKeys;
    orderedKeys.reserve(counts.size());
    for (const auto& [k, n] : counts) orderedKeys.push_back(k);
    std::sort(orderedKeys.begin(), orderedKeys.end());

    setGroupedHeaders(table, query, result);
    for (ValueType k : orderedKeys) {
        const uint64_t n = counts.at(k);
        std::vector<std::string> row;
        for (const auto& item : query.selectItems) {
            switch (item.kind) {
                case SelectItem::Kind::Column: row.push_back(std::to_string(k)); break;
                case SelectItem::Kind::Sum: row.push_back(std::to_string(sums[item.column.index][k])); break;
                case SelectItem::Kind::Avg:
                    row.push_back(formatDouble(static_cast<double>(sums[item.column.index][k]) / static_cast<double>(n)));
                    break;
                default: row.push_back(std::to_string(n)); break;
            }
        }
        result.rows.push_back(std::move(row));
    }
    return true;
}

// Generic hash aggregation over any key types (including STRING and multi-column
// keys), any aggregates, with or without WHERE. Groups come out sorted by key.
MiniSQLResult executeGroupedQuery(Engine& engine, Table& table, const ParsedQuery& query,
                                  const std::vector<ColumnRef>& keys) {
    MiniSQLResult result;
    if (tryFastGroupBy(engine, table, query, keys, result)) return result;

    struct Group {
        std::vector<ColValue> key;
        std::vector<AggregateState> aggs;
    };
    std::vector<Group> groups;
    std::unordered_map<std::string, size_t> index;

    const auto rowIDs = executeWhere(table, query);
    std::string encoded;
    std::vector<ColValue> key(keys.size());
    for (uint32_t rowID : rowIDs) {
        encoded.clear();
        bool live = true;
        for (size_t k = 0; k < keys.size() && live; ++k) {
            auto v = table.fetchTypedValue(rowID, keys[k].index);
            if (!v) {
                live = false;
                break;
            }
            key[k] = std::move(*v);
            // Self-delimiting encoding: type tag + fixed-width bytes, or length-prefixed string.
            encoded.push_back(static_cast<char>(key[k].type));
            if (key[k].type == ColType::STRING) {
                const uint32_t len = static_cast<uint32_t>(key[k].str.size());
                encoded.append(reinterpret_cast<const char*>(&len), sizeof(len));
                encoded.append(key[k].str);
            } else {
                encoded.append(reinterpret_cast<const char*>(&key[k].i64),
                               colValueBytes(key[k].type));
            }
        }
        if (!live) continue;

        auto [it, inserted] = index.emplace(encoded, groups.size());
        if (inserted) groups.push_back({key, std::vector<AggregateState>(query.selectItems.size())});
        Group& g = groups[it->second];
        for (size_t i = 0; i < query.selectItems.size(); ++i) {
            const auto& item = query.selectItems[i];
            if (!item.isAggregate()) continue;
            if (item.kind == SelectItem::Kind::CountStar) {
                ++g.aggs[i].count;
            } else if (auto cell = table.fetchTypedValue(rowID, item.column.index)) {
                g.aggs[i].add(*cell);
            }
        }
    }

    if (g_trace) {
        std::string keyText;
        for (const auto& k : keys) keyText += (keyText.empty() ? "" : ", ") + k.text;
        trace("Aggregate: CPU hash aggregation on (" + keyText + "), " + std::to_string(groups.size()) + " groups");
    }
    std::sort(groups.begin(), groups.end(), [](const Group& a, const Group& b) {
        for (size_t k = 0; k < a.key.size(); ++k) {
            if (a.key[k] < b.key[k]) return true;
            if (b.key[k] < a.key[k]) return false;
        }
        return false;
    });

    setGroupedHeaders(table, query, result);
    for (const auto& g : groups) {
        std::vector<std::string> row;
        for (size_t i = 0; i < query.selectItems.size(); ++i) {
            const auto& item = query.selectItems[i];
            if (item.isAggregate()) {
                row.push_back(formatAggregate(table, item, g.aggs[i]));
            } else {
                size_t k = 0;
                while (keys[k].index != item.column.index) ++k;
                row.push_back(formatColValue(g.key[k]));
            }
        }
        result.rows.push_back(std::move(row));
    }
    return result;
}

bool tableExists(const std::string& tableName) {
    const std::string tablePath = tableName + ".mdb";
    return access(tablePath.c_str(), F_OK) == 0;
}

Table& openExistingTable(Engine& engine, const std::string& tableName) {
    if (!tableExists(tableName))
        throw std::invalid_argument("table does not exist");
    return engine.openTable(tableName);
}

MiniSQLResult rowsAffected(size_t n) {
    MiniSQLResult result;
    result.headers.push_back("rows_affected");
    result.types.push_back(ColType::INT64);
    result.rows.push_back({std::to_string(n)});
    return result;
}

MiniSQLResult executeCreateTable(Engine& engine, const ParsedStatement& stmt) {
    const std::string& name = stmt.query.tableName;
    if (tableExists(name))
        throw std::invalid_argument("table already exists");
    engine.createTypedTable(name, stmt.columnTypes);

    MiniSQLResult result;
    result.headers.push_back("created");
    result.types.push_back(ColType::STRING);
    result.rows.push_back({name});
    return result;
}

MiniSQLResult executeInsert(Engine& engine, const ParsedStatement& stmt) {
    Table& table = openExistingTable(engine, stmt.query.tableName);

    // Coerce every row before writing any, so a bad literal leaves the table untouched.
    std::vector<std::vector<ColValue>> rows;
    rows.reserve(stmt.insertRows.size());
    for (const auto& literals : stmt.insertRows) {
        if (literals.size() != table.numColumns()) {
            throw std::invalid_argument("expected " + std::to_string(table.numColumns()) +
                                        " values per row, got " + std::to_string(literals.size()));
        }
        std::vector<ColValue> row;
        row.reserve(literals.size());
        for (size_t c = 0; c < literals.size(); ++c)
            row.push_back(coerceLiteral(literals[c], table.columnFile(static_cast<uint16_t>(c)).colType(), c));
        rows.push_back(std::move(row));
    }

    // One WAL transaction: a crash mid-statement never leaves a partial INSERT.
    table.applyAtomic({}, rows);
    return rowsAffected(rows.size());
}

MiniSQLResult executeDelete(Engine& engine, const ParsedStatement& stmt) {
    Table& table = openExistingTable(engine, stmt.query.tableName);
    const auto rowIDs = executeWhere(table, stmt.query);
    table.applyAtomic(rowIDs, {});
    return rowsAffected(rowIDs.size());
}

// UPDATE is copy-on-write: matching rows are deleted and re-inserted with the new
// values in one WAL transaction, so updated rows receive new row IDs.
MiniSQLResult executeUpdate(Engine& engine, const ParsedStatement& stmt) {
    Table& table = openExistingTable(engine, stmt.query.tableName);

    std::vector<std::pair<uint16_t, ColValue>> sets;
    for (const auto& a : stmt.assignments) {
        if (a.column.index >= table.numColumns())
            throw std::invalid_argument("column " + a.column.text + " out of bounds");
        sets.emplace_back(a.column.index,
                          coerceLiteral(a.literal, table.columnFile(a.column.index).colType(), a.column.index));
    }

    const auto rowIDs = executeWhere(table, stmt.query);
    std::vector<std::vector<ColValue>> newRows;
    newRows.reserve(rowIDs.size());
    for (uint32_t rowID : rowIDs) {
        auto old = table.fetchTypedRow(rowID);
        std::vector<ColValue> row;
        row.reserve(old.size());
        for (auto& cell : old) {
            if (!cell) throw std::runtime_error("row " + std::to_string(rowID) + " vanished during UPDATE");
            row.push_back(std::move(*cell));
        }
        for (const auto& [col, value] : sets) row[col] = value;
        newRows.push_back(std::move(row));
    }

    table.applyAtomic(rowIDs, newRows);
    return rowsAffected(rowIDs.size());
}

MiniSQLResult executeDescribe(Engine& engine, const ParsedStatement& stmt) {
    Table& table = openExistingTable(engine, stmt.query.tableName);
    MiniSQLResult result;
    result.headers = {"column", "type"};
    result.types = {ColType::STRING, ColType::STRING};
    for (uint16_t c = 0; c < table.numColumns(); ++c)
        result.rows.push_back({"c" + std::to_string(c), colTypeName(table.columnFile(c).colType())});
    return result;
}

void applyOrderAndLimit(const ParsedQuery& query, MiniSQLResult& result,
                        bool limitAlreadyApplied) {
    if (!query.orderBy.empty()) {
        struct ResolvedKey {
            size_t idx;
            bool numeric;
            bool descending;
        };
        std::vector<ResolvedKey> keys;
        for (const auto& key : query.orderBy) {
            size_t idx = 0;
            if (key.position != 0) {
                if (key.position > result.headers.size())
                    throw std::invalid_argument("ORDER BY position out of range");
                idx = key.position - 1;
            } else {
                auto it = std::find(result.headers.begin(), result.headers.end(), key.header);
                if (it == result.headers.end())
                    throw std::invalid_argument("ORDER BY " + key.header + " must appear in the SELECT list");
                idx = static_cast<size_t>(it - result.headers.begin());
            }
            keys.push_back({idx, result.types[idx] != ColType::STRING, key.descending});
        }

        // Empty cells (e.g. MIN over no rows) sort before any value.
        auto compareCell = [](const std::string& a, const std::string& b, bool numeric) -> int {
            if (a.empty() || b.empty()) return a.empty() == b.empty() ? 0 : (a.empty() ? -1 : 1);
            if (numeric) {
                const long double x = std::strtold(a.c_str(), nullptr);
                const long double y = std::strtold(b.c_str(), nullptr);
                return x < y ? -1 : (y < x ? 1 : 0);
            }
            return a.compare(b) < 0 ? -1 : (a == b ? 0 : 1);
        };
        std::stable_sort(result.rows.begin(), result.rows.end(),
                         [&](const std::vector<std::string>& lhs, const std::vector<std::string>& rhs) {
                             for (const auto& key : keys) {
                                 int cmp = compareCell(lhs[key.idx], rhs[key.idx], key.numeric);
                                 if (key.descending) cmp = -cmp;
                                 if (cmp != 0) return cmp < 0;
                             }
                             return false;
                         });
    }

    if (!limitAlreadyApplied && (query.hasLimit || query.offset > 0)) {
        const size_t begin = static_cast<size_t>(std::min<uint64_t>(query.offset, result.rows.size()));
        size_t end = result.rows.size();
        if (query.hasLimit) end = static_cast<size_t>(std::min<uint64_t>(begin + query.limit, end));
        result.rows = std::vector<std::vector<std::string>>(result.rows.begin() + begin,
                                                            result.rows.begin() + end);
    }
}

MiniSQLResult executeSelect(Engine& engine, const ParsedQuery& query) {
    Table& table = openExistingTable(engine, query.tableName);
    validateQueryShape(table, query);

    bool aggregateQuery = false;
    for (const auto& item : query.selectItems) aggregateQuery = aggregateQuery || item.isAggregate();

    MiniSQLResult result;
    bool limitApplied = false;
    if (query.distinct) {
        // DISTINCT == GROUP BY every selected column.
        ParsedQuery grouped = query;
        grouped.distinct = false;
        if (grouped.selectItems.size() == 1 && grouped.selectItems[0].kind == SelectItem::Kind::Star) {
            grouped.selectItems.clear();
            for (uint16_t c = 0; c < table.numColumns(); ++c) {
                SelectItem item;
                item.column = {c, "c" + std::to_string(c)};
                grouped.selectItems.push_back(item);
            }
        }
        for (const auto& item : grouped.selectItems) {
            const bool seen = std::any_of(grouped.groupBy.begin(), grouped.groupBy.end(),
                                          [&](const ColumnRef& k) { return k.index == item.column.index; });
            if (!seen) grouped.groupBy.push_back(item.column);
        }
        result = executeGroupedQuery(engine, table, grouped, grouped.groupBy);
    } else if (!query.groupBy.empty()) {
        result = executeGroupedQuery(engine, table, query, query.groupBy);
    } else if (aggregateQuery) {
        result = executeScalarAggregateQuery(table, query);
    } else {
        result = executeProjectionQuery(table, query);
        limitApplied = query.orderBy.empty();
    }

    if (!query.orderBy.empty()) trace("Sort: " + std::to_string(query.orderBy.size()) + " key(s), " +
                                      std::to_string(result.rows.size()) + " rows");
    applyOrderAndLimit(query, result, limitApplied);
    return result;
}

const char* statementName(ParsedStatement::Kind kind) {
    switch (kind) {
        case ParsedStatement::Kind::Select: return "SELECT";
        case ParsedStatement::Kind::CreateTable: return "CREATE TABLE";
        case ParsedStatement::Kind::Insert: return "INSERT";
        case ParsedStatement::Kind::Delete: return "DELETE";
        case ParsedStatement::Kind::Update: return "UPDATE";
        case ParsedStatement::Kind::Describe: return "DESCRIBE";
    }
    return "?";
}

// EXPLAIN runs SELECTs for real (so timings and row counts are actual), and for
// DELETE / UPDATE evaluates only the WHERE clause — nothing is written.
MiniSQLResult executeExplain(Engine& engine, const ParsedStatement& stmt) {
    Trace t;
    g_trace = &t;
    struct Reset {
        ~Reset() { g_trace = nullptr; }
    } reset;

    const auto start = std::chrono::steady_clock::now();
    t.add(std::string("Statement: ") + statementName(stmt.kind));
    if (stmt.kind == ParsedStatement::Kind::Select || stmt.kind == ParsedStatement::Kind::Delete ||
        stmt.kind == ParsedStatement::Kind::Update) {
        Table& table = openExistingTable(engine, stmt.query.tableName);
        t.add("Table: '" + stmt.query.tableName + "' (" + std::to_string(table.rowCount()) + " live rows, " +
              std::to_string(table.numColumns()) + " columns; GPU " +
              (!table.useGPU() ? "disabled" : metalIsAvailable() ? "available" : "unavailable") +
              ", threshold " + std::to_string(table.gpuThreshold()) + " rows)");
        if (stmt.kind == ParsedStatement::Kind::Select) {
            const auto result = executeSelect(engine, stmt.query);
            t.add("Output: " + std::to_string(result.rows.size()) + " rows");
        } else {
            const auto ids = executeWhere(table, stmt.query);
            t.add(std::string("Write: would ") + (stmt.kind == ParsedStatement::Kind::Delete ? "delete " : "rewrite ") +
                  std::to_string(ids.size()) + " rows as one WAL transaction (not executed)");
        }
    } else {
        t.add("(no plan: statement is executed directly)");
    }
    t.add("Total: " + fmtMs(msSince(start)));

    MiniSQLResult result;
    result.headers = {"plan"};
    result.types = {ColType::STRING};
    for (auto& line : t.lines) result.rows.push_back({std::move(line)});
    return result;
}

} // namespace

MiniSQLResult executeMiniSQL(Engine& engine, const std::string& sqlText) {
    const ParsedStatement stmt = sql::parse(sqlText);
    if (stmt.explain) return executeExplain(engine, stmt);
    switch (stmt.kind) {
        case ParsedStatement::Kind::CreateTable: return executeCreateTable(engine, stmt);
        case ParsedStatement::Kind::Insert: return executeInsert(engine, stmt);
        case ParsedStatement::Kind::Delete: return executeDelete(engine, stmt);
        case ParsedStatement::Kind::Describe: return executeDescribe(engine, stmt);
        case ParsedStatement::Kind::Update: return executeUpdate(engine, stmt);
        case ParsedStatement::Kind::Select: break;
    }
    return executeSelect(engine, stmt.query);
}
