// Mini-SQL executor. Parsing lives in SqlParser, WHERE evaluation in WhereEval;
// this file validates statement shape against the table schema and executes.
#include "MiniSQL.hpp"

#include <algorithm>
#include <optional>
#include <cctype>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <fstream>
#include <unordered_map>
#include <sstream>
#include <stdexcept>
#include <string>
#include <unistd.h>
#include <utility>
#include <vector>

#include "Csv.hpp"
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

// ── transaction overlay ──────────────────────────────────────────────────────
// For a statement inside a transaction: g_txn is the transaction, g_changes its
// pending changes for the statement's table (null until it writes that table).
// Reads merge committed rows with pending ones; writes are buffered.
thread_local TxnHook* g_txn = nullptr;
thread_local TableChanges* g_changes = nullptr;
thread_local std::string g_tableName;

bool isPending(uint32_t rowID) { return g_changes && rowID >= g_changes->virtualBase; }

std::optional<ColValue> cellOf(Table& table, uint32_t rowID, uint16_t col) {
    if (isPending(rowID)) {
        const size_t i = rowID - g_changes->virtualBase;
        if (i >= g_changes->inserted.size() || !g_changes->insertedLive[i]) return std::nullopt;
        return g_changes->inserted[i][col];
    }
    return table.fetchTypedValue(rowID, col);
}

// Writes one statement's effect: applied atomically now (autocommit), or buffered
// into the transaction's pending changes.
void applyWrite(Table& table, const std::vector<uint32_t>& deletes, std::vector<std::vector<ColValue>> inserts) {
    if (!g_txn) {
        table.applyAtomic(deletes, inserts);
        return;
    }
    for (const auto& row : inserts) table.validateRow(row);
    TableChanges& ch = g_txn->changesForWrite(g_tableName, table);
    g_changes = &ch;
    for (uint32_t id : deletes) {
        if (id >= ch.virtualBase) {
            const size_t i = id - ch.virtualBase;
            if (i < ch.insertedLive.size()) ch.insertedLive[i] = false;
        } else if (table.isLive(id)) {
            ch.deletedBase.insert(id);
        }
    }
    for (auto& row : inserts) {
        ch.inserted.push_back(std::move(row));
        ch.insertedLive.push_back(true);
    }
}

// ── value formatting ─────────────────────────────────────────────────────────

// Shortest decimal that parses back to exactly the same value (like PostgreSQL 12+):
// 19.99 prints as "19.99", 0.1 + 0.2 as "0.30000000000000004". Lossless for COPY
// export → import and readable for display.
std::string formatDouble(double v) {
    char buf[64];
    for (int precision = 15; precision <= 17; ++precision) {
        std::snprintf(buf, sizeof(buf), "%.*g", precision, v);
        if (precision == 17 || std::strtod(buf, nullptr) == v) break;
    }
    return buf;
}

std::string formatFloat(float v) {
    char buf[64];
    for (int precision = 6; precision <= 9; ++precision) {
        std::snprintf(buf, sizeof(buf), "%.*g", precision, static_cast<double>(v));
        if (precision == 9 || std::strtof(buf, nullptr) == v) break;
    }
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
    switch (value.type) {
        case ColType::UINT32: return std::to_string(value.u32);
        case ColType::INT64: return std::to_string(value.i64);
        case ColType::FLOAT: return formatFloat(value.f32);
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

std::vector<ColType> columnTypes(const Table& table) {
    std::vector<ColType> types;
    types.reserve(table.numColumns());
    for (uint16_t c = 0; c < table.numColumns(); ++c) types.push_back(table.columnFile(c).colType());
    return types;
}

// `input` is the type of the aggregated column (ignored for COUNT(*)).
ColType aggregateType(ColType input, const SelectItem& item) {
    switch (item.kind) {
        case SelectItem::Kind::CountStar:
        case SelectItem::Kind::Count:
            return ColType::INT64;
        case SelectItem::Kind::Sum:
            return isIntegral(input) ? ColType::INT64 : ColType::DOUBLE;
        case SelectItem::Kind::Avg:
            return ColType::DOUBLE;
        default:
            return input;
    }
}

ColType inputType(const std::vector<ColType>& types, const SelectItem& item) {
    return item.kind == SelectItem::Kind::CountStar ? ColType::INT64 : types.at(item.column.index);
}

std::string formatAggregate(ColType input, const SelectItem& item, const AggregateState& state) {
    switch (item.kind) {
        case SelectItem::Kind::CountStar:
        case SelectItem::Kind::Count:
            return std::to_string(state.count);
        case SelectItem::Kind::Sum:
            if (isIntegral(input)) return formatInt128(state.intSum);
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

void validateColumnRef(const std::vector<ColType>& types, uint16_t colIdx) {
    if (colIdx >= types.size())
        throw std::invalid_argument("column index out of bounds");
}

void validateQueryShape(const std::vector<ColType>& types, const ParsedQuery& query) {
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
                validateColumnRef(types, item.column.index);
                break;
            case SelectItem::Kind::CountStar:
                ++aggregateCount;
                break;
            case SelectItem::Kind::Count:
            case SelectItem::Kind::Min:
            case SelectItem::Kind::Max:
                ++aggregateCount;
                validateColumnRef(types, item.column.index);
                break;
            case SelectItem::Kind::Sum:
            case SelectItem::Kind::Avg:
                ++aggregateCount;
                validateColumnRef(types, item.column.index);
                if (types[item.column.index] == ColType::STRING)
                    throw std::invalid_argument(item.header() + " requires a numeric column");
                break;
        }
    }

    if (hasStar && query.selectItems.size() != 1)
        throw std::invalid_argument("SELECT * cannot be combined with other projections");
    if (query.where) validateWhere(types, *query.where);

    if (query.distinct) {
        if (aggregateCount > 0 || !query.groupBy.empty())
            throw std::invalid_argument("SELECT DISTINCT cannot be combined with aggregates or GROUP BY");
        return;
    }

    if (!query.groupBy.empty()) {
        if (hasStar) throw std::invalid_argument("SELECT * with GROUP BY is not supported");
        for (const auto& key : query.groupBy) validateColumnRef(types, key.index);
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

void validateQueryShape(const Table& table, const ParsedQuery& query) {
    validateQueryShape(columnTypes(table), query);
}

// ── SELECT execution ─────────────────────────────────────────────────────────

// Matching row IDs, sorted: committed matches minus rows this transaction deleted,
// followed by matching pending rows (whose virtual IDs sort after every committed ID).
std::vector<uint32_t> mergePending(std::vector<uint32_t> ids, const WhereExpr* where) {
    if (!g_changes) return ids;
    if (!g_changes->deletedBase.empty())
        ids.erase(std::remove_if(ids.begin(), ids.end(),
                                 [](uint32_t id) { return g_changes->deletedBase.count(id) > 0; }),
                  ids.end());
    size_t pending = 0;
    for (size_t i = 0; i < g_changes->inserted.size(); ++i) {
        if (!g_changes->insertedLive[i]) continue;
        if (where && !rowMatches(*where, g_changes->inserted[i])) continue;
        ids.push_back(g_changes->virtualBase + static_cast<uint32_t>(i));
        ++pending;
    }
    trace("Transaction: " + std::to_string(g_changes->deletedBase.size()) + " pending deletes hidden, " +
          std::to_string(pending) + " pending rows matched");
    return ids;
}

std::vector<uint32_t> executeWhere(Table& table, const ParsedQuery& query) {
    const auto start = std::chrono::steady_clock::now();
    if (!query.where) {
        auto ids = allLiveRowIDs(table);
        trace("Filter: none (all " + std::to_string(ids.size()) + " live rows)");
        return mergePending(std::move(ids), nullptr);
    }
    std::vector<std::string> leaves;
    auto ids = evaluateWhere(table, *query.where, g_trace ? &leaves : nullptr);
    if (g_trace) {
        trace("Filter: " + whereToString(*query.where));
        for (const auto& leaf : leaves) trace("  -> " + leaf);
        trace("  matched " + std::to_string(ids.size()) + " rows in " + fmtMs(msSince(start)));
    }
    return mergePending(std::move(ids), query.where.get());
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
            auto v = cellOf(table, rowIDs[i], c);
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

// Aggregation runs over any row source: `n` rows, cell(i, col) -> std::optional<ColValue>
// (nullopt for a row that vanished), with column types `types`.
template <class Cell>
MiniSQLResult aggregateRows(size_t n, Cell cell, const std::vector<ColType>& types, const ParsedQuery& query) {
    trace("Aggregate: scalar over " + std::to_string(n) + " rows");
    MiniSQLResult result;
    std::vector<std::string> row;
    for (const auto& item : query.selectItems) {
        AggregateState state;
        if (item.kind == SelectItem::Kind::CountStar) {
            state.count = n;
        } else {
            for (size_t i = 0; i < n; ++i)
                if (auto v = cell(i, item.column.index)) state.add(*v);
        }
        const ColType in = inputType(types, item);
        result.headers.push_back(item.header());
        result.types.push_back(aggregateType(in, item));
        row.push_back(formatAggregate(in, item, state));
    }
    result.rows.push_back(std::move(row));
    return result;
}

MiniSQLResult executeScalarAggregateQuery(Table& table, const ParsedQuery& query) {
    const auto rowIDs = executeWhere(table, query);
    return aggregateRows(
        rowIDs.size(), [&](size_t i, uint16_t c) { return cellOf(table, rowIDs[i], c); }, columnTypes(table), query);
}

void setGroupedHeaders(const std::vector<ColType>& types, const ParsedQuery& query, MiniSQLResult& result) {
    for (const auto& item : query.selectItems) {
        result.headers.push_back(item.header());
        result.types.push_back(item.isAggregate() ? aggregateType(inputType(types, item), item)
                                                  : types.at(item.column.index));
    }
}

void setGroupedHeaders(const Table& table, const ParsedQuery& query, MiniSQLResult& result) {
    setGroupedHeaders(columnTypes(table), query, result);
}

// GPU-capable fast path: one UINT32 key, no WHERE, only COUNT / SUM / AVG over
// UINT32 columns. Routes through GroupBy::countByKey / sumByKey, which dispatch to
// the Metal group-by kernel for large tables.
bool tryFastGroupBy(Engine& engine, Table& table, const ParsedQuery& query,
                    const std::vector<ColumnRef>& keys, MiniSQLResult& result) {
    if (keys.size() != 1 || query.where) return false;
    if (g_changes && g_changes->hasChanges()) return false;  // must see pending rows: generic path
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
template <class Cell>
MiniSQLResult groupRows(size_t n, Cell cell, const std::vector<ColType>& types, const ParsedQuery& query,
                        const std::vector<ColumnRef>& keys) {
    MiniSQLResult result;
    struct Group {
        std::vector<ColValue> key;
        std::vector<AggregateState> aggs;
    };
    std::vector<Group> groups;
    std::unordered_map<std::string, size_t> index;

    std::string encoded;
    std::vector<ColValue> key(keys.size());
    for (size_t rowID = 0; rowID < n; ++rowID) {
        encoded.clear();
        bool live = true;
        for (size_t k = 0; k < keys.size() && live; ++k) {
            auto v = cell(rowID, keys[k].index);
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
            } else if (auto v = cell(rowID, item.column.index)) {
                g.aggs[i].add(*v);
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

    setGroupedHeaders(types, query, result);
    for (const auto& g : groups) {
        std::vector<std::string> row;
        for (size_t i = 0; i < query.selectItems.size(); ++i) {
            const auto& item = query.selectItems[i];
            if (item.isAggregate()) {
                row.push_back(formatAggregate(inputType(types, item), item, g.aggs[i]));
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

MiniSQLResult executeGroupedQuery(Engine& engine, Table& table, const ParsedQuery& query,
                                  const std::vector<ColumnRef>& keys) {
    MiniSQLResult result;
    if (tryFastGroupBy(engine, table, query, keys, result)) return result;
    const auto rowIDs = executeWhere(table, query);
    return groupRows(
        rowIDs.size(), [&](size_t i, uint16_t c) { return cellOf(table, rowIDs[i], c); }, columnTypes(table), query,
        keys);
}

Table& openExistingTable(Engine& engine, const std::string& tableName) {
    if (!engine.tableExists(tableName))
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
    if (engine.tableExists(name))
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
    applyWrite(table, {}, std::move(rows));
    return rowsAffected(stmt.insertRows.size());
}

MiniSQLResult executeDelete(Engine& engine, const ParsedStatement& stmt) {
    Table& table = openExistingTable(engine, stmt.query.tableName);
    const auto rowIDs = executeWhere(table, stmt.query);
    applyWrite(table, rowIDs, {});
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
        std::vector<ColValue> row;
        row.reserve(table.numColumns());
        for (uint16_t c = 0; c < table.numColumns(); ++c) {
            auto cell = cellOf(table, rowID, c);
            if (!cell) throw std::runtime_error("row " + std::to_string(rowID) + " vanished during UPDATE");
            row.push_back(std::move(*cell));
        }
        for (const auto& [col, value] : sets) row[col] = value;
        newRows.push_back(std::move(row));
    }

    const size_t n = rowIDs.size();
    applyWrite(table, rowIDs, std::move(newRows));
    return rowsAffected(n);
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

// ── JOIN execution ───────────────────────────────────────────────────────────
// SELECT ... FROM a [AS x] [INNER] JOIN b [AS y] ON x.cN = y.cM [AND ...] [JOIN ...]
//
// Plan: every column reference is resolved to a position in the combined row (the
// tables' columns side by side, in FROM order) and canonicalized to "alias.cN".
// WHERE conjuncts that touch a single table are pushed into that table's scan, so
// they keep zone-map pruning and the hybrid GPU/CPU scan; the rest run row-wise on
// joined rows. Tables are joined left-deep with a hash join (build on the joined
// table, probe with the rows so far); then projection / aggregation / GROUP BY /
// DISTINCT / ORDER BY / LIMIT run exactly as for one table. Inside a transaction
// each table's scan sees that transaction's pending changes.

struct JoinSource {
    std::string name;
    std::string alias;
    Table* table = nullptr;
    uint16_t offset = 0;  // first column of this table in the combined row
    uint16_t width = 0;
    std::vector<std::shared_ptr<WhereExpr>> pushed;  // conjuncts on this table only (local columns)
    std::vector<bool> needed;                         // local columns to materialize
};

struct JoinPlan {
    std::vector<JoinSource> sources;
    std::vector<ColType> types;  // combined row layout
    ParsedQuery query;           // column refs resolved to combined positions
    // keys[j] joins sources[j + 1]: (combined column of an earlier table, local column of sources[j + 1])
    std::vector<std::vector<std::pair<uint16_t, uint16_t>>> keys;
    std::shared_ptr<WhereExpr> residual;  // conjuncts spanning tables (combined columns)
};

std::vector<std::string> statementTables(const ParsedQuery& query) {
    std::vector<std::string> names{query.tableName};
    for (const auto& j : query.joins) names.push_back(j.tableName);
    return names;
}

std::shared_ptr<WhereExpr> cloneWhere(const WhereExpr& e) {
    auto copy = std::make_shared<WhereExpr>(e);
    for (auto& child : copy->children) child = cloneWhere(*child);
    return copy;
}

template <class F>
void forEachLeaf(WhereExpr& e, F&& f) {
    if (e.kind == WhereExpr::Kind::And || e.kind == WhereExpr::Kind::Or || e.kind == WhereExpr::Kind::Not) {
        for (auto& child : e.children) forEachLeaf(*child, f);
    } else {
        f(e);
    }
}

char joinKeyClass(ColType t) {
    switch (t) {
        case ColType::UINT32:
        case ColType::INT64: return 'I';
        case ColType::FLOAT:
        case ColType::DOUBLE: return 'F';
        case ColType::STRING: return 'S';
    }
    return '?';
}

JoinPlan planJoin(Engine& engine, const ParsedQuery& query) {
    JoinPlan plan;
    auto addSource = [&](const std::string& name, const std::string& alias) {
        for (const auto& s : plan.sources)
            if (s.alias == alias)
                throw std::invalid_argument("table name \"" + alias + "\" specified more than once (give it an alias)");
        if (!engine.tableExists(name)) throw std::invalid_argument("table '" + name + "' does not exist");
        JoinSource src;
        src.name = name;
        src.alias = alias;
        src.table = &engine.openTable(name);
        if (plan.types.size() + src.table->numColumns() > 0xFFFFu)
            throw std::invalid_argument("too many columns in join");
        src.offset = static_cast<uint16_t>(plan.types.size());
        src.width = src.table->numColumns();
        src.needed.assign(src.width, false);
        for (ColType t : columnTypes(*src.table)) plan.types.push_back(t);
        plan.sources.push_back(std::move(src));
    };
    addSource(query.tableName, query.alias.empty() ? query.tableName : query.alias);
    for (const auto& j : query.joins) addSource(j.tableName, j.alias.empty() ? j.tableName : j.alias);

    auto sourceOf = [&](uint16_t combined) {
        size_t s = 0;
        while (s + 1 < plan.sources.size() && combined >= plan.sources[s + 1].offset) ++s;
        return s;
    };
    // Resolves `ref` in place; returns the index of its table.
    auto resolve = [&](ColumnRef& ref) -> size_t {
        const std::string col = "c" + std::to_string(ref.index);
        size_t found = plan.sources.size();
        if (!ref.qualifier.empty()) {
            for (size_t s = 0; s < plan.sources.size(); ++s)
                if (plan.sources[s].alias == ref.qualifier) found = s;
            if (found == plan.sources.size())
                throw std::invalid_argument("missing FROM-clause entry for table \"" + ref.qualifier + "\"");
            if (ref.index >= plan.sources[found].width)
                throw std::invalid_argument("column " + ref.qualifier + "." + col + " does not exist");
        } else {
            for (size_t s = 0; s < plan.sources.size(); ++s) {
                if (ref.index >= plan.sources[s].width) continue;
                if (found != plan.sources.size())
                    throw std::invalid_argument("column reference \"" + col + "\" is ambiguous (qualify it, e.g. " +
                                                plan.sources[found].alias + "." + col + ")");
                found = s;
            }
            if (found == plan.sources.size()) throw std::invalid_argument("column " + col + " does not exist");
        }
        const JoinSource& src = plan.sources[found];
        ref.qualifier = src.alias;
        ref.text = src.alias + "." + col;
        ref.index = static_cast<uint16_t>(src.offset + ref.index);
        return found;
    };

    ParsedQuery& q = plan.query;
    q = query;
    q.joins.clear();
    q.selectItems.clear();
    for (const auto& item : query.selectItems) {
        if (item.kind == SelectItem::Kind::Star) {  // * or alias.*: expand to that table's columns
            bool matched = false;
            for (const auto& src : plan.sources) {
                if (!item.column.qualifier.empty() && item.column.qualifier != src.alias) continue;
                matched = true;
                for (uint16_t c = 0; c < src.width; ++c) {
                    SelectItem col;
                    col.column = {c, "c" + std::to_string(c), src.alias};
                    resolve(col.column);
                    q.selectItems.push_back(col);
                }
            }
            if (!matched)
                throw std::invalid_argument("missing FROM-clause entry for table \"" + item.column.qualifier + "\"");
            continue;
        }
        SelectItem resolved = item;
        if (resolved.kind != SelectItem::Kind::CountStar) resolve(resolved.column);
        q.selectItems.push_back(resolved);
    }
    if (q.where) {
        q.where = cloneWhere(*query.where);
        forEachLeaf(*q.where, [&](WhereExpr& leaf) {
            resolve(leaf.column);
            if (leaf.kind == WhereExpr::Kind::ColumnCompare) resolve(leaf.rhs);
        });
    }
    for (auto& key : q.groupBy) resolve(key);
    for (auto& key : q.orderBy) {
        if (key.position != 0) continue;
        if (key.item.kind != SelectItem::Kind::CountStar) resolve(key.item.column);
        key.header = key.item.header();
    }

    for (size_t j = 0; j < query.joins.size(); ++j) {
        const size_t right = j + 1;
        std::vector<std::pair<uint16_t, uint16_t>> keys;
        for (auto [l, r] : query.joins[j].on) {
            size_t sl = resolve(l);
            size_t sr = resolve(r);
            if (sl == right && sr < right) {
                std::swap(l, r);
                std::swap(sl, sr);
            }
            if (!(sr == right && sl < right))
                throw std::invalid_argument("JOIN condition " + l.text + " = " + r.text + " must compare a column of \"" +
                                            plan.sources[right].alias + "\" with a column of an earlier table");
            if (joinKeyClass(plan.types[l.index]) != joinKeyClass(plan.types[r.index]))
                throw std::invalid_argument(std::string("cannot join ") + colTypeName(plan.types[l.index]) + " " +
                                            l.text + " with " + colTypeName(plan.types[r.index]) + " " + r.text);
            keys.emplace_back(l.index, static_cast<uint16_t>(r.index - plan.sources[right].offset));
        }
        plan.keys.push_back(std::move(keys));
    }

    validateQueryShape(plan.types, q);

    // Split WHERE: single-table conjuncts go to that table's scan.
    if (q.where) {
        std::vector<std::shared_ptr<WhereExpr>> conjuncts;
        if (q.where->kind == WhereExpr::Kind::And) conjuncts = q.where->children;
        else conjuncts.push_back(q.where);
        std::vector<std::shared_ptr<WhereExpr>> rest;
        for (const auto& c : conjuncts) {
            std::vector<bool> touches(plan.sources.size(), false);
            forEachLeaf(*c, [&](WhereExpr& leaf) {
                touches[sourceOf(leaf.column.index)] = true;
                if (leaf.kind == WhereExpr::Kind::ColumnCompare) touches[sourceOf(leaf.rhs.index)] = true;
            });
            if (std::count(touches.begin(), touches.end(), true) != 1) {
                rest.push_back(c);
                continue;
            }
            JoinSource& src = plan.sources[std::find(touches.begin(), touches.end(), true) - touches.begin()];
            auto local = cloneWhere(*c);
            auto toLocal = [&](ColumnRef& ref) {
                ref.index = static_cast<uint16_t>(ref.index - src.offset);
                ref.text = "c" + std::to_string(ref.index);
                ref.qualifier.clear();
            };
            forEachLeaf(*local, [&](WhereExpr& leaf) {
                toLocal(leaf.column);
                if (leaf.kind == WhereExpr::Kind::ColumnCompare) toLocal(leaf.rhs);
            });
            src.pushed.push_back(local);
        }
        if (rest.size() == 1) {
            plan.residual = rest.front();
        } else if (!rest.empty()) {
            plan.residual = std::make_shared<WhereExpr>();
            plan.residual->kind = WhereExpr::Kind::And;
            plan.residual->children = rest;
        }
    }

    // Materialize only the columns the statement reads after the scans.
    auto need = [&](uint16_t combined) {
        JoinSource& src = plan.sources[sourceOf(combined)];
        src.needed[combined - src.offset] = true;
    };
    for (const auto& item : q.selectItems)
        if (item.kind != SelectItem::Kind::CountStar) need(item.column.index);
    for (const auto& key : q.groupBy) need(key.index);
    for (const auto& key : q.orderBy)
        if (key.position == 0 && key.item.kind != SelectItem::Kind::CountStar) need(key.item.column.index);
    for (size_t j = 0; j < plan.keys.size(); ++j)
        for (const auto& [l, r] : plan.keys[j]) {
            need(l);
            plan.sources[j + 1].needed[r] = true;
        }
    if (plan.residual)
        forEachLeaf(*plan.residual, [&](WhereExpr& leaf) {
            need(leaf.column.index);
            if (leaf.kind == WhereExpr::Kind::ColumnCompare) need(leaf.rhs.index);
        });
    return plan;
}

// Live rows of one table (its pushed-down filter applied), at local column positions.
std::vector<std::vector<ColValue>> scanJoinSource(const JoinSource& src) {
    TableChanges* const saved = g_changes;
    struct Restore {
        TableChanges* saved;
        ~Restore() { g_changes = saved; }
    } restore{saved};
    g_changes = g_txn ? g_txn->changesFor(src.name) : nullptr;

    ParsedQuery scan;
    scan.tableName = src.name;
    if (src.pushed.size() == 1) {
        scan.where = src.pushed.front();
    } else if (!src.pushed.empty()) {
        scan.where = std::make_shared<WhereExpr>();
        scan.where->kind = WhereExpr::Kind::And;
        scan.where->children = src.pushed;
    }
    trace("Scan: '" + src.name + "'" + (src.alias != src.name ? " AS " + src.alias : std::string()));
    const auto ids = executeWhere(*src.table, scan);

    std::vector<std::vector<ColValue>> rows;
    rows.reserve(ids.size());
    for (uint32_t id : ids) {
        std::vector<ColValue> row(src.width);
        bool live = true;
        for (uint16_t c = 0; c < src.width && live; ++c) {
            if (!src.needed[c]) continue;
            auto v = cellOf(*src.table, id, c);
            if (!v) live = false;
            else row[c] = std::move(*v);
        }
        if (live) rows.push_back(std::move(row));
    }
    return rows;
}

// Equality key over `cols` of `row`: integers compare as integers, FLOAT / DOUBLE as
// doubles, strings bytewise. NaN equals nothing (returns false).
bool encodeJoinKey(const std::vector<ColValue>& row, const std::vector<uint16_t>& cols, std::string& out) {
    out.clear();
    for (uint16_t c : cols) {
        const ColValue& v = row[c];
        switch (v.type) {
            case ColType::UINT32:
            case ColType::INT64: {
                const int64_t x = v.type == ColType::UINT32 ? static_cast<int64_t>(v.u32) : v.i64;
                out.push_back('I');
                out.append(reinterpret_cast<const char*>(&x), sizeof(x));
                break;
            }
            case ColType::FLOAT:
            case ColType::DOUBLE: {
                double x = v.type == ColType::FLOAT ? static_cast<double>(v.f32) : v.f64;
                if (x != x) return false;
                if (x == 0) x = 0;  // -0 joins 0
                out.push_back('F');
                out.append(reinterpret_cast<const char*>(&x), sizeof(x));
                break;
            }
            case ColType::STRING: {
                const uint32_t len = static_cast<uint32_t>(v.str.size());
                out.push_back('S');
                out.append(reinterpret_cast<const char*>(&len), sizeof(len));
                out.append(v.str);
                break;
            }
        }
    }
    return true;
}

MiniSQLResult executeJoinSelect(Engine& engine, const ParsedQuery& query) {
    JoinPlan plan = planJoin(engine, query);
    const ParsedQuery& q = plan.query;
    const size_t width = plan.types.size();

    // Rows so far, at combined positions (columns of not-yet-joined tables unset).
    std::vector<std::vector<ColValue>> rows;
    for (auto& r : scanJoinSource(plan.sources[0])) {
        r.resize(width);
        rows.push_back(std::move(r));
    }

    std::string key;
    for (size_t j = 0; j < plan.keys.size(); ++j) {
        const JoinSource& src = plan.sources[j + 1];
        const auto start = std::chrono::steady_clock::now();
        const auto build = scanJoinSource(src);
        std::vector<uint16_t> leftCols;
        std::vector<uint16_t> rightCols;
        for (const auto& [l, r] : plan.keys[j]) {
            leftCols.push_back(l);
            rightCols.push_back(r);
        }
        std::unordered_map<std::string, std::vector<uint32_t>> table;
        table.reserve(build.size());
        for (uint32_t i = 0; i < build.size(); ++i)
            if (encodeJoinKey(build[i], rightCols, key)) table[key].push_back(i);

        std::vector<std::vector<ColValue>> joined;
        for (auto& left : rows) {
            if (!encodeJoinKey(left, leftCols, key)) continue;
            auto it = table.find(key);
            if (it == table.end()) continue;
            for (uint32_t i : it->second) {
                std::vector<ColValue> out = left;
                for (uint16_t c = 0; c < src.width; ++c)
                    if (src.needed[c]) out[src.offset + c] = build[i][c];
                joined.push_back(std::move(out));
            }
        }
        if (g_trace) {
            std::string on;
            for (const auto& [l, r] : query.joins[j].on) on += (on.empty() ? "" : " AND ") + l.text + " = " + r.text;
            trace("Join: CPU hash join ON " + on + " (build " + std::to_string(build.size()) + " rows of '" +
                  src.name + "', probe " + std::to_string(rows.size()) + ") -> " + std::to_string(joined.size()) +
                  " rows in " + fmtMs(msSince(start)));
        }
        rows = std::move(joined);
    }

    if (plan.residual) {
        const size_t before = rows.size();
        rows.erase(std::remove_if(rows.begin(), rows.end(),
                                  [&](const std::vector<ColValue>& r) { return !rowMatches(*plan.residual, r); }),
                   rows.end());
        trace("Filter (joined rows): " + whereToString(*plan.residual) + " -> " + std::to_string(rows.size()) +
              " of " + std::to_string(before) + " rows");
    }

    auto cell = [&](size_t i, uint16_t c) { return std::optional<ColValue>(rows[i][c]); };
    bool aggregateQuery = false;
    for (const auto& item : q.selectItems) aggregateQuery = aggregateQuery || item.isAggregate();

    MiniSQLResult result;
    if (q.distinct) {
        ParsedQuery grouped = q;
        grouped.distinct = false;
        for (const auto& item : grouped.selectItems) {
            const bool seen = std::any_of(grouped.groupBy.begin(), grouped.groupBy.end(),
                                          [&](const ColumnRef& k) { return k.index == item.column.index; });
            if (!seen) grouped.groupBy.push_back(item.column);
        }
        result = groupRows(rows.size(), cell, plan.types, grouped, grouped.groupBy);
    } else if (!q.groupBy.empty()) {
        result = groupRows(rows.size(), cell, plan.types, q, q.groupBy);
    } else if (aggregateQuery) {
        result = aggregateRows(rows.size(), cell, plan.types, q);
    } else {
        // Projection; ORDER BY columns that are not selected ride along hidden.
        std::vector<uint16_t> cols;
        for (const auto& item : q.selectItems) {
            cols.push_back(item.column.index);
            result.headers.push_back(item.header());
            result.types.push_back(plan.types[item.column.index]);
        }
        const size_t visible = cols.size();
        for (const auto& key : q.orderBy) {
            if (key.position != 0 || key.item.kind != SelectItem::Kind::Column) continue;
            if (std::find(result.headers.begin(), result.headers.end(), key.header) != result.headers.end()) continue;
            cols.push_back(key.item.column.index);
            result.headers.push_back(key.header);
            result.types.push_back(plan.types[key.item.column.index]);
        }
        trace("Project: " + std::to_string(visible) + " column(s) over " + std::to_string(rows.size()) + " rows");
        result.rows.reserve(rows.size());
        for (const auto& r : rows) {
            std::vector<std::string> out;
            out.reserve(cols.size());
            for (uint16_t c : cols) out.push_back(formatColValue(r[c]));
            result.rows.push_back(std::move(out));
        }
        if (!q.orderBy.empty())
            trace("Sort: " + std::to_string(q.orderBy.size()) + " key(s), " + std::to_string(result.rows.size()) + " rows");
        applyOrderAndLimit(q, result, false);
        result.headers.resize(visible);
        result.types.resize(visible);
        for (auto& row : result.rows) row.resize(visible);
        return result;
    }
    if (!q.orderBy.empty())
        trace("Sort: " + std::to_string(q.orderBy.size()) + " key(s), " + std::to_string(result.rows.size()) + " rows");
    applyOrderAndLimit(q, result, false);
    return result;
}

// Locks every table a statement reads, in a fixed (address) order so two joins
// over the same tables can never deadlock; a table named twice is locked once.
class StatementLocks {
public:
    StatementLocks(Engine& engine, const std::vector<std::string>& names) {
        std::vector<std::mutex*> mutexes;
        for (const auto& name : names) {
            (void)engine.resolveTableBase(name);  // sandbox check before locking
            mutexes.push_back(&engine.tableMutex(name));
        }
        std::sort(mutexes.begin(), mutexes.end());
        mutexes.erase(std::unique(mutexes.begin(), mutexes.end()), mutexes.end());
        for (std::mutex* m : mutexes) locks_.emplace_back(*m);
    }

private:
    std::vector<std::unique_lock<std::mutex>> locks_;
};

MiniSQLResult executeSelect(Engine& engine, const ParsedQuery& query) {
    if (!query.joins.empty()) return executeJoinSelect(engine, query);
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
        // ORDER BY may name columns that are not projected: carry them as hidden
        // trailing columns for the sort and drop them afterwards.
        ParsedQuery withHidden = query;
        const bool star = query.selectItems.size() == 1 && query.selectItems[0].kind == SelectItem::Kind::Star;
        if (!star) {
            for (const auto& key : query.orderBy) {
                if (key.position != 0 || key.header.size() < 2 || key.header[0] != 'c') continue;
                const bool projected = std::any_of(query.selectItems.begin(), query.selectItems.end(),
                                                   [&](const SelectItem& it) { return it.header() == key.header; });
                const bool numeric = std::all_of(key.header.begin() + 1, key.header.end(),
                                                 [](char ch) { return std::isdigit(static_cast<unsigned char>(ch)); });
                if (projected || !numeric) continue;
                SelectItem hidden;
                hidden.column = {static_cast<uint16_t>(std::stoul(key.header.substr(1))), key.header};
                validateColumnRef(table, hidden.column.index);
                withHidden.selectItems.push_back(hidden);
            }
        }
        const size_t visible = query.selectItems.size();
        result = executeProjectionQuery(table, withHidden);
        limitApplied = query.orderBy.empty();
        if (!star && withHidden.selectItems.size() > visible) {
            if (!query.orderBy.empty()) trace("Sort: " + std::to_string(query.orderBy.size()) + " key(s), " +
                                              std::to_string(result.rows.size()) + " rows");
            applyOrderAndLimit(query, result, limitApplied);
            result.headers.resize(visible);
            result.types.resize(visible);
            for (auto& row : result.rows) row.resize(visible);
            return result;
        }
    }

    if (!query.orderBy.empty()) trace("Sort: " + std::to_string(query.orderBy.size()) + " key(s), " +
                                      std::to_string(result.rows.size()) + " rows");
    applyOrderAndLimit(query, result, limitApplied);
    return result;
}

// COPY '<table>' FROM '<file>': parse the whole file first (so a bad record leaves
// the table untouched), insert as one WAL transaction, then checkpoint so a large
// import does not leave a large WAL behind.
MiniSQLResult executeCopyFrom(Engine& engine, const ParsedStatement& stmt) {
    Table& table = openExistingTable(engine, stmt.query.tableName);
    std::ifstream in(engine.resolveFile(stmt.copyFile), std::ios::binary);
    if (!in) throw std::invalid_argument("cannot open '" + stmt.copyFile + "' for reading");

    std::vector<std::vector<ColValue>> rows;
    std::vector<std::string> fields;
    std::vector<bool> quoted;
    size_t line = 0;
    bool skipHeader = stmt.copyHeader;
    while (csv::readRecord(in, fields, &quoted)) {
        ++line;
        if (skipHeader) {
            skipHeader = false;
            continue;
        }
        if (fields.size() == 1 && fields[0].empty() && !quoted[0] && table.numColumns() != 1) continue;
        if (fields.size() != table.numColumns()) {
            throw std::invalid_argument("CSV record " + std::to_string(line) + ": expected " +
                                        std::to_string(table.numColumns()) + " fields, got " +
                                        std::to_string(fields.size()));
        }
        std::vector<ColValue> row;
        row.reserve(fields.size());
        for (size_t c = 0; c < fields.size(); ++c) {
            const ColType type = table.columnFile(static_cast<uint16_t>(c)).colType();
            Token tok{type == ColType::STRING ? TokenKind::String : TokenKind::Number, std::move(fields[c])};
            try {
                row.push_back(coerceLiteral(tok, type, c));
            } catch (const std::invalid_argument& ex) {
                throw std::invalid_argument("CSV record " + std::to_string(line) + ": " + ex.what());
            }
        }
        rows.push_back(std::move(row));
    }

    const size_t n = rows.size();
    applyWrite(table, {}, std::move(rows));
    if (!g_txn) table.flushDurable();  // inside a transaction the rows are still pending
    return rowsAffected(n);
}

// COPY '<table>' TO '<file>' / COPY (SELECT ...) TO '<file>'. Writes to a temp file
// and renames it into place, so readers never observe a half-written export.
MiniSQLResult executeCopyTo(Engine& engine, const ParsedStatement& stmt) {
    MiniSQLResult data;
    if (stmt.copyQuery) {
        data = executeSelect(engine, stmt.query);
    } else {
        ParsedQuery all;
        all.tableName = stmt.query.tableName;
        SelectItem star;
        star.kind = SelectItem::Kind::Star;
        all.selectItems.push_back(star);
        data = executeSelect(engine, all);
    }

    const std::string target = engine.resolveFile(stmt.copyFile);
    const std::string tmp = target + ".tmp";
    {
        std::ofstream out(tmp, std::ios::binary | std::ios::trunc);
        if (!out) throw std::invalid_argument("cannot open '" + stmt.copyFile + "' for writing");
        if (stmt.copyHeader) csv::writeRecord(out, data.headers);
        for (const auto& row : data.rows) csv::writeRecord(out, row);
        out.flush();
        if (!out) throw std::runtime_error("write to '" + tmp + "' failed");
    }
    if (std::rename(tmp.c_str(), target.c_str()) != 0) {
        std::remove(tmp.c_str());
        throw std::runtime_error("cannot move export into place at '" + stmt.copyFile + "'");
    }
    return rowsAffected(data.rows.size());
}

const char* statementName(ParsedStatement::Kind kind) {
    switch (kind) {
        case ParsedStatement::Kind::Select: return "SELECT";
        case ParsedStatement::Kind::CreateTable: return "CREATE TABLE";
        case ParsedStatement::Kind::Insert: return "INSERT";
        case ParsedStatement::Kind::Delete: return "DELETE";
        case ParsedStatement::Kind::Update: return "UPDATE";
        case ParsedStatement::Kind::Describe: return "DESCRIBE";
        case ParsedStatement::Kind::Copy: return "COPY";
        default: break;
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
    if (stmt.kind == ParsedStatement::Kind::Select && !stmt.query.joins.empty()) {
        const auto result = executeSelect(engine, stmt.query);
        t.add("Output: " + std::to_string(result.rows.size()) + " rows");
    } else if (stmt.kind == ParsedStatement::Kind::Select || stmt.kind == ParsedStatement::Kind::Delete ||
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

MiniSQLResult describeMiniSQL(Engine& engine, const std::string& sqlText,
                              const std::vector<std::string>* params) {
    std::vector<std::string> dummy;
    if (!params) {
        dummy.assign(sql::countParams(sqlText), "0");
        params = &dummy;
    }
    const ParsedStatement stmt = sql::parse(sqlText, params);
    MiniSQLResult shape;
    if (stmt.explain) {
        shape.headers = {"plan"};
        shape.types = {ColType::STRING};
        return shape;
    }
    if (stmt.kind == ParsedStatement::Kind::Describe) {
        shape.headers = {"column", "type"};
        shape.types = {ColType::STRING, ColType::STRING};
        return shape;
    }
    if (stmt.kind != ParsedStatement::Kind::Select) return shape;

    StatementLocks locks(engine, statementTables(stmt.query));
    if (!stmt.query.joins.empty()) {
        const JoinPlan plan = planJoin(engine, stmt.query);
        setGroupedHeaders(plan.types, plan.query, shape);
        return shape;
    }
    Table& table = openExistingTable(engine, stmt.query.tableName);
    validateQueryShape(table, stmt.query);
    const auto& items = stmt.query.selectItems;
    if (items.size() == 1 && items[0].kind == SelectItem::Kind::Star) {
        for (uint16_t c = 0; c < table.numColumns(); ++c) {
            shape.headers.push_back("c" + std::to_string(c));
            shape.types.push_back(table.columnFile(c).colType());
        }
    } else {
        setGroupedHeaders(table, stmt.query, shape);
    }
    return shape;
}

bool TableChanges::hasChanges() const {
    if (!deletedBase.empty()) return true;
    return std::find(insertedLive.begin(), insertedLive.end(), true) != insertedLive.end();
}

std::vector<std::vector<ColValue>> TableChanges::liveInserts() const {
    std::vector<std::vector<ColValue>> rows;
    for (size_t i = 0; i < inserted.size(); ++i)
        if (insertedLive[i]) rows.push_back(inserted[i]);
    return rows;
}

std::vector<uint32_t> TableChanges::deletedRowIDs() const {
    std::vector<uint32_t> ids(deletedBase.begin(), deletedBase.end());
    std::sort(ids.begin(), ids.end());
    return ids;
}

MiniSQLResult executeParsedStatement(Engine& engine, const ParsedStatement& stmt, TxnHook* txn) {
    if (stmt.isTransactionControl())
        throw std::invalid_argument("BEGIN / COMMIT / ROLLBACK need a session (use Session, the REPL, or a server)");
    // Serialize statements per table so concurrent sessions (server threads) never
    // share a Table's mutable caches. A join locks every table it reads.
    StatementLocks locks(engine, statementTables(stmt.query));

    struct Scope {
        Scope(TxnHook* t, const std::string& name) {
            g_txn = t;
            g_tableName = name;
            g_changes = t ? t->changesFor(name) : nullptr;
        }
        ~Scope() {
            g_txn = nullptr;
            g_changes = nullptr;
            g_tableName.clear();
        }
    } scope(txn, stmt.query.tableName);

    if (stmt.explain) return executeExplain(engine, stmt);
    switch (stmt.kind) {
        case ParsedStatement::Kind::CreateTable: {
            auto r = executeCreateTable(engine, stmt);
            if (txn) r.notices.push_back("CREATE TABLE is not transactional: the table remains even if this "
                                         "transaction rolls back");
            return r;
        }
        case ParsedStatement::Kind::Insert: return executeInsert(engine, stmt);
        case ParsedStatement::Kind::Delete: return executeDelete(engine, stmt);
        case ParsedStatement::Kind::Describe: return executeDescribe(engine, stmt);
        case ParsedStatement::Kind::Update: return executeUpdate(engine, stmt);
        case ParsedStatement::Kind::Copy:
            return stmt.copyFrom ? executeCopyFrom(engine, stmt) : executeCopyTo(engine, stmt);
        case ParsedStatement::Kind::Select: break;
        default: break;
    }
    return executeSelect(engine, stmt.query);
}
