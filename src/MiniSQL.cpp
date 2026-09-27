// Mini-SQL executor. Parsing lives in SqlParser, WHERE evaluation in WhereEval;
// this file validates statement shape against the table schema and executes.
#include "MiniSQL.hpp"

#include <algorithm>
#include <cstdint>
#include <cstdlib>
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

using namespace sql;

namespace {

struct AggregateState {
    uint64_t count = 0;
    long double sum = 0.0;
    ColValue min;
    ColValue max;
    bool hasValue = false;
};

std::string formatColValue(const ColValue& value) {
    std::ostringstream out;
    switch (value.type) {
        case ColType::UINT32: return std::to_string(value.u32);
        case ColType::INT64: return std::to_string(value.i64);
        case ColType::FLOAT:
            out << value.f32;
            return out.str();
        case ColType::DOUBLE:
            out << value.f64;
            return out.str();
        case ColType::STRING:
            return value.str;
    }
    return "";
}

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
    if (hasStar && query.hasGroupBy)
        throw std::invalid_argument("SELECT * with GROUP BY is not supported");

    if (query.where) validateWhere(table, *query.where);

    if (query.hasGroupBy) {
        validateColumnRef(table, query.groupBy.index);
        if (query.where)
            throw std::invalid_argument("GROUP BY with WHERE is not supported in mini-SQL v1");
        if (aggregateCount != 1)
            throw std::invalid_argument("GROUP BY queries require exactly one aggregate expression");
        if (query.selectItems.size() != 2 ||
            query.selectItems[0].kind != SelectItem::Kind::Column ||
            query.selectItems[0].column.index != query.groupBy.index) {
            throw std::invalid_argument("GROUP BY queries must select the group key first");
        }
        const auto& agg = query.selectItems[1];
        if (agg.kind != SelectItem::Kind::CountStar && agg.kind != SelectItem::Kind::Count) {
            if (table.columnFile(query.groupBy.index).colType() != ColType::UINT32 ||
                table.columnFile(agg.column.index).colType() != ColType::UINT32) {
                throw std::invalid_argument("GROUP BY v1 supports UINT32 key/value columns only");
            }
        } else if (table.columnFile(query.groupBy.index).colType() != ColType::UINT32) {
            throw std::invalid_argument("GROUP BY v1 supports UINT32 key columns only");
        }
        return;
    }

    if (aggregateCount > 0) {
        if (hasColumn || hasStar)
            throw std::invalid_argument("aggregate queries cannot mix aggregates with plain columns");
    }
}

std::vector<uint32_t> executeWhere(Table& table, const ParsedQuery& query) {
    if (!query.where) return allLiveRowIDs(table);
    return evaluateWhere(table, *query.where);
}

MiniSQLResult executeProjectionQuery(Table& table, const ParsedQuery& query) {
    std::vector<uint16_t> cols;
    MiniSQLResult result;

    if (query.selectItems.size() == 1 && query.selectItems[0].kind == SelectItem::Kind::Star) {
        for (uint16_t c = 0; c < table.numColumns(); ++c) {
            cols.push_back(c);
            result.headers.push_back("c" + std::to_string(c));
        }
    } else {
        for (const auto& item : query.selectItems) {
            cols.push_back(item.column.index);
            result.headers.push_back(item.header());
        }
    }

    // Without ORDER BY, LIMIT/OFFSET can stop materializing early.
    const auto rowIDs = executeWhere(table, query);
    size_t begin = 0;
    size_t end = rowIDs.size();
    if (query.orderBy.empty()) {
        begin = static_cast<size_t>(std::min<uint64_t>(query.offset, rowIDs.size()));
        if (query.hasLimit) end = static_cast<size_t>(std::min<uint64_t>(begin + query.limit, end));
    }

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

AggregateState computeAggregate(Table& table, const std::vector<uint32_t>& rowIDs, const SelectItem& item) {
    AggregateState state;
    if (item.kind == SelectItem::Kind::CountStar) {
        state.count = rowIDs.size();
        return state;
    }
    for (uint32_t rowID : rowIDs) {
        auto cell = table.fetchTypedValue(rowID, item.column.index);
        if (!cell) continue;
        if (!state.hasValue) {
            state.min = *cell;
            state.max = *cell;
            state.hasValue = true;
        } else {
            if (*cell < state.min) state.min = *cell;
            if (*cell > state.max) state.max = *cell;
        }
        ++state.count;
        state.sum += cell->toDouble();
    }
    return state;
}

std::string formatAggregate(const SelectItem& item, const AggregateState& state) {
    switch (item.kind) {
        case SelectItem::Kind::CountStar:
        case SelectItem::Kind::Count:
            return std::to_string(state.count);
        case SelectItem::Kind::Sum:
            return state.count == 0 ? "0" : formatColValue(ColValue(static_cast<double>(state.sum)));
        case SelectItem::Kind::Min:
            return state.hasValue ? formatColValue(state.min) : "";
        case SelectItem::Kind::Max:
            return state.hasValue ? formatColValue(state.max) : "";
        case SelectItem::Kind::Avg: {
            std::ostringstream out;
            out << (state.count == 0 ? 0.0L : state.sum / static_cast<long double>(state.count));
            return out.str();
        }
        default:
            throw std::invalid_argument("not an aggregate");
    }
}

MiniSQLResult executeScalarAggregateQuery(Table& table, const ParsedQuery& query) {
    const auto rowIDs = executeWhere(table, query);
    MiniSQLResult result;
    std::vector<std::string> row;
    for (const auto& item : query.selectItems) {
        result.headers.push_back(item.header());
        row.push_back(formatAggregate(item, computeAggregate(table, rowIDs, item)));
    }
    result.rows.push_back(std::move(row));
    return result;
}

template <typename Map, typename Format>
void appendSortedGroups(const Map& groups, MiniSQLResult& result, Format format) {
    std::vector<std::pair<typename Map::key_type, typename Map::mapped_type>> rows(groups.begin(), groups.end());
    std::sort(rows.begin(), rows.end(), [](const auto& lhs, const auto& rhs) { return lhs.first < rhs.first; });
    for (const auto& [key, value] : rows) result.rows.push_back({std::to_string(key), format(value)});
}

MiniSQLResult executeGroupByQuery(Engine& engine, const ParsedQuery& query) {
    MiniSQLResult result;
    const auto& agg = query.selectItems[1];
    result.headers = {query.selectItems[0].header(), agg.header()};
    const auto toStr = [](const auto& v) { return std::to_string(v); };

    switch (agg.kind) {
        case SelectItem::Kind::CountStar:
        case SelectItem::Kind::Count:
            appendSortedGroups(engine.groupCount(query.tableName, query.groupBy.index), result, toStr);
            return result;
        case SelectItem::Kind::Sum:
            appendSortedGroups(engine.groupSum(query.tableName, query.groupBy.index, agg.column.index), result, toStr);
            return result;
        case SelectItem::Kind::Min:
            appendSortedGroups(engine.groupMin(query.tableName, query.groupBy.index, agg.column.index), result, toStr);
            return result;
        case SelectItem::Kind::Max:
            appendSortedGroups(engine.groupMax(query.tableName, query.groupBy.index, agg.column.index), result, toStr);
            return result;
        case SelectItem::Kind::Avg:
            appendSortedGroups(engine.groupAvg(query.tableName, query.groupBy.index, agg.column.index), result,
                               [](double v) {
                                   std::ostringstream out;
                                   out << v;
                                   return out.str();
                               });
            return result;
        default:
            throw std::invalid_argument("unsupported GROUP BY aggregate");
    }
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
    for (uint16_t c = 0; c < table.numColumns(); ++c)
        result.rows.push_back({"c" + std::to_string(c), colTypeName(table.columnFile(c).colType())});
    return result;
}

bool outputColumnIsNumeric(const Table& table, const ParsedQuery& query, size_t idx) {
    if (query.hasGroupBy) return true;  // UINT32 key + numeric aggregate
    const auto& first = query.selectItems.front();
    if (first.kind == SelectItem::Kind::Star)
        return table.columnFile(static_cast<uint16_t>(idx)).colType() != ColType::STRING;
    const auto& item = query.selectItems[idx];
    if (item.kind == SelectItem::Kind::Column || item.kind == SelectItem::Kind::Min ||
        item.kind == SelectItem::Kind::Max)
        return table.columnFile(item.column.index).colType() != ColType::STRING;
    return true;
}

void applyOrderAndLimit(const Table& table, const ParsedQuery& query, MiniSQLResult& result,
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
            keys.push_back({idx, outputColumnIsNumeric(table, query, idx), key.descending});
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
    if (query.hasGroupBy) {
        result = executeGroupByQuery(engine, query);
    } else if (aggregateQuery) {
        result = executeScalarAggregateQuery(table, query);
    } else {
        result = executeProjectionQuery(table, query);
        limitApplied = query.orderBy.empty();
    }

    applyOrderAndLimit(table, query, result, limitApplied);
    return result;
}

} // namespace

MiniSQLResult executeMiniSQL(Engine& engine, const std::string& sqlText) {
    const ParsedStatement stmt = sql::parse(sqlText);
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
