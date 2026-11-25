#pragma once

#include <cstdint>
#include <string>
#include <unordered_set>
#include <vector>

#include "ValueTypes.hpp"

class Engine;
class Table;
namespace sql { struct ParsedStatement; }

struct MiniSQLResult {
    std::vector<std::string> headers;
    // Logical type of each output column (same length as headers). Aggregates
    // report the type of their result: COUNT → INT64, SUM over integers → INT64,
    // SUM over floats / AVG → DOUBLE, MIN / MAX → the input column's type.
    std::vector<ColType> types;
    std::vector<std::vector<std::string>> rows;
    // Warnings for the client (e.g. "there is no transaction in progress").
    std::vector<std::string> notices;
};

// Parses and executes one mini-SQL statement in autocommit mode. Throws
// std::invalid_argument for user errors (syntax, unknown column, type mismatch,
// missing table) and std::runtime_error for storage failures. Transaction control
// (BEGIN / COMMIT / ROLLBACK) needs a Session (Session.hpp) and is rejected here.
// `params` binds $1..$n placeholders (text values, typed by the column they meet).
MiniSQLResult executeMiniSQL(Engine& engine, const std::string& sql,
                             const std::vector<std::string>* params = nullptr);

// Result shape (headers + types, no rows) without executing the statement. Row-
// less statements (INSERT / UPDATE / DELETE / CREATE TABLE / COPY / transaction
// control) describe as no columns. Unbound placeholders are treated as unknown values.
MiniSQLResult describeMiniSQL(Engine& engine, const std::string& sql,
                              const std::vector<std::string>* params = nullptr);

// ── Transaction plumbing (used by Session) ───────────────────────────────────

// One transaction's not-yet-committed changes to one table. Nothing here reaches
// the table files until COMMIT (the WAL is redo-only), so ROLLBACK just drops it.
struct TableChanges {
    // Pending rows get virtual row IDs virtualBase + i. virtualBase is the table's
    // rowsRecorded() when the transaction first wrote it; the transaction holds the
    // table's write lock from then on, so no committed row can take those IDs.
    uint32_t virtualBase = 0;
    std::unordered_set<uint32_t> deletedBase;   // committed rows this transaction deleted
    std::vector<std::vector<ColValue>> inserted;
    std::vector<bool> insertedLive;             // false once deleted / updated again

    bool hasChanges() const;
    std::vector<std::vector<ColValue>> liveInserts() const;
    std::vector<uint32_t> deletedRowIDs() const;
};

// How the executor reaches the current transaction, if any.
class TxnHook {
public:
    virtual ~TxnHook() = default;
    // Pending changes for `table`, or nullptr if this transaction has not written it.
    virtual TableChanges* changesFor(const std::string& table) = 0;
    // Changes to buffer a write into (created on first use). The caller has
    // already taken the table's write lock for this transaction.
    virtual TableChanges& changesForWrite(const std::string& table, Table& t) = 0;
};

// Executes an already-parsed, non-transaction-control statement. With `txn`, reads
// see the transaction's pending changes and writes are buffered instead of applied.
MiniSQLResult executeParsedStatement(Engine& engine, const sql::ParsedStatement& stmt, TxnHook* txn);
