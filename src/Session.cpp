#include "Session.hpp"

#include <mutex>
#include <stdexcept>

#include "Engine.hpp"
#include "SqlParser.hpp"
#include "Table.hpp"

using sql::ParsedStatement;

namespace {

bool isWrite(const ParsedStatement& s) {
    if (s.explain) return false;  // EXPLAIN DELETE / UPDATE evaluates only the WHERE clause
    switch (s.kind) {
        case ParsedStatement::Kind::Insert:
        case ParsedStatement::Kind::Update:
        case ParsedStatement::Kind::Delete:
            return true;
        case ParsedStatement::Kind::Copy:
            return s.copyFrom;
        default:
            return false;
    }
}

MiniSQLResult statusResult(const char* tag) {
    MiniSQLResult r;
    r.headers = {"status"};
    r.types = {ColType::STRING};
    r.rows = {{tag}};
    return r;
}

}  // namespace

Session::Session(Engine& engine) : engine_(engine), id_(engine.newSessionId()) {}

Session::~Session() {
    try {
        rollback();
    } catch (...) {
    }
}

TableChanges* Session::changesFor(const std::string& table) {
    auto it = changes_.find(table);
    return it == changes_.end() ? nullptr : &it->second;
}

TableChanges& Session::changesForWrite(const std::string& table, Table& t) {
    auto [it, inserted] = changes_.try_emplace(table);
    if (inserted) it->second.virtualBase = static_cast<uint32_t>(t.rowsRecorded());
    return it->second;
}

void Session::endTransaction() {
    changes_.clear();
    engine_.releaseWriteLocks(id_);
    status_ = Status::Idle;
}

void Session::rollback() {
    if (status_ == Status::Idle) return;
    endTransaction();
}

void Session::commit() {
    std::vector<Engine::TableCommit> commits;
    for (const auto& [table, ch] : changes_)
        if (ch.hasChanges()) commits.push_back({table, ch.deletedRowIDs(), ch.liveInserts()});
    try {
        if (commits.size() == 1) {
            // One table: its WAL already makes the batch atomic.
            const auto& c = commits.front();
            std::lock_guard<std::mutex> lk(engine_.tableMutex(c.table));
            engine_.openTable(c.table).applyAtomic(c.deletes, c.inserts);
        } else if (commits.size() > 1) {
            engine_.commitTables(commits);
        }
    } catch (...) {
        endTransaction();  // a failed COMMIT rolls the transaction back, as in PostgreSQL
        throw;
    }
    endTransaction();
}

MiniSQLResult Session::execute(const std::string& sqlText, const std::vector<std::string>* params) {
    ParsedStatement stmt;
    try {
        stmt = sql::parse(sqlText, params);
    } catch (...) {
        // In a failed transaction everything but COMMIT / ROLLBACK reports the
        // aborted state, even when it would not parse (as PostgreSQL does).
        if (status_ == Status::Failed)
            throw std::invalid_argument("current transaction is aborted, commands ignored until end of transaction block");
        if (status_ == Status::InTransaction) status_ = Status::Failed;
        throw;
    }

    switch (stmt.kind) {
        case ParsedStatement::Kind::Begin: {
            MiniSQLResult r = statusResult("BEGIN");
            if (status_ != Status::Idle) r.notices.push_back("there is already a transaction in progress");
            else status_ = Status::InTransaction;
            return r;
        }
        case ParsedStatement::Kind::Commit: {
            if (status_ == Status::Idle) {
                MiniSQLResult r = statusResult("COMMIT");
                r.notices.push_back("there is no transaction in progress");
                return r;
            }
            if (status_ == Status::Failed) {  // COMMIT of an aborted transaction rolls back
                endTransaction();
                return statusResult("ROLLBACK");
            }
            commit();
            return statusResult("COMMIT");
        }
        case ParsedStatement::Kind::Rollback: {
            MiniSQLResult r = statusResult("ROLLBACK");
            if (status_ == Status::Idle) r.notices.push_back("there is no transaction in progress");
            rollback();
            return r;
        }
        default:
            break;
    }

    if (status_ == Status::Failed)
        throw std::invalid_argument("current transaction is aborted, commands ignored until end of transaction block");

    const bool inTxn = status_ == Status::InTransaction;
    const bool write = isWrite(stmt);
    try {
        if (write) engine_.acquireWriteLock(stmt.query.tableName, id_, lockTimeout_);
        MiniSQLResult r = executeParsedStatement(engine_, stmt, inTxn ? static_cast<TxnHook*>(this) : nullptr);
        if (write && !inTxn) engine_.releaseWriteLock(stmt.query.tableName, id_);
        return r;
    } catch (...) {
        if (inTxn) status_ = Status::Failed;
        else if (write) engine_.releaseWriteLock(stmt.query.tableName, id_);
        throw;
    }
}

MiniSQLResult Session::describe(const std::string& sqlText, const std::vector<std::string>* params) {
    return describeMiniSQL(engine_, sqlText, params);
}

// Autocommit execution without a caller-owned session.
MiniSQLResult executeMiniSQL(Engine& engine, const std::string& sqlText, const std::vector<std::string>* params) {
    Session session(engine);
    const ParsedStatement stmt = sql::parse(sqlText, params);
    if (stmt.isTransactionControl())
        throw std::invalid_argument("BEGIN / COMMIT / ROLLBACK need a session (use Session, the REPL, or a server)");
    return session.execute(sqlText, params);
}
