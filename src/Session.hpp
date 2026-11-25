#pragma once
// A client session: runs mini-SQL statements with transaction support.
//
//   BEGIN / START TRANSACTION ... COMMIT / END     ROLLBACK / ABORT
//
// Outside a transaction every statement autocommits atomically. Inside one,
// writes are buffered per table (nothing reaches the table files or WAL), later
// statements in the same transaction see them, and COMMIT applies them: one
// atomic WAL batch for a single table, or the engine's commit journal for several.
// ROLLBACK, a failed COMMIT, or destroying the session discards them.
//
// Isolation is READ COMMITTED: readers never block and only ever see committed
// data; a transaction takes a table-level write lock when it first writes a table
// and holds it to the end, so concurrent writers to that table wait (up to the lock
// timeout, then fail with "could not obtain lock"). After an error inside a
// transaction the session is "failed" (as in PostgreSQL): every statement except
// ROLLBACK / COMMIT is rejected until the transaction ends.
//
// A Session is used by one thread at a time; sessions share an Engine safely.

#include <chrono>
#include <cstdint>
#include <map>
#include <string>
#include <vector>

#include "MiniSQL.hpp"

class Engine;

class Session : private TxnHook {
public:
    enum class Status { Idle, InTransaction, Failed };

    explicit Session(Engine& engine);
    ~Session() override;  // rolls back an open transaction
    Session(const Session&) = delete;
    Session& operator=(const Session&) = delete;

    MiniSQLResult execute(const std::string& sql, const std::vector<std::string>* params = nullptr);
    MiniSQLResult describe(const std::string& sql, const std::vector<std::string>* params = nullptr);

    Status status() const { return status_; }
    void rollback();
    void setLockTimeout(std::chrono::milliseconds t) { lockTimeout_ = t; }

private:
    TableChanges* changesFor(const std::string& table) override;
    TableChanges& changesForWrite(const std::string& table, Table& t) override;
    void commit();
    void endTransaction();

    Engine& engine_;
    const uint64_t id_;
    Status status_ = Status::Idle;
    std::map<std::string, TableChanges> changes_;
    std::chrono::milliseconds lockTimeout_{10'000};
};
