// Transactions (Session): isolation, rollback, read-your-writes through every
// statement kind, failed-transaction state, write-lock conflicts, and atomic
// multi-table commits including crash recovery from the commit journal.
#include "../Engine.hpp"
#include "../MiniSQL.hpp"
#include "../Session.hpp"
#include "../TableTools.hpp"

#include <atomic>
#include <cassert>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <fstream>
#include <sstream>
#include <stdexcept>
#include <string>
#include <sys/stat.h>
#include <sys/wait.h>
#include <thread>
#include <unistd.h>
#include <vector>

namespace {

using Rows = std::vector<std::vector<std::string>>;
const std::string kDir = "/tmp/txn_test";

void removeTable(const std::string& name) {
    const std::string base = kDir + "/" + name;
    for (const char* ext : {".mdb", ".mdb.idx", ".mdb.wal", ".mdb.1.str"}) std::remove((base + ext).c_str());
}

void reset() {
    ::mkdir(kDir.c_str(), 0777);
    for (const char* t : {"acct", "ledger", "other"}) removeTable(t);
    std::remove((kDir + "/.mdb_txn_journal").c_str());
}

Rows q(Session& s, const std::string& sql) { return s.execute(sql).rows; }

std::string scalar(Session& s, const std::string& sql) { return s.execute(sql).rows.at(0).at(0); }

bool fails(Session& s, const std::string& sql, const std::string& needle = "") {
    try {
        s.execute(sql);
    } catch (const std::exception& ex) {
        return needle.empty() || std::string(ex.what()).find(needle) != std::string::npos;
    }
    return false;
}

void setupAccounts(Engine& e) {
    Session s(e);
    s.execute("CREATE TABLE acct (UINT32, STRING, INT64)");   // id, owner, balance
    s.execute("INSERT INTO acct VALUES (1, 'ada', 100), (2, 'bob', 50), (3, 'cy', 0)");
    s.execute("CREATE TABLE ledger (UINT32, INT64)");          // acct id, delta
}

}  // namespace

int main() {
    // ── isolation + commit + rollback ───────────────────────────────────────
    reset();
    {
        Engine e;
        e.setDataDir(kDir);
        setupAccounts(e);
        Session a(e), b(e);
        q(a, "BEGIN");
        assert(a.status() == Session::Status::InTransaction);
        q(a, "INSERT INTO acct VALUES (4, 'dee', 7)");
        assert(scalar(a, "SELECT count(*) FROM acct") == "4");  // sees its own insert
        assert(scalar(b, "SELECT count(*) FROM acct") == "3");  // others don't (no dirty reads)
        q(a, "COMMIT");
        assert(a.status() == Session::Status::Idle);
        assert(scalar(b, "SELECT count(*) FROM acct") == "4");

        q(a, "BEGIN");
        q(a, "DELETE FROM acct WHERE c0 = 4");
        q(a, "UPDATE acct SET c2 = 999 WHERE c1 = 'ada'");
        q(a, "INSERT INTO acct VALUES (5, 'eve', 1)");
        assert(scalar(a, "SELECT count(*) FROM acct") == "4");
        q(a, "ROLLBACK");
        assert((q(b, "SELECT c0, c2 FROM acct ORDER BY c0") == Rows{{"1", "100"}, {"2", "50"}, {"3", "0"}, {"4", "7"}}));
    }
    {
        Engine e;  // rolled-back work never reached the files
        e.setDataDir(kDir);
        Session s(e);
        assert((q(s, "SELECT c0, c2 FROM acct ORDER BY c0") == Rows{{"1", "100"}, {"2", "50"}, {"3", "0"}, {"4", "7"}}));
    }

    // ── read-your-writes through every statement kind ───────────────────────
    reset();
    {
        Engine e;
        e.setDataDir(kDir);
        setupAccounts(e);
        Session s(e);
        q(s, "BEGIN");
        q(s, "INSERT INTO acct VALUES (10, 'zed', 500), (11, 'amy', 5)");
        q(s, "UPDATE acct SET c2 = 60 WHERE c0 = 2");               // committed row → pending version
        q(s, "UPDATE acct SET c2 = 501 WHERE c0 = 10");             // pending row updated again
        q(s, "DELETE FROM acct WHERE c0 = 3 OR c0 = 11");           // one committed, one pending
        assert((q(s, "SELECT c0, c1, c2 FROM acct ORDER BY c0") ==
                Rows{{"1", "ada", "100"}, {"2", "bob", "60"}, {"10", "zed", "501"}}));
        assert((q(s, "SELECT c1 FROM acct WHERE c2 > 80 ORDER BY c1") == Rows{{"ada"}, {"zed"}}));
        assert((q(s, "SELECT count(*), sum(c2), max(c2) FROM acct") == Rows{{"3", "661", "501"}}));
        assert((q(s, "SELECT c0, count(*) FROM acct GROUP BY c0") == Rows{{"1", "1"}, {"2", "1"}, {"10", "1"}}));
        assert((q(s, "SELECT c1 FROM acct ORDER BY c2 DESC LIMIT 1") == Rows{{"zed"}}));
        assert((q(s, "SELECT DISTINCT c1 FROM acct WHERE c1 != 'ada'") == Rows{{"bob"}, {"zed"}}));
        q(s, "COPY acct TO 'acct.csv'");                             // export sees pending rows too
        {
            std::ifstream in(kDir + "/acct.csv");
            std::stringstream ss;
            ss << in.rdbuf();
            assert(ss.str() == "1,ada,100\n2,bob,60\n10,zed,501\n");
        }
        auto plan = s.execute("EXPLAIN SELECT c0 FROM acct WHERE c2 > 0");
        bool sawTxn = false;
        for (const auto& row : plan.rows) sawTxn = sawTxn || row[0].find("Transaction:") != std::string::npos;
        assert(sawTxn);
        q(s, "COMMIT");
        auto v = tools::verifyTable(kDir + "/acct");
        assert(v.ok());
        std::remove((kDir + "/acct.csv").c_str());
    }
    {
        Engine e;
        e.setDataDir(kDir);
        Session s(e);
        assert((q(s, "SELECT c0, c1, c2 FROM acct ORDER BY c0") ==
                Rows{{"1", "ada", "100"}, {"2", "bob", "60"}, {"10", "zed", "501"}}));
    }

    // ── failed transactions, statement errors, notices ──────────────────────
    reset();
    {
        Engine e;
        e.setDataDir(kDir);
        setupAccounts(e);
        Session s(e);
        auto r = s.execute("COMMIT");
        assert(r.rows[0][0] == "COMMIT" && r.notices.size() == 1);  // no transaction in progress
        q(s, "BEGIN");
        assert(s.execute("BEGIN").notices.size() == 1);              // already in progress
        q(s, "INSERT INTO acct VALUES (7, 'g', 1)");
        assert(fails(s, "INSERT INTO acct VALUES ('bad', 'h', 1)"));
        assert(s.status() == Session::Status::Failed);
        assert(fails(s, "SELECT * FROM acct", "current transaction is aborted"));
        assert(s.execute("COMMIT").rows[0][0] == "ROLLBACK");        // commit of a failed txn rolls back
        assert(s.status() == Session::Status::Idle);
        assert(scalar(s, "SELECT count(*) FROM acct") == "3");       // the good insert went too
        assert(fails(s, "INSERT INTO acct VALUES ('bad', 'h', 1)")); // autocommit error: no state change
        assert(s.status() == Session::Status::Idle);
        assert(fails(s, "SAVEPOINT sp", "savepoints"));
        bool rejected = false;
        try {
            executeMiniSQL(e, "BEGIN");
        } catch (const std::invalid_argument&) {
            rejected = true;
        }
        assert(rejected);  // free-function API is autocommit-only

        {  // destroying a session rolls back its open transaction and frees its locks
            Session t(e);
            q(t, "BEGIN");
            q(t, "INSERT INTO acct VALUES (8, 'h', 1)");
        }
        Session u(e);
        u.setLockTimeout(std::chrono::milliseconds(50));
        q(u, "INSERT INTO acct VALUES (9, 'i', 1)");  // lock was released
        assert(scalar(u, "SELECT count(*) FROM acct") == "4");
    }

    // ── write locks: conflicts, non-blocking readers, waiting writers ───────
    reset();
    {
        Engine e;
        e.setDataDir(kDir);
        setupAccounts(e);
        Session a(e), b(e);
        b.setLockTimeout(std::chrono::milliseconds(100));
        q(a, "BEGIN");
        q(a, "UPDATE acct SET c2 = 1 WHERE c0 = 1");
        assert(fails(b, "INSERT INTO acct VALUES (5, 'x', 0)", "could not obtain lock"));  // autocommit writer
        assert(scalar(b, "SELECT c2 FROM acct WHERE c0 = 1") == "100");                    // reader: committed data
        q(b, "INSERT INTO ledger VALUES (1, 5)");                                          // other tables are free
        q(b, "BEGIN");
        assert(fails(b, "DELETE FROM acct WHERE c0 = 2", "could not obtain lock"));
        assert(b.status() == Session::Status::Failed);
        q(b, "ROLLBACK");

        // A writer that waits long enough proceeds once the lock holder commits.
        b.setLockTimeout(std::chrono::milliseconds(5000));
        std::atomic<bool> done{false};
        std::thread waiter([&] {
            q(b, "UPDATE acct SET c2 = 0 WHERE c0 = 99");  // never matches; just needs the lock
            done = true;
        });
        std::this_thread::sleep_for(std::chrono::milliseconds(150));
        assert(!done);
        q(a, "COMMIT");
        waiter.join();
        assert(done);
        assert(scalar(b, "SELECT c2 FROM acct WHERE c0 = 1") == "1");
    }

    // ── multi-table commit: atomic transfer ─────────────────────────────────
    reset();
    {
        Engine e;
        e.setDataDir(kDir);
        setupAccounts(e);
        Session s(e);
        q(s, "BEGIN");
        q(s, "UPDATE acct SET c2 = 70 WHERE c0 = 1");
        q(s, "UPDATE acct SET c2 = 80 WHERE c0 = 2");
        q(s, "INSERT INTO ledger VALUES (1, -30), (2, 30)");
        q(s, "COMMIT");
        assert(scalar(s, "SELECT sum(c2) FROM acct") == "150");
        assert(scalar(s, "SELECT sum(c1) FROM ledger") == "0");
        struct stat st{};
        assert(::stat((kDir + "/.mdb_txn_journal").c_str(), &st) != 0);  // journal cleared after commit
    }

    // ── crash during a multi-table commit: the journal rolls it forward ─────
    for (int crashAfter : {0, 1}) {  // crash before applying any table / after applying one
        reset();
        {
            Engine e;
            e.setDataDir(kDir);
            setupAccounts(e);
        }
        const pid_t child = ::fork();
        if (child == 0) {
            ::setenv("MDB_TEST_CRASH_AFTER_TABLES", std::to_string(crashAfter).c_str(), 1);
            Engine e;
            e.setDataDir(kDir);
            Session s(e);
            s.execute("BEGIN");
            s.execute("UPDATE acct SET c2 = 70 WHERE c0 = 1");
            s.execute("INSERT INTO ledger VALUES (1, -30), (2, 30)");
            s.execute("COMMIT");  // _Exit(0)s inside the commit
            std::_Exit(3);
        }
        int status = 0;
        ::waitpid(child, &status, 0);
        assert(WIFEXITED(status) && WEXITSTATUS(status) == 0);
        struct stat st{};
        assert(::stat((kDir + "/.mdb_txn_journal").c_str(), &st) == 0);  // crash left the journal behind

        Engine e;
        e.setDataDir(kDir);
        Session s(e);
        assert(scalar(s, "SELECT c2 FROM acct WHERE c0 = 1") == "70");   // replayed or already applied
        assert(scalar(s, "SELECT count(*), sum(c1) FROM ledger") == "2");
        assert(scalar(s, "SELECT sum(c1) FROM ledger") == "0");
        assert(::stat((kDir + "/.mdb_txn_journal").c_str(), &st) != 0);
        assert(tools::verifyTable(kDir + "/acct").ok() && tools::verifyTable(kDir + "/ledger").ok());
    }

    // ── a torn (never committed) journal is discarded ───────────────────────
    reset();
    {
        Engine e;
        e.setDataDir(kDir);
        setupAccounts(e);
    }
    {
        std::ofstream j(kDir + "/.mdb_txn_journal", std::ios::binary);
        const uint32_t hdr[4] = {0x4A58544D, 1, 1000, 0};  // claims a 1000-byte payload that never arrived
        j.write(reinterpret_cast<const char*>(hdr), sizeof(hdr));
    }
    {
        Engine e;
        e.setDataDir(kDir);
        Session s(e);
        assert(scalar(s, "SELECT count(*) FROM acct") == "3");
        struct stat st{};
        assert(::stat((kDir + "/.mdb_txn_journal").c_str(), &st) != 0);
    }

    reset();
    std::puts("test_transactions: passed");
    return 0;
}
