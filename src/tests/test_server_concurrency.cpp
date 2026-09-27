// Concurrent server sessions over one shared Engine:
//  - parallel writers on one table lose no rows and leave it verifiably intact
//  - writers on different tables run side by side
//  - the connection limit rejects extra clients cleanly
//  - SIGTERM shuts down gracefully (exit 0) and checkpoints: data survives reopen
//  - --data-dir sandboxes table and COPY paths
//  - oversized requests and idle sessions are cut off
#include "../Engine.hpp"
#include "../MiniSQL.hpp"
#include "../TableTools.hpp"
#include "net_test_util.hpp"

#include <atomic>
#include <cassert>
#include <cstdio>
#include <string>
#include <sys/stat.h>
#include <thread>
#include <vector>

using namespace nettest;

namespace {

void removeTable(const std::string& base) {
    for (const char* ext : {".mdb", ".mdb.idx", ".mdb.wal", ".mdb.1.str"}) std::remove((base + ext).c_str());
}

bool startsWith(const std::string& s, const std::string& prefix) { return s.rfind(prefix, 0) == 0; }

}  // namespace

int main() {
    const std::string shared = "/tmp/conc_shared", other = "/tmp/conc_other";
    removeTable(shared);
    removeTable(other);

    const uint16_t port = reservePort();
    const pid_t server = spawnMdb({"serve", std::to_string(port), "--max-connections", "6"});

    {
        const int admin = connectTo(port);
        assert(startsWith(query(admin, "CREATE TABLE '" + shared + "' (UINT32, STRING)"), "OK\n"));
        assert(startsWith(query(admin, "CREATE TABLE '" + other + "' (UINT32)"), "OK\n"));
        ::close(admin);
    }

    // 4 writers on the shared table + 1 on another table, all at once.
    constexpr int kWriters = 4, kPerWriter = 150;
    std::atomic<int> failures{0};
    std::vector<std::thread> threads;
    for (int w = 0; w < kWriters; ++w) {
        threads.emplace_back([&, w] {
            try {
                const int fd = connectTo(port);
                for (int i = 0; i < kPerWriter; ++i) {
                    const int id = w * 1000 + i;
                    std::string r = query(fd, "INSERT INTO '" + shared + "' VALUES (" + std::to_string(id) +
                                                  ", 'w" + std::to_string(w) + "')");
                    if (r != "OK\nrows_affected\n1\nEND\n") ++failures;
                    if (i % 25 == 0) {  // interleave reads with the writes
                        r = query(fd, "SELECT count(*) FROM '" + shared + "' WHERE c1 = 'w" + std::to_string(w) + "'");
                        if (!startsWith(r, "OK\ncount(*)\n")) ++failures;
                    }
                }
                ::close(fd);
            } catch (...) {
                ++failures;
            }
        });
    }
    threads.emplace_back([&] {
        try {
            const int fd = connectTo(port);
            for (int i = 0; i < 200; ++i)
                if (query(fd, "INSERT INTO '" + other + "' VALUES (" + std::to_string(i) + ")") !=
                    "OK\nrows_affected\n1\nEND\n")
                    ++failures;
            ::close(fd);
        } catch (...) {
            ++failures;
        }
    });
    for (auto& t : threads) t.join();
    assert(failures == 0);

    {
        const int fd = connectTo(port);
        assert(query(fd, "SELECT count(*) FROM '" + shared + "'") ==
               "OK\ncount(*)\n" + std::to_string(kWriters * kPerWriter) + "\nEND\n");
        assert(query(fd, "SELECT count(*), min(c0), max(c0) FROM '" + other + "'") ==
               "OK\ncount(*)\tmin(c0)\tmax(c0)\n200\t0\t199\nEND\n");
        ::close(fd);
    }

    // Connection limit: hold 6 sessions open, the 7th is refused.
    {
        std::vector<int> held;
        for (int i = 0; i < 6; ++i) {
            held.push_back(connectTo(port));
            assert(startsWith(query(held.back(), "SELECT count(*) FROM '" + other + "'"), "OK\n"));
        }
        const int extra = connectTo(port);
        char buf[128] = {};
        const ssize_t n = ::recv(extra, buf, sizeof(buf) - 1, 0);
        assert(n > 0 && std::string(buf) == "ERR\ttoo many connections\nEND\n");
        ::close(extra);
        for (int fd : held) ::close(fd);
    }

    // Oversized request (no newline) is rejected and the session closed.
    {
        const int fd = connectTo(port);
        // give the server a moment to count the freed slots from the block above
        usleep(300000);
        std::string big(17u << 20, 'x');
        try {
            sendAll(fd, big);
        } catch (...) {
        }
        char buf[256] = {};
        const ssize_t n = ::recv(fd, buf, sizeof(buf) - 1, 0);
        assert(n > 0 && startsWith(buf, "ERR\trequest exceeds"));
        ::close(fd);
    }

    // Graceful shutdown with a session still connected: exit 0, tables checkpointed.
    {
        const int idle = connectTo(port);
        assert(stopMdb(server) == 0);
        ::close(idle);
    }
    {
        struct stat st{};
        ::stat((shared + ".mdb.wal").c_str(), &st);
        assert(st.st_size == 8);  // WAL folded into the base files (header only)
        assert(tools::verifyTable(shared).ok());
        Engine e;
        auto r = executeMiniSQL(e, "SELECT c1, count(*) FROM '" + shared + "' GROUP BY c1");
        assert(r.rows.size() == kWriters);
        for (const auto& row : r.rows) assert(row[1] == std::to_string(kPerWriter));
    }

    // --data-dir sandbox + idle timeout.
    {
        const std::string dir = "/tmp/conc_sandbox";
        ::mkdir(dir.c_str(), 0777);
        removeTable(dir + "/t");
        const uint16_t port2 = reservePort();
        const pid_t sandboxed = spawnMdb({"serve", std::to_string(port2), "--data-dir", dir, "--idle-timeout", "1"});
        const int fd = connectTo(port2);
        assert(startsWith(query(fd, "CREATE TABLE 't' (UINT32)"), "OK\n"));
        assert(startsWith(query(fd, "INSERT INTO 't' VALUES (1), (2)"), "OK\n"));
        assert(startsWith(query(fd, "SELECT * FROM '/tmp/conc_shared'"), "ERR\ttable name must be relative"));
        assert(startsWith(query(fd, "CREATE TABLE '../escape' (UINT32)"), "ERR\ttable name must not contain '..'"));
        assert(startsWith(query(fd, "COPY 't' TO '/tmp/leak.csv'"), "ERR\tfile path must be relative"));
        assert(startsWith(query(fd, "COPY 't' TO 'out.csv'"), "OK\n"));
        struct stat st{};
        assert(::stat((dir + "/out.csv").c_str(), &st) == 0);
        assert(::stat((dir + "/t.mdb").c_str(), &st) == 0);

        // Idle timeout: after >1s of silence the server says so and hangs up.
        char buf[128] = {};
        const ssize_t n = ::recv(fd, buf, sizeof(buf) - 1, 0);
        assert(n > 0 && std::string(buf) == "ERR\tidle timeout\nEND\n");
        assert(::recv(fd, buf, sizeof(buf), 0) == 0);
        ::close(fd);
        assert(stopMdb(sandboxed) == 0);
        std::remove((dir + "/out.csv").c_str());
        removeTable(dir + "/t");
    }

    removeTable(shared);
    removeTable(other);
    std::puts("test_server_concurrency: passed");
    return 0;
}
