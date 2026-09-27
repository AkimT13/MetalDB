// PostgreSQL wire protocol (mdb pgserve) with a minimal hand-written v3 client:
// SSL negotiation, startup, password auth, typed RowDescription, DataRow,
// CommandComplete tags, SQLSTATE errors (and skipping the rest of a multi-
// statement query), EmptyQueryResponse, transaction no-ops, extended-protocol
// rejection with Sync recovery, graceful shutdown.
#include "net_test_util.hpp"

#include <cassert>
#include <cstdio>
#include <cstring>
#include <string>
#include <vector>

using namespace nettest;

namespace {

struct Message {
    char type;
    std::string body;
};

class PgClient {
public:
    explicit PgClient(uint16_t port) : fd_(connectTo(port)) {}
    ~PgClient() {
        if (fd_ >= 0) ::close(fd_);
    }

    void sendRaw(const std::string& bytes) { sendAll(fd_, bytes); }

    static std::string be32(int32_t v) {
        std::string s(4, '\0');
        for (int i = 0; i < 4; ++i) s[i] = static_cast<char>((v >> (24 - 8 * i)) & 0xFF);
        return s;
    }

    void sendMsg(char type, const std::string& body) {
        std::string m(1, type);
        m += be32(static_cast<int32_t>(body.size() + 4));
        m += body;
        sendRaw(m);
    }

    char readByte() {
        fill(1);
        const char c = buf_[0];
        buf_.erase(0, 1);
        return c;
    }

    Message readMsg() {
        fill(5);
        Message m{buf_[0], {}};
        const int32_t len = i32(buf_, 1);
        fill(static_cast<size_t>(len) + 1);
        m.body = buf_.substr(5, static_cast<size_t>(len) - 4);
        buf_.erase(0, static_cast<size_t>(len) + 1);
        return m;
    }

    // Reads messages up to and including ReadyForQuery.
    std::vector<Message> untilReady() {
        std::vector<Message> out;
        while (true) {
            out.push_back(readMsg());
            if (out.back().type == 'Z') return out;
        }
    }

    bool closedByPeer() {
        char c;
        return ::recv(fd_, &c, 1, 0) == 0;
    }

    static int32_t i32(const std::string& s, size_t pos) {
        return static_cast<int32_t>((uint32_t(uint8_t(s[pos])) << 24) | (uint32_t(uint8_t(s[pos + 1])) << 16) |
                                    (uint32_t(uint8_t(s[pos + 2])) << 8) | uint32_t(uint8_t(s[pos + 3])));
    }
    static int16_t i16(const std::string& s, size_t pos) {
        return static_cast<int16_t>((uint16_t(uint8_t(s[pos])) << 8) | uint16_t(uint8_t(s[pos + 1])));
    }

private:
    void fill(size_t n) {
        char chunk[4096];
        while (buf_.size() < n) {
            const ssize_t r = ::recv(fd_, chunk, sizeof(chunk), 0);
            if (r <= 0) throw std::runtime_error("connection closed");
            buf_.append(chunk, static_cast<size_t>(r));
        }
    }

    int fd_;
    std::string buf_;
};

std::string startupPacket(const std::string& user) {
    std::string body = PgClient::be32(196608);
    body += "user";
    body.push_back('\0');
    body += user;
    body.push_back('\0');
    body += "database";
    body.push_back('\0');
    body += "db";
    body.push_back('\0');
    body.push_back('\0');
    return PgClient::be32(static_cast<int32_t>(body.size() + 4)) + body;
}

// Returns the startup reply messages through the first ReadyForQuery.
std::vector<Message> connectAndStart(PgClient& c, const std::string& password = "") {
    c.sendRaw(PgClient::be32(8) + PgClient::be32(80877103));  // SSLRequest
    assert(c.readByte() == 'N');
    c.sendRaw(startupPacket("tester"));
    Message first = c.readMsg();
    assert(first.type == 'R');
    if (PgClient::i32(first.body, 0) == 3) {  // cleartext password requested
        std::string pw = password;
        pw.push_back('\0');
        c.sendMsg('p', pw);
        first = c.readMsg();
        if (first.type == 'E') return {first};
    }
    assert(PgClient::i32(first.body, 0) == 0);  // AuthenticationOk
    auto rest = c.untilReady();
    rest.insert(rest.begin(), first);
    return rest;
}

std::string field(const Message& err, char code) {
    size_t pos = 0;
    while (pos < err.body.size() && err.body[pos] != '\0') {
        const char f = err.body[pos++];
        const std::string value = err.body.c_str() + pos;
        pos += value.size() + 1;
        if (f == code) return value;
    }
    return "";
}

struct Col {
    std::string name;
    int32_t oid;
};

std::vector<Col> parseRowDescription(const Message& m) {
    std::vector<Col> cols;
    const int16_t n = PgClient::i16(m.body, 0);
    size_t pos = 2;
    for (int i = 0; i < n; ++i) {
        Col c;
        c.name = m.body.c_str() + pos;
        pos += c.name.size() + 1;
        c.oid = PgClient::i32(m.body, pos + 6);
        pos += 18;
        cols.push_back(c);
    }
    return cols;
}

std::vector<std::string> parseDataRow(const Message& m) {
    std::vector<std::string> cells;
    const int16_t n = PgClient::i16(m.body, 0);
    size_t pos = 2;
    for (int i = 0; i < n; ++i) {
        const int32_t len = PgClient::i32(m.body, pos);
        pos += 4;
        cells.push_back(m.body.substr(pos, static_cast<size_t>(len)));
        pos += static_cast<size_t>(len);
    }
    return cells;
}

std::string types(const std::vector<Message>& msgs) {
    std::string t;
    for (const auto& m : msgs) t.push_back(m.type);
    return t;
}

std::vector<Message> simpleQuery(PgClient& c, const std::string& sql) {
    std::string body = sql;
    body.push_back('\0');
    c.sendMsg('Q', body);
    return c.untilReady();
}

}  // namespace

int main() {
    for (const char* ext : {".mdb", ".mdb.idx", ".mdb.wal", ".mdb.1.str"})
        std::remove((std::string("/tmp/pgwire_tbl") + ext).c_str());

    const uint16_t port = reservePort();
    const pid_t server = spawnMdb({"pgserve", std::to_string(port)});

    {
        PgClient c(port);
        const auto hello = connectAndStart(c);
        bool sawVersion = false;
        for (const auto& m : hello)
            if (m.type == 'S' && m.body.rfind("server_version", 0) == 0) sawVersion = true;
        assert(sawVersion);
        assert(hello.back().type == 'Z' && hello.back().body == "I");

        // DDL + DML command tags.
        auto r = simpleQuery(c, "CREATE TABLE '/tmp/pgwire_tbl' (UINT32, STRING, DOUBLE, INT64, FLOAT)");
        assert(types(r) == "CZ" && r[0].body == std::string("CREATE TABLE") + '\0');
        r = simpleQuery(c, "INSERT INTO '/tmp/pgwire_tbl' VALUES (1, 'a', 1.5, -7, 0.5), (2, 'b', 2.5, 8, 1)");
        assert(types(r) == "CZ" && r[0].body == std::string("INSERT 0 2") + '\0');

        // SELECT: typed RowDescription, DataRows, tag.
        r = simpleQuery(c, "SELECT * FROM '/tmp/pgwire_tbl' ORDER BY c0");
        assert(types(r) == "TDDCZ");
        const auto cols = parseRowDescription(r[0]);
        assert(cols.size() == 5 && cols[0].name == "c0");
        assert(cols[0].oid == 20 && cols[1].oid == 25 && cols[2].oid == 701 && cols[3].oid == 20 && cols[4].oid == 700);
        assert((parseDataRow(r[1]) == std::vector<std::string>{"1", "a", "1.5", "-7", "0.5"}));
        assert(r[3].body == std::string("SELECT 2") + '\0');

        r = simpleQuery(c, "UPDATE '/tmp/pgwire_tbl' SET c1 = 'z' WHERE c0 = 2");
        assert(r[0].body == std::string("UPDATE 1") + '\0');

        // Multi-statement query; an error stops the rest of the batch.
        r = simpleQuery(c, "SELECT count(*) FROM '/tmp/pgwire_tbl'; SELECT * FROM '/tmp/pgwire_missing'; "
                           "DELETE FROM '/tmp/pgwire_tbl'");
        assert(types(r) == "TDCEZ");
        assert(field(r[3], 'C') == "42P01" && field(r[3], 'S') == "ERROR");
        r = simpleQuery(c, "SELECT count(*) FROM '/tmp/pgwire_tbl'");
        assert(parseDataRow(r[1])[0] == "2");  // DELETE after the error did not run

        // Syntax / column errors carry SQLSTATEs.
        r = simpleQuery(c, "SELEC 1");
        assert(r[0].type == 'E' && field(r[0], 'C') == "42601");
        r = simpleQuery(c, "SELECT c9 FROM '/tmp/pgwire_tbl'");
        assert(r[0].type == 'E' && field(r[0], 'C') == "42703");

        // Empty query, comments only, health check, transaction no-ops.
        r = simpleQuery(c, "  ; -- nothing\n");
        assert(types(r) == "IZ");
        r = simpleQuery(c, "SELECT 1");
        assert(types(r) == "TDCZ" && parseDataRow(r[1])[0] == "1");
        r = simpleQuery(c, "BEGIN; INSERT INTO '/tmp/pgwire_tbl' VALUES (3, 'c', 0, 0, 0); ROLLBACK");
        assert(types(r) == "CCNCZ");  // BEGIN, INSERT, NOTICE, ROLLBACK
        r = simpleQuery(c, "SET search_path TO public");
        assert(r[0].body == std::string("SET") + '\0');

        // Extended protocol: one error, ignore until Sync, then usable again.
        c.sendMsg('P', std::string("\0SELECT 1\0\0\0", 12));
        c.sendMsg('B', std::string("\0\0\0\0\0\0\0\0", 8));
        c.sendMsg('E', std::string("\0\0\0\0\0", 5));
        c.sendMsg('S', "");
        auto ext = c.untilReady();
        assert(types(ext) == "EZ" && field(ext[0], 'C') == "0A000");
        r = simpleQuery(c, "SELECT count(*) FROM '/tmp/pgwire_tbl'");
        assert(parseDataRow(r[1])[0] == "3");

        c.sendMsg('X', "");
        assert(c.closedByPeer());
    }
    assert(stopMdb(server) == 0);

    // Password authentication.
    {
        const uint16_t port2 = reservePort();
        const pid_t secured = spawnMdb({"pgserve", std::to_string(port2), "--password", "s3cret"});
        {
            PgClient bad(port2);
            const auto r = connectAndStart(bad, "wrong");
            assert(r.size() == 1 && r[0].type == 'E' && field(r[0], 'C') == "28P01");
            assert(bad.closedByPeer());
        }
        {
            PgClient good(port2);
            const auto r = connectAndStart(good, "s3cret");
            assert(r.back().type == 'Z');
            const auto q = simpleQuery(good, "SELECT count(*) FROM '/tmp/pgwire_tbl'");
            assert(parseDataRow(q[1])[0] == "3");
        }
        assert(stopMdb(secured) == 0);
    }

    for (const char* ext : {".mdb", ".mdb.idx", ".mdb.wal", ".mdb.1.str"})
        std::remove((std::string("/tmp/pgwire_tbl") + ext).c_str());
    std::puts("test_pgwire: passed");
    return 0;
}
