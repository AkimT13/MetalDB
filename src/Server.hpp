#pragma once

#include <cstddef>
#include <cstdint>
#include <string>

class Engine;

struct ServerOptions {
    enum class Protocol { Line, Postgres };

    Protocol protocol = Protocol::Line;
    uint16_t port = 0;
    std::string bindAddress = "127.0.0.1";  // IPv4 address to listen on
    size_t maxConnections = 64;             // extra clients get "ERR too many connections"
    int idleTimeoutSec = 0;                 // close idle sessions after N seconds (0 = never)
    size_t maxRequestBytes = 16u << 20;     // longest accepted request line
    std::string dataDir;                    // sandbox table / COPY paths under this dir
    bool syncCommit = false;                // fsync the WAL on every commit
    bool verbose = false;                   // log connections to stderr
    std::string password;                   // Postgres: require this cleartext password (empty = trust)
};

// Buffered, stop-aware socket I/O shared by the protocol handlers.
class SessionIO {
public:
    enum class Status { Data, Closed, Stopping, IdleTimeout, Error };

    SessionIO(int fd, const ServerOptions& opts) : fd_(fd), opts_(opts) {}

    // Appends newly received bytes to `buf`. Returns Data when something was read;
    // otherwise why the session should end.
    Status readSome(std::string& buf);
    bool send(const std::string& bytes);
    int fd() const { return fd_; }

private:
    int fd_;
    const ServerOptions& opts_;
};

// Protocol handlers (Server.cpp: line protocol, PgWire.cpp: Postgres v3).
void handleLineSession(SessionIO& io, Engine& engine, const ServerOptions& opts);
void handlePgSession(SessionIO& io, Engine& engine, const ServerOptions& opts);

// Line-protocol server (one SQL statement per line; responses framed by END), or
// a PostgreSQL v3 wire-protocol server when options.protocol == Postgres.
// Serves clients concurrently (one thread per connection) over one shared
// Engine; statements are serialized per table. SIGINT / SIGTERM trigger a
// graceful shutdown: stop accepting, let in-flight statements finish, close
// sessions, checkpoint every open table, exit 0.
int runServer(const ServerOptions& options);
int runServer(unsigned short port);
