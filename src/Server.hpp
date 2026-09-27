#pragma once

#include <cstddef>
#include <cstdint>
#include <string>

struct ServerOptions {
    uint16_t port = 0;
    std::string bindAddress = "127.0.0.1";  // IPv4 address to listen on
    size_t maxConnections = 64;             // extra clients get "ERR too many connections"
    int idleTimeoutSec = 0;                 // close idle sessions after N seconds (0 = never)
    size_t maxRequestBytes = 16u << 20;     // longest accepted request line
    std::string dataDir;                    // sandbox table / COPY paths under this dir
    bool syncCommit = false;                // fsync the WAL on every commit
    bool verbose = false;                   // log connections to stderr
};

// Line-protocol server (one SQL statement per line; responses framed by END).
// Serves clients concurrently (one thread per connection) over one shared
// Engine; statements are serialized per table. SIGINT / SIGTERM trigger a
// graceful shutdown: stop accepting, let in-flight statements finish, close
// sessions, checkpoint every open table, exit 0.
int runServer(const ServerOptions& options);
int runServer(unsigned short port);
