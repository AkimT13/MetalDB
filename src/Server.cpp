#include "Server.hpp"

#include <atomic>
#include <cerrno>
#include <condition_variable>
#include <csignal>
#include <cstdio>
#include <cstring>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <system_error>
#include <chrono>
#include <string>
#include <thread>
#include <vector>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <poll.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>

#include "Engine.hpp"
#include "MiniSQL.hpp"
#include "QuerySession.hpp"

namespace {

std::atomic<bool> g_stop{false};

void onStopSignal(int) { g_stop.store(true); }

std::string trimLine(const std::string& input) {
    size_t start = 0;
    while (start < input.size() && (input[start] == ' ' || input[start] == '\t' ||
                                    input[start] == '\r' || input[start] == '\n')) {
        ++start;
    }
    size_t end = input.size();
    while (end > start && (input[end - 1] == ' ' || input[end - 1] == '\t' ||
                           input[end - 1] == '\r' || input[end - 1] == '\n')) {
        --end;
    }
    return input.substr(start, end - start);
}

bool sendAll(int fd, const std::string& payload) {
    size_t sent = 0;
    while (sent < payload.size()) {
#ifdef MSG_NOSIGNAL
        const ssize_t rc = ::send(fd, payload.data() + sent, payload.size() - sent, MSG_NOSIGNAL);
#else
        const ssize_t rc = ::send(fd, payload.data() + sent, payload.size() - sent, 0);
#endif
        if (rc < 0) {
            if (errno == EINTR) continue;
            return false;
        }
        sent += static_cast<size_t>(rc);
    }
    return true;
}

// Responses never contain raw newlines inside an ERR line: the message is one line.
std::string errLine(std::string message) {
    for (char& ch : message)
        if (ch == '\n' || ch == '\r') ch = ' ';
    return "ERR\t" + message + "\nEND\n";
}

std::string executeRequest(Engine& engine, const std::string& request) {
    const std::string sql = trimLine(request);
    if (sql.empty()) return "ERR\tempty request\nEND\n";
    if (sql == ".quit") return "BYE\nEND\n";

    try {
        const std::string body = formatMiniSQLResult(executeMiniSQL(engine, sql));
        return "OK\n" + body + "END\n";
    } catch (const std::exception& ex) {
        return errLine(ex.what());
    }
}

struct ConnectionTracker {
    std::mutex mu;
    std::condition_variable cv;
    size_t active = 0;
};

// `tracker` is shared-owned: the accept loop may observe active == 0 and tear down
// while this thread is still returning from the final notify.
// `tracker` is shared-owned: the accept loop may observe active == 0 and tear down
// while this thread is still returning from the final notify.
void runSession(int clientFd, Engine& engine, const ServerOptions& opts,
                std::shared_ptr<ConnectionTracker> tracker, unsigned long id) {
    if (opts.verbose) std::fprintf(stderr, "[conn %lu] open\n", id);
    try {
        SessionIO io(clientFd, opts);
        if (opts.protocol == ServerOptions::Protocol::Postgres) handlePgSession(io, engine, opts);
        else handleLineSession(io, engine, opts);
    } catch (const std::exception& ex) {
        std::fprintf(stderr, "[conn %lu] session error: %s\n", id, ex.what());
    }
    ::close(clientFd);
    if (opts.verbose) std::fprintf(stderr, "[conn %lu] closed\n", id);

    std::lock_guard<std::mutex> g(tracker->mu);
    --tracker->active;
    tracker->cv.notify_all();
}

}  // namespace

SessionIO::Status SessionIO::readSome(std::string& buf) {
    int idleMs = 0;
    char chunk[8192];
    while (true) {
        if (g_stop.load()) return Status::Stopping;
        pollfd pfd{fd_, POLLIN, 0};
        const int ready = ::poll(&pfd, 1, 200);
        if (ready < 0) {
            if (errno == EINTR) continue;
            return Status::Error;
        }
        if (ready == 0) {
            idleMs += 200;
            if (opts_.idleTimeoutSec > 0 && idleMs >= opts_.idleTimeoutSec * 1000) return Status::IdleTimeout;
            continue;
        }
        const ssize_t n = ::recv(fd_, chunk, sizeof(chunk), 0);
        if (n == 0) return Status::Closed;
        if (n < 0) {
            if (errno == EINTR) continue;
            return Status::Error;
        }
        buf.append(chunk, static_cast<size_t>(n));
        return Status::Data;
    }
}

bool SessionIO::send(const std::string& bytes) { return sendAll(fd_, bytes); }

void handleLineSession(SessionIO& io, Engine& engine, const ServerOptions& opts) {
    std::string buffered;
    while (true) {
        const auto status = io.readSome(buffered);
        if (status == SessionIO::Status::IdleTimeout) {
            (void)io.send("ERR\tidle timeout\nEND\n");
            return;
        }
        if (status != SessionIO::Status::Data) return;

        size_t newlinePos = 0;
        while ((newlinePos = buffered.find('\n')) != std::string::npos) {
            std::string line = buffered.substr(0, newlinePos);
            buffered.erase(0, newlinePos + 1);
            if (!io.send(executeRequest(engine, line))) return;
            if (trimLine(line) == ".quit") return;
        }
        if (buffered.size() > opts.maxRequestBytes) {
            (void)io.send(errLine("request exceeds " + std::to_string(opts.maxRequestBytes) + " bytes"));
            return;
        }
    }
}

int runServer(unsigned short port) {
    ServerOptions opts;
    opts.port = port;
    return runServer(opts);
}

int runServer(const ServerOptions& opts) {
    // A client that disconnects mid-response must not kill the whole server.
    std::signal(SIGPIPE, SIG_IGN);
    g_stop.store(false);
    struct sigaction sa{};
    sa.sa_handler = onStopSignal;
    sigemptyset(&sa.sa_mask);
    sa.sa_flags = 0;  // no SA_RESTART: let poll() return EINTR promptly
    sigaction(SIGINT, &sa, nullptr);
    sigaction(SIGTERM, &sa, nullptr);

    Engine engine;
    if (!opts.dataDir.empty()) engine.setDataDir(opts.dataDir);
    engine.setSyncCommit(opts.syncCommit);

    const int listenFd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (listenFd < 0) {
        std::fprintf(stderr, "server error: socket failed: %s\n", std::strerror(errno));
        return 1;
    }

    int reuse = 1;
    if (::setsockopt(listenFd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse)) != 0) {
        std::fprintf(stderr, "server error: setsockopt failed: %s\n", std::strerror(errno));
        ::close(listenFd);
        return 1;
    }
#ifdef SO_NOSIGPIPE
    (void)::setsockopt(listenFd, SOL_SOCKET, SO_NOSIGPIPE, &reuse, sizeof(reuse));
#endif

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(opts.port);
    if (::inet_pton(AF_INET, opts.bindAddress.c_str(), &addr.sin_addr) != 1) {
        std::fprintf(stderr, "server error: invalid bind address '%s'\n", opts.bindAddress.c_str());
        ::close(listenFd);
        return 1;
    }

    if (::bind(listenFd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0) {
        std::fprintf(stderr, "server error: bind failed: %s\n", std::strerror(errno));
        ::close(listenFd);
        return 1;
    }

    if (::listen(listenFd, 128) != 0) {
        std::fprintf(stderr, "server error: listen failed: %s\n", std::strerror(errno));
        ::close(listenFd);
        return 1;
    }

    std::printf("MetalDB %s server listening on %s:%u (max %zu connections%s%s)\n",
                opts.protocol == ServerOptions::Protocol::Postgres ? "PostgreSQL-protocol" : "line-protocol",
                opts.bindAddress.c_str(),
                static_cast<unsigned>(opts.port), opts.maxConnections,
                opts.dataDir.empty() ? "" : ", data dir ", opts.dataDir.c_str());
    std::fflush(stdout);

    auto tracker = std::make_shared<ConnectionTracker>();
    unsigned long nextID = 1;
    while (!g_stop.load()) {
        pollfd pfd{listenFd, POLLIN, 0};
        const int ready = ::poll(&pfd, 1, 200);
        if (ready <= 0) continue;  // timeout or EINTR: re-check g_stop

        const int clientFd = ::accept(listenFd, nullptr, nullptr);
        if (clientFd < 0) {
            if (errno == EINTR || errno == ECONNABORTED || errno == EAGAIN) continue;
            std::fprintf(stderr, "server error: accept failed: %s\n", std::strerror(errno));
            continue;  // e.g. EMFILE: keep serving existing clients
        }
#ifdef SO_NOSIGPIPE
        (void)::setsockopt(clientFd, SOL_SOCKET, SO_NOSIGPIPE, &reuse, sizeof(reuse));
#endif

        {
            std::lock_guard<std::mutex> g(tracker->mu);
            if (tracker->active >= opts.maxConnections) {
                (void)sendAll(clientFd, "ERR\ttoo many connections\nEND\n");
                ::close(clientFd);
                continue;
            }
            ++tracker->active;
        }
        try {
            std::thread(runSession, clientFd, std::ref(engine), std::cref(opts), tracker, nextID++).detach();
        } catch (const std::system_error& ex) {
            std::fprintf(stderr, "server error: cannot start session thread: %s\n", ex.what());
            (void)sendAll(clientFd, "ERR\tserver overloaded\nEND\n");
            ::close(clientFd);
            std::lock_guard<std::mutex> g(tracker->mu);
            --tracker->active;
        }
    }

    // Graceful shutdown: sessions notice g_stop within one poll interval, finish
    // their in-flight statement, and close.
    ::close(listenFd);
    {
        std::unique_lock<std::mutex> lk(tracker->mu);
        tracker->cv.wait_for(lk, std::chrono::seconds(30), [&] { return tracker->active == 0; });
    }
    try {
        engine.flushAll();
    } catch (const std::exception& ex) {
        std::fprintf(stderr, "server error: checkpoint during shutdown failed: %s\n", ex.what());
        return 1;
    }
    std::printf("MetalDB server stopped cleanly\n");
    std::fflush(stdout);
    return 0;
}
