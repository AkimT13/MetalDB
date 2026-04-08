#include "Server.hpp"

#include <cstdio>
#include <cerrno>
#include <cstring>
#include <stdexcept>
#include <string>
#include <vector>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>

#include "Engine.hpp"
#include "MiniSQL.hpp"
#include "QuerySession.hpp"

namespace {

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
        const ssize_t rc = ::send(fd, payload.data() + sent, payload.size() - sent, 0);
        if (rc < 0) {
            if (errno == EINTR) continue;
            return false;
        }
        sent += static_cast<size_t>(rc);
    }
    return true;
}

std::string executeRequest(Engine& engine, const std::string& request) {
    const std::string sql = trimLine(request);
    if (sql.empty()) return "ERR\tempty request\nEND\n";
    if (sql == ".quit") return "BYE\nEND\n";

    try {
        const std::string body = formatMiniSQLResult(executeMiniSQL(engine, sql));
        return "OK\n" + body + "END\n";
    } catch (const std::exception& ex) {
        return std::string("ERR\t") + ex.what() + "\nEND\n";
    }
}

bool handleClient(int clientFd) {
    Engine engine;
    std::string buffered;
    char chunk[4096];

    while (true) {
        const ssize_t nread = ::recv(clientFd, chunk, sizeof(chunk), 0);
        if (nread == 0) return true;
        if (nread < 0) {
            if (errno == EINTR) continue;
            return false;
        }
        buffered.append(chunk, static_cast<size_t>(nread));

        size_t newlinePos = 0;
        while ((newlinePos = buffered.find('\n')) != std::string::npos) {
            std::string line = buffered.substr(0, newlinePos);
            buffered.erase(0, newlinePos + 1);
            const std::string response = executeRequest(engine, line);
            if (!sendAll(clientFd, response)) return false;
            if (trimLine(line) == ".quit") return true;
        }
    }
}

} // namespace

int runServer(unsigned short port) {
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

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    addr.sin_port = htons(port);

    if (::bind(listenFd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0) {
        std::fprintf(stderr, "server error: bind failed: %s\n", std::strerror(errno));
        ::close(listenFd);
        return 1;
    }

    if (::listen(listenFd, 16) != 0) {
        std::fprintf(stderr, "server error: listen failed: %s\n", std::strerror(errno));
        ::close(listenFd);
        return 1;
    }

    std::printf("MetalDB server listening on 127.0.0.1:%u\n", static_cast<unsigned>(port));
    std::fflush(stdout);

    while (true) {
        const int clientFd = ::accept(listenFd, nullptr, nullptr);
        if (clientFd < 0) {
            if (errno == EINTR) continue;
            std::fprintf(stderr, "server error: accept failed: %s\n", std::strerror(errno));
            ::close(listenFd);
            return 1;
        }
        (void)handleClient(clientFd);
        ::close(clientFd);
    }
}
