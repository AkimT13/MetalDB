#pragma once
// Socket / child-process helpers shared by the server integration tests.

#include <arpa/inet.h>
#include <csignal>
#include <cstdint>
#include <stdexcept>
#include <string>
#include <sys/socket.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>
#include <vector>

namespace nettest {

inline uint16_t reservePort() {
    const int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) throw std::runtime_error("socket failed");
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    addr.sin_port = 0;
    if (::bind(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0) {
        ::close(fd);
        throw std::runtime_error("bind failed");
    }
    socklen_t len = sizeof(addr);
    ::getsockname(fd, reinterpret_cast<sockaddr*>(&addr), &len);
    ::close(fd);
    return ntohs(addr.sin_port);
}

inline int connectTo(uint16_t port) {
    for (int attempt = 0; attempt < 100; ++attempt) {
        const int fd = ::socket(AF_INET, SOCK_STREAM, 0);
        if (fd < 0) throw std::runtime_error("socket failed");
        sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_port = htons(port);
        ::inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr);
        if (::connect(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) == 0) return fd;
        ::close(fd);
        usleep(50000);
    }
    throw std::runtime_error("server did not accept connections");
}

inline void sendAll(int fd, const std::string& payload) {
    size_t sent = 0;
    while (sent < payload.size()) {
        const ssize_t n = ::send(fd, payload.data() + sent, payload.size() - sent, 0);
        if (n <= 0) throw std::runtime_error("send failed");
        sent += static_cast<size_t>(n);
    }
}

// Line protocol: send one statement, read until the END terminator.
inline std::string query(int fd, const std::string& sql) {
    sendAll(fd, sql + "\n");
    std::string out;
    char buf[4096];
    while (out.size() < 4 || out.compare(out.size() - 4, 4, "END\n") != 0) {
        const ssize_t n = ::recv(fd, buf, sizeof(buf), 0);
        if (n <= 0) throw std::runtime_error("recv failed (connection closed)");
        out.append(buf, static_cast<size_t>(n));
    }
    return out;
}

// Forks ./mdb with the given arguments. If the test aborts on a failed assert,
// the handler below kills the child so it cannot outlive the test run.
inline volatile pid_t g_child = 0;

inline void killChildOnAbort(int sig) {
    if (g_child > 0) ::kill(g_child, SIGKILL);
    std::signal(sig, SIG_DFL);
    std::raise(sig);
}

inline pid_t spawnMdb(const std::vector<std::string>& args) {
    const pid_t pid = ::fork();
    if (pid < 0) throw std::runtime_error("fork failed");
    if (pid == 0) {
        std::vector<char*> argv;
        argv.push_back(const_cast<char*>("./mdb"));
        for (const auto& a : args) argv.push_back(const_cast<char*>(a.c_str()));
        argv.push_back(nullptr);
        ::execv("./mdb", argv.data());
        _exit(127);
    }
    g_child = pid;
    std::signal(SIGABRT, killChildOnAbort);
    std::signal(SIGSEGV, killChildOnAbort);
    return pid;
}

// SIGTERM + wait; returns the exit status (or -1 if killed by a signal).
inline int stopMdb(pid_t pid) {
    ::kill(pid, SIGTERM);
    int status = 0;
    ::waitpid(pid, &status, 0);
    g_child = 0;
    return WIFEXITED(status) ? WEXITSTATUS(status) : -1;
}

}  // namespace nettest
