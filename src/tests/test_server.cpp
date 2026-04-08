#include "../Engine.hpp"

#include <arpa/inet.h>
#include <cassert>
#include <csignal>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <stdexcept>
#include <string>
#include <sys/socket.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

namespace {

uint16_t reservePort() {
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
    if (::getsockname(fd, reinterpret_cast<sockaddr*>(&addr), &len) != 0) {
        ::close(fd);
        throw std::runtime_error("getsockname failed");
    }
    const uint16_t port = ntohs(addr.sin_port);
    ::close(fd);
    return port;
}

int connectToServer(uint16_t port) {
    for (int attempt = 0; attempt < 50; ++attempt) {
        const int fd = ::socket(AF_INET, SOCK_STREAM, 0);
        if (fd < 0) throw std::runtime_error("socket failed");

        sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_port = htons(port);
        ::inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr);
        if (::connect(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) == 0)
            return fd;

        ::close(fd);
        usleep(100000);
    }
    throw std::runtime_error("server did not accept connections");
}

std::string recvResponse(int fd) {
    std::string out;
    char buf[1024];
    while (out.find("END\n") == std::string::npos) {
        const ssize_t nread = ::recv(fd, buf, sizeof(buf), 0);
        if (nread <= 0) throw std::runtime_error("recv failed");
        out.append(buf, static_cast<size_t>(nread));
    }
    return out;
}

std::string sendQuery(int fd, const std::string& query) {
    std::string payload = query;
    payload.push_back('\n');

    size_t sent = 0;
    while (sent < payload.size()) {
        const ssize_t nwritten = ::send(fd, payload.data() + sent, payload.size() - sent, 0);
        if (nwritten <= 0) throw std::runtime_error("send failed");
        sent += static_cast<size_t>(nwritten);
    }
    return recvResponse(fd);
}

pid_t spawnServer(uint16_t port) {
    const pid_t pid = fork();
    if (pid < 0) throw std::runtime_error("fork failed");
    if (pid == 0) {
        const std::string portArg = std::to_string(port);
        execl("./mdb", "./mdb", "serve", portArg.c_str(), static_cast<char*>(nullptr));
        _exit(127);
    }
    return pid;
}

void stopServer(pid_t pid) {
    ::kill(pid, SIGTERM);
    int status = 0;
    (void)::waitpid(pid, &status, 0);
}

} // namespace

int main() {
    Engine e;
    e.createTypedTable("/tmp/sql_server", {ColType::UINT32, ColType::UINT32, ColType::STRING});
    e.insertTyped("/tmp/sql_server", {ColValue(uint32_t(1)), ColValue(uint32_t(10)), ColValue(std::string("alice"))});
    e.insertTyped("/tmp/sql_server", {ColValue(uint32_t(2)), ColValue(uint32_t(20)), ColValue(std::string("bob"))});
    e.insertTyped("/tmp/sql_server", {ColValue(uint32_t(2)), ColValue(uint32_t(30)), ColValue(std::string("alice"))});

    const uint16_t port = reservePort();
    const pid_t serverPid = spawnServer(port);

    try {
        int fd = connectToServer(port);

        std::string r1 = sendQuery(fd, "SELECT c0, c1 FROM '/tmp/sql_server' WHERE c0 = 2");
        assert(r1 == "OK\nc0\tc1\n2\t20\n2\t30\nEND\n");

        std::string r2 = sendQuery(fd, "SELECT count(*) FROM '/tmp/sql_server' WHERE c2 = 'alice'");
        assert(r2 == "OK\ncount(*)\n2\nEND\n");

        std::string r3 = sendQuery(fd, "SELECT c0 FROM '/tmp/sql_server' WHERE c0 = 1 AND c1 = 10 OR c2 = 'alice'");
        assert(r3.rfind("ERR\tmixed AND/OR WHERE clauses are not supported\nEND\n", 0) == 0);

        std::string r4 = sendQuery(fd, "");
        assert(r4 == "ERR\tempty request\nEND\n");

        std::string r5 = sendQuery(fd, ".quit");
        assert(r5 == "BYE\nEND\n");
        ::close(fd);

        fd = connectToServer(port);
        std::string r6 = sendQuery(fd, "SELECT c9 FROM '/tmp/sql_server'");
        assert(r6.rfind("ERR\tcolumn index out of bounds\nEND\n", 0) == 0);

        std::string r7 = sendQuery(fd, "SELECT c0 FROM '/tmp/sql_missing'");
        assert(r7.rfind("ERR\t", 0) == 0);
        assert(::access("/tmp/sql_missing.mdb", F_OK) != 0);

        std::string r8 = sendQuery(fd, ".quit");
        assert(r8 == "BYE\nEND\n");
        ::close(fd);
    } catch (...) {
        stopServer(serverPid);
        std::remove("/tmp/sql_server.mdb");
        std::remove("/tmp/sql_server.mdb.idx");
        std::remove("/tmp/sql_server.mdb.2.str");
        throw;
    }

    stopServer(serverPid);
    std::remove("/tmp/sql_server.mdb");
    std::remove("/tmp/sql_server.mdb.idx");
    std::remove("/tmp/sql_server.mdb.2.str");
    std::puts("test_server: passed");
    return 0;
}
