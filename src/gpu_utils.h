#pragma once
// Shared utility for locating .metallib files at runtime.
// The metallib is expected to sit alongside the running executable.
#include <string>
#include <mach-o/dyld.h>

inline std::string metallibPath(const char* filename) {
    char buf[2048];
    uint32_t size = sizeof(buf);
    if (_NSGetExecutablePath(buf, &size) == 0) {
        std::string p(buf);
        auto slash = p.rfind('/');
        if (slash != std::string::npos)
            return p.substr(0, slash + 1) + filename;
    }
    return filename; // fallback: resolve against CWD
}
