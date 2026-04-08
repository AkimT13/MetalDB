#include "QuerySession.hpp"

#include <cstdio>
#include <string>

#include "Engine.hpp"
#include "MiniSQL.hpp"

std::string formatMiniSQLResult(const MiniSQLResult& result) {
    std::string out;
    for (size_t i = 0; i < result.headers.size(); ++i) {
        if (i) out += '\t';
        out += result.headers[i];
    }
    out += '\n';
    for (const auto& row : result.rows) {
        for (size_t i = 0; i < row.size(); ++i) {
            if (i) out += '\t';
            out += row[i];
        }
        out += '\n';
    }
    return out;
}

bool executeMiniSQLToStream(Engine& engine, const std::string& sql, const char* errorPrefix) {
    try {
        const std::string rendered = formatMiniSQLResult(executeMiniSQL(engine, sql));
        std::fwrite(rendered.data(), 1, rendered.size(), stdout);
        return true;
    } catch (const std::exception& ex) {
        std::fprintf(stderr, "%s error: %s\n", errorPrefix, ex.what());
        return false;
    }
}
