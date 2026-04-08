#pragma once

#include <cstdint>
#include <string>
#include <vector>

class Engine;

struct MiniSQLResult {
    std::vector<std::string> headers;
    std::vector<std::vector<std::string>> rows;
};

MiniSQLResult executeMiniSQL(Engine& engine, const std::string& sql);

