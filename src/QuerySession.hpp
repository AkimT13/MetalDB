#pragma once

#include <string>

class Engine;
struct MiniSQLResult;

std::string formatMiniSQLResult(const MiniSQLResult& result);
bool executeMiniSQLToStream(Engine& engine, const std::string& sql, const char* errorPrefix);
