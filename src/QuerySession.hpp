#pragma once

#include <string>

class Session;
struct MiniSQLResult;

std::string formatMiniSQLResult(const MiniSQLResult& result);
// Runs one statement in `session`, prints the result (and notices) or the error.
bool executeMiniSQLToStream(Session& session, const std::string& sql, const char* errorPrefix);
