#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "ValueTypes.hpp"

class Engine;

struct MiniSQLResult {
    std::vector<std::string> headers;
    // Logical type of each output column (same length as headers). Aggregates
    // report the type of their result: COUNT → INT64, SUM over integers → INT64,
    // SUM over floats / AVG → DOUBLE, MIN / MAX → the input column's type.
    std::vector<ColType> types;
    std::vector<std::vector<std::string>> rows;
};

// Parses and executes one mini-SQL statement. Throws std::invalid_argument for
// user errors (syntax, unknown column, type mismatch, missing table) and
// std::runtime_error for storage failures.
MiniSQLResult executeMiniSQL(Engine& engine, const std::string& sql);
