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
// `params` binds $1..$n placeholders (text values, typed by the column they meet).
MiniSQLResult executeMiniSQL(Engine& engine, const std::string& sql,
                             const std::vector<std::string>* params = nullptr);

// Result shape (headers + types, no rows) without executing the statement. Row-
// less statements (INSERT / UPDATE / DELETE / CREATE TABLE / COPY) describe as no
// columns. Unbound placeholders are treated as unknown values.
MiniSQLResult describeMiniSQL(Engine& engine, const std::string& sql,
                              const std::vector<std::string>* params = nullptr);
