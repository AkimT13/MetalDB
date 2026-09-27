#pragma once

#include <string>
#include <vector>

#include "SqlAst.hpp"

namespace sql {

// Throws std::invalid_argument with a user-facing message on any lexical or
// syntax error.
std::vector<Token> tokenize(const std::string& input);

// `params` binds $1..$n (text values). A statement containing placeholders
// cannot be parsed without them.
ParsedStatement parse(const std::string& input, const std::vector<std::string>* params = nullptr);

// Highest $n placeholder index in the statement (0 if none).
size_t countParams(const std::string& input);

}  // namespace sql
