#pragma once

#include <string>
#include <vector>

#include "SqlAst.hpp"

namespace sql {

// Throws std::invalid_argument with a user-facing message on any lexical or
// syntax error.
std::vector<Token> tokenize(const std::string& input);
ParsedStatement parse(const std::string& input);

}  // namespace sql
