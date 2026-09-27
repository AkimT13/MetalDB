#pragma once
// Validation, literal coercion, and evaluation of mini-SQL WHERE trees.
//
// Evaluation produces a sorted vector of live row IDs. Leaves on UINT32 columns
// with =, <, <=, >, >=, BETWEEN are lowered onto Table::scanPredicate so they keep
// the zone-map pruning and hybrid GPU/CPU dispatch; everything else (typed numeric
// columns, string ordering, IN, !=, NOT) runs as a CPU scan or as sorted row-ID
// set algebra (intersect / union / complement).

#include <cstdint>
#include <string>
#include <vector>

#include "SqlAst.hpp"

class Table;

namespace sql {

// Converts a literal into a ColValue of exactly `type` (INSERT / UPDATE values).
// Throws std::invalid_argument if the literal does not fit the column type.
ColValue coerceLiteral(const Token& token, ColType type, size_t colIdx);

// Checks column bounds and literal/column type compatibility for every leaf.
void validateWhere(const Table& table, const WhereExpr& expr);

// Evaluates `expr` (validated first) against `table`. When `trace` is non-null,
// one human-readable line per leaf access path is appended (used by EXPLAIN).
std::vector<uint32_t> evaluateWhere(Table& table, const WhereExpr& expr,
                                    std::vector<std::string>* trace = nullptr);

// Renders the expression back to SQL-ish text (EXPLAIN / error messages).
std::string whereToString(const WhereExpr& expr);

std::vector<uint32_t> allLiveRowIDs(Table& table);

}  // namespace sql
