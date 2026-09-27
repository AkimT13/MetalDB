#pragma once
// Mini-SQL abstract syntax tree shared by the parser (SqlParser), the WHERE
// evaluator (WhereEval), and the executor (MiniSQL).

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "ValueTypes.hpp"

namespace sql {

enum class TokenKind {
    Identifier,
    Number,
    String,
    Comma,
    Star,
    LParen,
    RParen,
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
    Semicolon,
    End,
};

struct Token {
    TokenKind kind = TokenKind::End;
    std::string text;
};

struct ColumnRef {
    uint16_t index = 0;
    std::string text;  // normalized "cN"
};

struct SelectItem {
    enum class Kind {
        Column,
        Star,
        CountStar,
        Count,   // COUNT(cN)
        Sum,
        Min,
        Max,
        Avg,
    };

    Kind kind = Kind::Column;
    ColumnRef column;

    bool isAggregate() const { return kind != Kind::Column && kind != Kind::Star; }
    // Output header, e.g. "c1", "count(*)", "sum(c2)".
    std::string header() const;
};

// Boolean WHERE expression tree.
struct WhereExpr {
    enum class Kind {
        Compare,  // column <op> literal
        Between,  // column BETWEEN lo AND hi   (literals[0], literals[1])
        In,       // column IN (literals...)
        And,
        Or,
        Not,
    };
    enum class Op { Eq, Ne, Lt, Le, Gt, Ge };

    Kind kind = Kind::Compare;
    Op op = Op::Eq;
    ColumnRef column;
    std::vector<Token> literals;
    std::vector<std::shared_ptr<WhereExpr>> children;
};

struct OrderKey {
    std::string header;   // output column header to sort on (e.g. "c1", "count(*)")
    size_t position = 0;  // 1-based output position when ordered by number; 0 otherwise
    bool descending = false;
};

struct ParsedQuery {
    std::string tableName;
    std::vector<SelectItem> selectItems;
    bool distinct = false;             // SELECT DISTINCT
    std::shared_ptr<WhereExpr> where;  // null when there is no WHERE clause
    std::vector<ColumnRef> groupBy;    // empty when there is no GROUP BY
    std::vector<OrderKey> orderBy;
    bool hasLimit = false;
    uint64_t limit = 0;
    uint64_t offset = 0;
};

struct Assignment {
    ColumnRef column;
    Token literal;
};

struct ParsedStatement {
    enum class Kind {
        Select,
        CreateTable,
        Insert,
        Delete,
        Update,
        Describe,
    };

    Kind kind = Kind::Select;
    ParsedQuery query;                          // Select; tableName/where also used by Delete/Update
    std::vector<ColType> columnTypes;           // CreateTable
    std::vector<std::vector<Token>> insertRows; // Insert: literal tokens, coerced at execution time
    std::vector<Assignment> assignments;        // Update
};

bool equalsIgnoreCase(const std::string& lhs, const char* rhs);
const char* colTypeName(ColType type);

}  // namespace sql
