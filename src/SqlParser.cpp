#include "SqlParser.hpp"

#include <algorithm>
#include <cctype>
#include <stdexcept>
#include <utility>

namespace sql {

bool equalsIgnoreCase(const std::string& lhs, const char* rhs) {
    size_t i = 0;
    for (; i < lhs.size() && rhs[i]; ++i) {
        if (std::toupper(static_cast<unsigned char>(lhs[i])) !=
            std::toupper(static_cast<unsigned char>(rhs[i]))) {
            return false;
        }
    }
    return i == lhs.size() && rhs[i] == '\0';
}

const char* colTypeName(ColType type) {
    switch (type) {
        case ColType::UINT32: return "UINT32";
        case ColType::INT64: return "INT64";
        case ColType::FLOAT: return "FLOAT";
        case ColType::DOUBLE: return "DOUBLE";
        case ColType::STRING: return "STRING";
    }
    return "UNKNOWN";
}

std::string SelectItem::header() const {
    switch (kind) {
        case Kind::Column: return column.text;
        case Kind::Star: return "*";
        case Kind::CountStar: return "count(*)";
        case Kind::Count: return "count(" + column.text + ")";
        case Kind::Sum: return "sum(" + column.text + ")";
        case Kind::Min: return "min(" + column.text + ")";
        case Kind::Max: return "max(" + column.text + ")";
        case Kind::Avg: return "avg(" + column.text + ")";
    }
    return "";
}

namespace {

// ── Lexer ────────────────────────────────────────────────────────────────────

class Tokenizer {
public:
    explicit Tokenizer(const std::string& input) : input_(input) {}

    std::vector<Token> run() {
        std::vector<Token> tokens;
        while (true) {
            skipWhitespaceAndComments();
            if (pos_ >= input_.size()) {
                tokens.push_back({TokenKind::End, ""});
                return tokens;
            }

            const char ch = input_[pos_];
            if (std::isalpha(static_cast<unsigned char>(ch)) || ch == '_') {
                tokens.push_back(readIdentifier());
                continue;
            }
            if (std::isdigit(static_cast<unsigned char>(ch)) ||
                ((ch == '-' || ch == '.') && pos_ + 1 < input_.size() &&
                 std::isdigit(static_cast<unsigned char>(input_[pos_ + 1])))) {
                tokens.push_back(readNumber());
                continue;
            }
            if (ch == '\'') {
                tokens.push_back(readString());
                continue;
            }

            ++pos_;
            const char next = pos_ < input_.size() ? input_[pos_] : '\0';
            switch (ch) {
                case ',': tokens.push_back({TokenKind::Comma, ","}); break;
                case '*': tokens.push_back({TokenKind::Star, "*"}); break;
                case '(': tokens.push_back({TokenKind::LParen, "("}); break;
                case ')': tokens.push_back({TokenKind::RParen, ")"}); break;
                case ';': tokens.push_back({TokenKind::Semicolon, ";"}); break;
                case '=': tokens.push_back({TokenKind::Eq, "="}); break;
                case '!':
                    if (next != '=') throw std::invalid_argument("unexpected character: !");
                    ++pos_;
                    tokens.push_back({TokenKind::Ne, "!="});
                    break;
                case '<':
                    if (next == '=') {
                        ++pos_;
                        tokens.push_back({TokenKind::Le, "<="});
                    } else if (next == '>') {
                        ++pos_;
                        tokens.push_back({TokenKind::Ne, "<>"});
                    } else {
                        tokens.push_back({TokenKind::Lt, "<"});
                    }
                    break;
                case '>':
                    if (next == '=') {
                        ++pos_;
                        tokens.push_back({TokenKind::Ge, ">="});
                    } else {
                        tokens.push_back({TokenKind::Gt, ">"});
                    }
                    break;
                default:
                    throw std::invalid_argument(std::string("unexpected character: ") + ch);
            }
        }
    }

private:
    Token readIdentifier() {
        const size_t start = pos_;
        while (pos_ < input_.size()) {
            const char ch = input_[pos_];
            if (!std::isalnum(static_cast<unsigned char>(ch)) && ch != '_') break;
            ++pos_;
        }
        return {TokenKind::Identifier, input_.substr(start, pos_ - start)};
    }

    // Accepts an optional leading '-', digits, an optional fraction and exponent.
    // Callers decide which shapes are valid for the target column type.
    Token readNumber() {
        const size_t start = pos_;
        if (input_[pos_] == '-') ++pos_;
        auto digits = [&] {
            while (pos_ < input_.size() && std::isdigit(static_cast<unsigned char>(input_[pos_]))) ++pos_;
        };
        digits();
        if (pos_ < input_.size() && input_[pos_] == '.') {
            ++pos_;
            digits();
        }
        if (pos_ < input_.size() && (input_[pos_] == 'e' || input_[pos_] == 'E')) {
            size_t p = pos_ + 1;
            if (p < input_.size() && (input_[p] == '+' || input_[p] == '-')) ++p;
            if (p < input_.size() && std::isdigit(static_cast<unsigned char>(input_[p]))) {
                pos_ = p;
                digits();
            }
        }
        return {TokenKind::Number, input_.substr(start, pos_ - start)};
    }

    Token readString() {
        ++pos_;
        std::string value;
        while (pos_ < input_.size()) {
            const char ch = input_[pos_++];
            if (ch == '\'') {
                // '' inside a literal is an escaped single quote
                if (pos_ < input_.size() && input_[pos_] == '\'') {
                    value.push_back('\'');
                    ++pos_;
                    continue;
                }
                return {TokenKind::String, value};
            }
            value.push_back(ch);
        }
        throw std::invalid_argument("unterminated string literal");
    }

    // Whitespace and `-- line comments`.
    void skipWhitespaceAndComments() {
        while (pos_ < input_.size()) {
            if (std::isspace(static_cast<unsigned char>(input_[pos_]))) {
                ++pos_;
            } else if (input_[pos_] == '-' && pos_ + 1 < input_.size() && input_[pos_ + 1] == '-') {
                while (pos_ < input_.size() && input_[pos_] != '\n') ++pos_;
            } else {
                return;
            }
        }
    }

    const std::string& input_;
    size_t pos_ = 0;
};

// ── Parser ───────────────────────────────────────────────────────────────────

class Parser {
public:
    explicit Parser(const std::vector<Token>& tokens) : tokens_(tokens) {}

    ParsedStatement parse() {
        ParsedStatement stmt;
        if (matchKeyword("CREATE")) {
            stmt.kind = ParsedStatement::Kind::CreateTable;
            parseCreateTable(stmt);
        } else if (matchKeyword("INSERT")) {
            stmt.kind = ParsedStatement::Kind::Insert;
            parseInsert(stmt);
        } else if (matchKeyword("DELETE")) {
            stmt.kind = ParsedStatement::Kind::Delete;
            parseDelete(stmt);
        } else if (matchKeyword("UPDATE")) {
            stmt.kind = ParsedStatement::Kind::Update;
            parseUpdate(stmt);
        } else if (matchKeyword("DESCRIBE")) {
            stmt.kind = ParsedStatement::Kind::Describe;
            stmt.query.tableName = expectTablePath();
        } else {
            stmt.kind = ParsedStatement::Kind::Select;
            stmt.query = parseSelect();
        }

        if (peek().kind == TokenKind::Semicolon) ++pos_;
        expect(TokenKind::End, "end of query");
        return stmt;
    }

private:
    ParsedQuery parseSelect() {
        ParsedQuery query;
        expectKeyword("SELECT");
        query.selectItems = parseSelectList();
        expectKeyword("FROM");
        query.tableName = expectTablePath();

        if (matchKeyword("WHERE")) query.where = parseOr();

        if (matchKeyword("GROUP")) {
            expectKeyword("BY");
            query.hasGroupBy = true;
            query.groupBy = parseColumnRef(expect(TokenKind::Identifier, "group-by column"));
        }

        if (matchKeyword("ORDER")) {
            expectKeyword("BY");
            do {
                query.orderBy.push_back(parseOrderKey());
            } while (match(TokenKind::Comma));
        }

        if (matchKeyword("LIMIT")) {
            query.hasLimit = true;
            query.limit = parseCount(expect(TokenKind::Number, "LIMIT count"));
            if (matchKeyword("OFFSET"))
                query.offset = parseCount(expect(TokenKind::Number, "OFFSET count"));
        }
        return query;
    }

    // CREATE TABLE '<path>' (UINT32, STRING, ...)   or   (c0 UINT32, c1 STRING, ...)
    void parseCreateTable(ParsedStatement& stmt) {
        expectKeyword("TABLE");
        stmt.query.tableName = expectTablePath();
        expect(TokenKind::LParen, "(");
        do {
            Token token = expect(TokenKind::Identifier, "column type");
            if (peek().kind == TokenKind::Identifier) {
                const ColumnRef ref = parseColumnRef(token);
                if (ref.index != stmt.columnTypes.size())
                    throw std::invalid_argument("CREATE TABLE columns must be named c0, c1, ... in order");
                token = expect(TokenKind::Identifier, "column type");
            }
            stmt.columnTypes.push_back(parseColType(token));
        } while (match(TokenKind::Comma));
        expect(TokenKind::RParen, ")");
        if (stmt.columnTypes.size() > 0xFFFFu)
            throw std::invalid_argument("too many columns");
    }

    // INSERT INTO '<path>' VALUES (v0, v1, ...) [, (...)]*
    void parseInsert(ParsedStatement& stmt) {
        expectKeyword("INTO");
        stmt.query.tableName = expectTablePath();
        expectKeyword("VALUES");
        do {
            expect(TokenKind::LParen, "(");
            std::vector<Token> row;
            do {
                row.push_back(expectLiteral("in VALUES"));
            } while (match(TokenKind::Comma));
            expect(TokenKind::RParen, ")");
            stmt.insertRows.push_back(std::move(row));
        } while (match(TokenKind::Comma));
    }

    // DELETE FROM '<path>' [WHERE ...]
    void parseDelete(ParsedStatement& stmt) {
        expectKeyword("FROM");
        stmt.query.tableName = expectTablePath();
        if (matchKeyword("WHERE")) stmt.query.where = parseOr();
    }

    // UPDATE '<path>' SET cN = literal [, cM = literal]* [WHERE ...]
    void parseUpdate(ParsedStatement& stmt) {
        stmt.query.tableName = expectTablePath();
        expectKeyword("SET");
        do {
            Assignment a;
            a.column = parseColumnRef(expect(TokenKind::Identifier, "column to SET"));
            expect(TokenKind::Eq, "=");
            a.literal = expectLiteral("in SET");
            for (const auto& prev : stmt.assignments)
                if (prev.column.index == a.column.index)
                    throw std::invalid_argument("column " + a.column.text + " assigned twice");
            stmt.assignments.push_back(std::move(a));
        } while (match(TokenKind::Comma));
        if (matchKeyword("WHERE")) stmt.query.where = parseOr();
    }

    // ── WHERE grammar ────────────────────────────────────────────────────────
    //   or      := and (OR and)*
    //   and     := not (AND not)*
    //   not     := NOT not | primary
    //   primary := '(' or ')'
    //            | col op literal
    //            | col [NOT] BETWEEN literal AND literal
    //            | col [NOT] IN '(' literal (',' literal)* ')'
    std::shared_ptr<WhereExpr> parseOr() {
        auto lhs = parseAnd();
        if (!isKeyword("OR")) return lhs;
        auto node = std::make_shared<WhereExpr>();
        node->kind = WhereExpr::Kind::Or;
        node->children.push_back(std::move(lhs));
        while (matchKeyword("OR")) node->children.push_back(parseAnd());
        return node;
    }

    std::shared_ptr<WhereExpr> parseAnd() {
        auto lhs = parseNot();
        if (!isKeyword("AND")) return lhs;
        auto node = std::make_shared<WhereExpr>();
        node->kind = WhereExpr::Kind::And;
        node->children.push_back(std::move(lhs));
        while (matchKeyword("AND")) node->children.push_back(parseNot());
        return node;
    }

    std::shared_ptr<WhereExpr> parseNot() {
        if (matchKeyword("NOT")) return negate(parseNot());
        return parsePrimary();
    }

    std::shared_ptr<WhereExpr> parsePrimary() {
        if (match(TokenKind::LParen)) {
            auto inner = parseOr();
            expect(TokenKind::RParen, ")");
            return inner;
        }

        auto leaf = std::make_shared<WhereExpr>();
        leaf->column = parseColumnRef(expect(TokenKind::Identifier, "predicate column"));

        const bool negated = matchKeyword("NOT");
        if (matchKeyword("BETWEEN")) {
            leaf->kind = WhereExpr::Kind::Between;
            leaf->literals.push_back(expectLiteral("as BETWEEN lower bound"));
            expectKeyword("AND");
            leaf->literals.push_back(expectLiteral("as BETWEEN upper bound"));
            return negated ? negate(leaf) : leaf;
        }
        if (matchKeyword("IN")) {
            leaf->kind = WhereExpr::Kind::In;
            expect(TokenKind::LParen, "(");
            do {
                leaf->literals.push_back(expectLiteral("in IN list"));
            } while (match(TokenKind::Comma));
            expect(TokenKind::RParen, ")");
            return negated ? negate(leaf) : leaf;
        }
        if (negated) throw std::invalid_argument("expected BETWEEN or IN after NOT");

        leaf->kind = WhereExpr::Kind::Compare;
        switch (peek().kind) {
            case TokenKind::Eq: leaf->op = WhereExpr::Op::Eq; break;
            case TokenKind::Ne: leaf->op = WhereExpr::Op::Ne; break;
            case TokenKind::Lt: leaf->op = WhereExpr::Op::Lt; break;
            case TokenKind::Le: leaf->op = WhereExpr::Op::Le; break;
            case TokenKind::Gt: leaf->op = WhereExpr::Op::Gt; break;
            case TokenKind::Ge: leaf->op = WhereExpr::Op::Ge; break;
            default:
                throw std::invalid_argument("expected comparison operator, BETWEEN, or IN");
        }
        ++pos_;
        leaf->literals.push_back(expectLiteral("after comparison operator"));
        return leaf;
    }

    static std::shared_ptr<WhereExpr> negate(std::shared_ptr<WhereExpr> inner) {
        auto node = std::make_shared<WhereExpr>();
        node->kind = WhereExpr::Kind::Not;
        node->children.push_back(std::move(inner));
        return node;
    }

    // ── helpers ──────────────────────────────────────────────────────────────

    std::vector<SelectItem> parseSelectList() {
        std::vector<SelectItem> items;
        do {
            items.push_back(parseSelectItem());
        } while (match(TokenKind::Comma));
        return items;
    }

    SelectItem parseSelectItem() {
        if (match(TokenKind::Star)) {
            SelectItem item;
            item.kind = SelectItem::Kind::Star;
            return item;
        }

        const Token token = expect(TokenKind::Identifier, "select item");
        if (peek().kind == TokenKind::LParen) {
            if (equalsIgnoreCase(token.text, "COUNT")) return parseAggregate(SelectItem::Kind::Count, true);
            if (equalsIgnoreCase(token.text, "SUM")) return parseAggregate(SelectItem::Kind::Sum, false);
            if (equalsIgnoreCase(token.text, "MIN")) return parseAggregate(SelectItem::Kind::Min, false);
            if (equalsIgnoreCase(token.text, "MAX")) return parseAggregate(SelectItem::Kind::Max, false);
            if (equalsIgnoreCase(token.text, "AVG")) return parseAggregate(SelectItem::Kind::Avg, false);
            throw std::invalid_argument("unknown function: " + token.text);
        }

        SelectItem item;
        item.kind = SelectItem::Kind::Column;
        item.column = parseColumnRef(token);
        return item;
    }

    SelectItem parseAggregate(SelectItem::Kind kind, bool allowStar) {
        expect(TokenKind::LParen, "(");
        SelectItem item;
        item.kind = kind;
        if (allowStar && match(TokenKind::Star)) {
            expect(TokenKind::RParen, ")");
            item.kind = SelectItem::Kind::CountStar;
            return item;
        }
        item.column = parseColumnRef(expect(TokenKind::Identifier, "aggregate column"));
        expect(TokenKind::RParen, ")");
        return item;
    }

    OrderKey parseOrderKey() {
        OrderKey key;
        if (peek().kind == TokenKind::Number) {
            key.position = parseCount(tokens_[pos_++]);
            if (key.position == 0) throw std::invalid_argument("ORDER BY position must be >= 1");
        } else {
            const SelectItem item = parseSelectItem();
            if (item.kind == SelectItem::Kind::Star)
                throw std::invalid_argument("ORDER BY * is not supported");
            key.header = item.header();
        }
        if (matchKeyword("DESC")) key.descending = true;
        else (void)matchKeyword("ASC");
        return key;
    }

    std::string expectTablePath() {
        const std::string path = expect(TokenKind::String, "table path string literal").text;
        if (path.empty()) throw std::invalid_argument("table path must not be empty");
        return path;
    }

    Token expectLiteral(const char* where) {
        if (peek().kind != TokenKind::Number && peek().kind != TokenKind::String)
            throw std::invalid_argument(std::string("expected numeric or string literal ") + where);
        return tokens_[pos_++];
    }

    static ColType parseColType(const Token& token) {
        if (equalsIgnoreCase(token.text, "UINT32")) return ColType::UINT32;
        if (equalsIgnoreCase(token.text, "INT64")) return ColType::INT64;
        if (equalsIgnoreCase(token.text, "FLOAT")) return ColType::FLOAT;
        if (equalsIgnoreCase(token.text, "DOUBLE")) return ColType::DOUBLE;
        if (equalsIgnoreCase(token.text, "STRING")) return ColType::STRING;
        throw std::invalid_argument("unknown column type: " + token.text);
    }

    static ColumnRef parseColumnRef(const Token& token) {
        if (token.kind != TokenKind::Identifier || token.text.size() < 2 ||
            (token.text[0] != 'c' && token.text[0] != 'C')) {
            throw std::invalid_argument("expected column reference like c0");
        }
        const std::string digits = token.text.substr(1);
        if (!std::all_of(digits.begin(), digits.end(),
                         [](char ch) { return std::isdigit(static_cast<unsigned char>(ch)); })) {
            throw std::invalid_argument("expected column reference like c0");
        }
        if (digits.size() > 5 || std::stoul(digits) > 0xFFFFul)
            throw std::invalid_argument("column index out of range");
        const unsigned long index = std::stoul(digits);
        return {static_cast<uint16_t>(index), "c" + std::to_string(index)};
    }

    static uint64_t parseCount(const Token& token) {
        if (token.text.empty() ||
            !std::all_of(token.text.begin(), token.text.end(),
                         [](char ch) { return std::isdigit(static_cast<unsigned char>(ch)); }))
            throw std::invalid_argument("expected non-negative integer, got " + token.text);
        try {
            return std::stoull(token.text);
        } catch (const std::out_of_range&) {
            throw std::invalid_argument("integer out of range: " + token.text);
        }
    }

    bool match(TokenKind kind) {
        if (peek().kind != kind) return false;
        ++pos_;
        return true;
    }

    bool isKeyword(const char* keyword) const {
        return peek().kind == TokenKind::Identifier && equalsIgnoreCase(peek().text, keyword);
    }

    bool matchKeyword(const char* keyword) {
        if (!isKeyword(keyword)) return false;
        ++pos_;
        return true;
    }

    Token expect(TokenKind kind, const char* what) {
        if (peek().kind != kind) {
            const std::string got = peek().kind == TokenKind::End ? "end of input" : "'" + peek().text + "'";
            throw std::invalid_argument(std::string("expected ") + what + ", got " + got);
        }
        return tokens_[pos_++];
    }

    void expectKeyword(const char* keyword) {
        if (!matchKeyword(keyword)) {
            const std::string got = peek().kind == TokenKind::End ? "end of input" : "'" + peek().text + "'";
            throw std::invalid_argument(std::string("expected keyword ") + keyword + ", got " + got);
        }
    }

    const Token& peek() const { return tokens_[pos_]; }

    const std::vector<Token>& tokens_;
    size_t pos_ = 0;
};

}  // namespace

std::vector<Token> tokenize(const std::string& input) {
    return Tokenizer(input).run();
}

ParsedStatement parse(const std::string& input) {
    const auto tokens = tokenize(input);
    return Parser(tokens).parse();
}

}  // namespace sql
