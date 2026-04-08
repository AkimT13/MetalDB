#include "MiniSQL.hpp"

#include <algorithm>
#include <cctype>
#include <cstdint>
#include <sstream>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <unistd.h>
#include <utility>
#include <vector>

#include "Engine.hpp"
#include "Predicate.hpp"
#include "Table.hpp"
#include "ValueTypes.hpp"

namespace {

enum class TokenKind {
    Identifier,
    Number,
    String,
    Comma,
    Star,
    LParen,
    RParen,
    Eq,
    Semicolon,
    End,
};

struct Token {
    TokenKind kind = TokenKind::End;
    std::string text;
};

struct ColumnRef {
    uint16_t index = 0;
    std::string text;
};

struct SelectItem {
    enum class Kind {
        Column,
        Star,
        CountStar,
        Sum,
        Min,
        Max,
        Avg,
    };

    Kind kind = Kind::Column;
    ColumnRef column;
};

struct ParsedWhere {
    enum class Connective {
        And,
        Or,
    };

    std::vector<Predicate> predicates;
    Connective connective = Connective::And;
    bool hasConnective = false;
};

struct ParsedQuery {
    std::string tableName;
    std::vector<SelectItem> selectItems;
    bool hasWhere = false;
    ParsedWhere where;
    bool hasGroupBy = false;
    ColumnRef groupBy;
};

struct AggregateState {
    uint64_t count = 0;
    long double sum = 0.0;
    ColValue min;
    ColValue max;
    bool hasValue = false;
};

class Tokenizer {
public:
    explicit Tokenizer(const std::string& input) : input_(input) {}

    std::vector<Token> tokenize() {
        std::vector<Token> tokens;
        while (true) {
            skipWhitespace();
            if (pos_ >= input_.size()) {
                tokens.push_back({TokenKind::End, ""});
                return tokens;
            }

            const char ch = input_[pos_];
            if (std::isalpha(static_cast<unsigned char>(ch)) || ch == '_') {
                tokens.push_back(readIdentifier());
                continue;
            }
            if (std::isdigit(static_cast<unsigned char>(ch))) {
                tokens.push_back(readNumber());
                continue;
            }
            if (ch == '\'') {
                tokens.push_back(readString());
                continue;
            }

            switch (ch) {
                case ',':
                    ++pos_;
                    tokens.push_back({TokenKind::Comma, ","});
                    break;
                case '*':
                    ++pos_;
                    tokens.push_back({TokenKind::Star, "*"});
                    break;
                case '(':
                    ++pos_;
                    tokens.push_back({TokenKind::LParen, "("});
                    break;
                case ')':
                    ++pos_;
                    tokens.push_back({TokenKind::RParen, ")"});
                    break;
                case '=':
                    ++pos_;
                    tokens.push_back({TokenKind::Eq, "="});
                    break;
                case ';':
                    ++pos_;
                    tokens.push_back({TokenKind::Semicolon, ";"});
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

    Token readNumber() {
        const size_t start = pos_;
        while (pos_ < input_.size() && std::isdigit(static_cast<unsigned char>(input_[pos_]))) ++pos_;
        return {TokenKind::Number, input_.substr(start, pos_ - start)};
    }

    Token readString() {
        ++pos_;
        std::string value;
        while (pos_ < input_.size()) {
            const char ch = input_[pos_++];
            if (ch == '\'') return {TokenKind::String, value};
            value.push_back(ch);
        }
        throw std::invalid_argument("unterminated string literal");
    }

    void skipWhitespace() {
        while (pos_ < input_.size() &&
               std::isspace(static_cast<unsigned char>(input_[pos_]))) ++pos_;
    }

    const std::string& input_;
    size_t pos_ = 0;
};

class Parser {
public:
    explicit Parser(const std::vector<Token>& tokens) : tokens_(tokens) {}

    ParsedQuery parse() {
        ParsedQuery query;
        expectKeyword("SELECT");
        query.selectItems = parseSelectList();
        expectKeyword("FROM");
        query.tableName = expect(TokenKind::String, "table path string literal").text;

        if (matchKeyword("WHERE")) {
            query.hasWhere = true;
            query.where = parseWhereClause();
        }

        if (matchKeyword("GROUP")) {
            expectKeyword("BY");
            query.hasGroupBy = true;
            query.groupBy = parseColumnRef(expect(TokenKind::Identifier, "group-by column"));
        }

        if (peek().kind == TokenKind::Semicolon) ++pos_;
        expect(TokenKind::End, "end of query");
        return query;
    }

private:
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
        if (equalsIgnoreCase(token.text, "COUNT")) return parseAggregate(SelectItem::Kind::CountStar, true);
        if (equalsIgnoreCase(token.text, "SUM")) return parseAggregate(SelectItem::Kind::Sum, false);
        if (equalsIgnoreCase(token.text, "MIN")) return parseAggregate(SelectItem::Kind::Min, false);
        if (equalsIgnoreCase(token.text, "MAX")) return parseAggregate(SelectItem::Kind::Max, false);
        if (equalsIgnoreCase(token.text, "AVG")) return parseAggregate(SelectItem::Kind::Avg, false);

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
            return item;
        }
        item.column = parseColumnRef(expect(TokenKind::Identifier, "aggregate column"));
        expect(TokenKind::RParen, ")");
        return item;
    }

    ParsedWhere parseWhereClause() {
        ParsedWhere where;
        where.predicates.push_back(parsePredicate());
        while (true) {
            if (matchKeyword("AND")) {
                if (where.hasConnective && where.connective != ParsedWhere::Connective::And)
                    throw std::invalid_argument("mixed AND/OR WHERE clauses are not supported");
                where.hasConnective = true;
                where.connective = ParsedWhere::Connective::And;
                where.predicates.push_back(parsePredicate());
                continue;
            }
            if (matchKeyword("OR")) {
                if (where.hasConnective && where.connective != ParsedWhere::Connective::Or)
                    throw std::invalid_argument("mixed AND/OR WHERE clauses are not supported");
                where.hasConnective = true;
                where.connective = ParsedWhere::Connective::Or;
                where.predicates.push_back(parsePredicate());
                continue;
            }
            return where;
        }
    }

    Predicate parsePredicate() {
        Predicate predicate;
        predicate.colIdx = parseColumnRef(expect(TokenKind::Identifier, "predicate column")).index;
        if (match(TokenKind::Eq)) {
            if (peek().kind == TokenKind::String) {
                predicate.kind = Predicate::Kind::EQ_STRING;
                predicate.needle = expect(TokenKind::String, "string literal").text;
                return predicate;
            }
            predicate.kind = Predicate::Kind::EQ;
            predicate.lo = parseNumber(expect(TokenKind::Number, "numeric literal"));
            predicate.hi = predicate.lo;
            return predicate;
        }
        expectKeyword("BETWEEN");
        predicate.kind = Predicate::Kind::BETWEEN;
        predicate.lo = parseNumber(expect(TokenKind::Number, "numeric lower bound"));
        expectKeyword("AND");
        predicate.hi = parseNumber(expect(TokenKind::Number, "numeric upper bound"));
        return predicate;
    }

    ColumnRef parseColumnRef(const Token& token) {
        if (token.kind != TokenKind::Identifier || token.text.size() < 2 ||
            (token.text[0] != 'c' && token.text[0] != 'C')) {
            throw std::invalid_argument("expected column reference like c0");
        }
        const std::string digits = token.text.substr(1);
        if (digits.empty() ||
            !std::all_of(digits.begin(), digits.end(), [](char ch) { return std::isdigit(static_cast<unsigned char>(ch)); })) {
            throw std::invalid_argument("expected column reference like c0");
        }
        const unsigned long index = std::stoul(digits);
        if (index > 0xFFFFul) throw std::invalid_argument("column index out of range");
        return {static_cast<uint16_t>(index), "c" + digits};
    }

    ValueType parseNumber(const Token& token) {
        const unsigned long long value = std::stoull(token.text);
        if (value > 0xFFFFFFFFull) throw std::invalid_argument("numeric literal out of UINT32 range");
        return static_cast<ValueType>(value);
    }

    bool match(TokenKind kind) {
        if (peek().kind != kind) return false;
        ++pos_;
        return true;
    }

    bool matchKeyword(const char* keyword) {
        if (peek().kind != TokenKind::Identifier) return false;
        if (!equalsIgnoreCase(peek().text, keyword)) return false;
        ++pos_;
        return true;
    }

    Token expect(TokenKind kind, const char* what) {
        if (peek().kind != kind)
            throw std::invalid_argument(std::string("expected ") + what);
        return tokens_[pos_++];
    }

    void expectKeyword(const char* keyword) {
        if (!matchKeyword(keyword))
            throw std::invalid_argument(std::string("expected keyword ") + keyword);
    }

    const Token& peek() const { return tokens_[pos_]; }

    static bool equalsIgnoreCase(const std::string& lhs, const char* rhs) {
        size_t i = 0;
        for (; i < lhs.size() && rhs[i]; ++i) {
            if (std::toupper(static_cast<unsigned char>(lhs[i])) !=
                std::toupper(static_cast<unsigned char>(rhs[i]))) {
                return false;
            }
        }
        return i == lhs.size() && rhs[i] == '\0';
    }

    const std::vector<Token>& tokens_;
    size_t pos_ = 0;
};

std::string formatColValue(const ColValue& value) {
    std::ostringstream out;
    switch (value.type) {
        case ColType::UINT32: return std::to_string(value.u32);
        case ColType::INT64: return std::to_string(value.i64);
        case ColType::FLOAT:
            out << value.f32;
            return out.str();
        case ColType::DOUBLE:
            out << value.f64;
            return out.str();
        case ColType::STRING:
            return value.str;
    }
    return "";
}

void validateColumnRef(const Table& table, uint16_t colIdx) {
    if (colIdx >= table.numColumns())
        throw std::invalid_argument("column index out of bounds");
}

void validateQueryShape(const Table& table, const ParsedQuery& query) {
    bool hasStar = false;
    bool hasColumn = false;
    int aggregateCount = 0;

    for (const auto& item : query.selectItems) {
        switch (item.kind) {
            case SelectItem::Kind::Star:
                hasStar = true;
                break;
            case SelectItem::Kind::Column:
                hasColumn = true;
                validateColumnRef(table, item.column.index);
                break;
            case SelectItem::Kind::CountStar:
                ++aggregateCount;
                break;
            case SelectItem::Kind::Sum:
            case SelectItem::Kind::Min:
            case SelectItem::Kind::Max:
            case SelectItem::Kind::Avg:
                ++aggregateCount;
                validateColumnRef(table, item.column.index);
                break;
        }
    }

    if (hasStar && query.selectItems.size() != 1)
        throw std::invalid_argument("SELECT * cannot be combined with other projections");
    if (hasStar && query.hasGroupBy)
        throw std::invalid_argument("SELECT * with GROUP BY is not supported");

    if (query.hasWhere) {
        for (const auto& predicate : query.where.predicates)
            validateColumnRef(table, predicate.colIdx);
    }

    if (query.hasGroupBy) {
        validateColumnRef(table, query.groupBy.index);
        if (query.hasWhere)
            throw std::invalid_argument("GROUP BY with WHERE is not supported in mini-SQL v1");
        if (aggregateCount != 1)
            throw std::invalid_argument("GROUP BY queries require exactly one aggregate expression");
        if (query.selectItems.size() != 2 ||
            query.selectItems[0].kind != SelectItem::Kind::Column ||
            query.selectItems[0].column.index != query.groupBy.index) {
            throw std::invalid_argument("GROUP BY queries must select the group key first");
        }
        const auto& agg = query.selectItems[1];
        if (agg.kind != SelectItem::Kind::CountStar) {
            if (table.columnFile(query.groupBy.index).colType() != ColType::UINT32 ||
                table.columnFile(agg.column.index).colType() != ColType::UINT32) {
                throw std::invalid_argument("GROUP BY v1 supports UINT32 key/value columns only");
            }
        } else if (table.columnFile(query.groupBy.index).colType() != ColType::UINT32) {
            throw std::invalid_argument("GROUP BY v1 supports UINT32 key columns only");
        }
        return;
    }

    if (aggregateCount > 0) {
        if (hasColumn || hasStar)
            throw std::invalid_argument("aggregate queries cannot mix aggregates with plain columns");
        if (aggregateCount != 1)
            throw std::invalid_argument("non-grouped aggregate queries support exactly one aggregate");
    }
}

std::vector<uint32_t> collectAllLiveRowIDs(Table& table) {
    std::vector<uint32_t> rowIDs;
    table.rowIndexForEachLive([&](uint32_t rowID, const std::vector<uint32_t>&) {
        rowIDs.push_back(rowID);
    });
    return rowIDs;
}

std::vector<uint32_t> executeWhere(Table& table, const ParsedQuery& query) {
    if (!query.hasWhere) return collectAllLiveRowIDs(table);
    if (query.where.predicates.size() == 1) return table.scanPredicate(query.where.predicates.front());
    if (query.where.connective == ParsedWhere::Connective::And)
        return table.whereAnd(query.where.predicates);
    return table.whereOr(query.where.predicates);
}

MiniSQLResult executeProjectionQuery(Table& table, const ParsedQuery& query) {
    std::vector<uint16_t> cols;
    MiniSQLResult result;

    if (query.selectItems.size() == 1 && query.selectItems[0].kind == SelectItem::Kind::Star) {
        cols.reserve(table.numColumns());
        result.headers.reserve(table.numColumns());
        for (uint16_t c = 0; c < table.numColumns(); ++c) {
            cols.push_back(c);
            result.headers.push_back("c" + std::to_string(c));
        }
    } else {
        cols.reserve(query.selectItems.size());
        result.headers.reserve(query.selectItems.size());
        for (const auto& item : query.selectItems) {
            cols.push_back(item.column.index);
            result.headers.push_back(item.column.text);
        }
    }

    const auto rowIDs = executeWhere(table, query);
    result.rows.reserve(rowIDs.size());
    for (uint32_t rowID : rowIDs) {
        auto row = table.fetchTypedRow(rowID);
        std::vector<std::string> outRow;
        outRow.reserve(cols.size());
        bool ok = true;
        for (uint16_t c : cols) {
            if (!row[c]) {
                ok = false;
                break;
            }
            outRow.push_back(formatColValue(*row[c]));
        }
        if (ok) result.rows.push_back(std::move(outRow));
    }
    return result;
}

AggregateState computeAggregate(Table& table, const std::vector<uint32_t>& rowIDs, const SelectItem& item) {
    AggregateState state;
    for (uint32_t rowID : rowIDs) {
        auto row = table.fetchTypedRow(rowID);
        if (item.kind == SelectItem::Kind::CountStar) {
            ++state.count;
            continue;
        }
        const auto& cell = row[item.column.index];
        if (!cell) continue;
        if (cell->type == ColType::STRING)
            throw std::invalid_argument("aggregate functions require numeric columns");
        if (!state.hasValue) {
            state.min = *cell;
            state.max = *cell;
            state.hasValue = true;
        } else {
            if (*cell < state.min) state.min = *cell;
            if (*cell > state.max) state.max = *cell;
        }
        ++state.count;
        state.sum += cell->toDouble();
    }
    return state;
}

MiniSQLResult executeScalarAggregateQuery(Table& table, const ParsedQuery& query) {
    const auto& item = query.selectItems.front();
    const auto rowIDs = executeWhere(table, query);
    const auto state = computeAggregate(table, rowIDs, item);

    MiniSQLResult result;
    switch (item.kind) {
        case SelectItem::Kind::CountStar:
            result.headers.push_back("count(*)");
            result.rows.push_back({std::to_string(state.count)});
            return result;
        case SelectItem::Kind::Sum:
            result.headers.push_back("sum(" + item.column.text + ")");
            result.rows.push_back({state.count == 0 ? "0" : formatColValue(ColValue(static_cast<double>(state.sum)))});
            return result;
        case SelectItem::Kind::Min:
            result.headers.push_back("min(" + item.column.text + ")");
            result.rows.push_back({state.hasValue ? formatColValue(state.min) : ""});
            return result;
        case SelectItem::Kind::Max:
            result.headers.push_back("max(" + item.column.text + ")");
            result.rows.push_back({state.hasValue ? formatColValue(state.max) : ""});
            return result;
        case SelectItem::Kind::Avg: {
            result.headers.push_back("avg(" + item.column.text + ")");
            std::ostringstream out;
            out << (state.count == 0 ? 0.0L : state.sum / static_cast<long double>(state.count));
            result.rows.push_back({out.str()});
            return result;
        }
        default:
            throw std::invalid_argument("not an aggregate query");
    }
}

MiniSQLResult executeGroupByQuery(Engine& engine, const ParsedQuery& query) {
    MiniSQLResult result;
    result.headers.push_back(query.selectItems[0].column.text);

    const auto& agg = query.selectItems[1];
    switch (agg.kind) {
        case SelectItem::Kind::CountStar: {
            result.headers.push_back("count(*)");
            auto groups = engine.groupCount(query.tableName, query.groupBy.index);
            std::vector<std::pair<ValueType, uint64_t>> rows(groups.begin(), groups.end());
            std::sort(rows.begin(), rows.end(), [](const auto& lhs, const auto& rhs) { return lhs.first < rhs.first; });
            for (const auto& [key, value] : rows)
                result.rows.push_back({std::to_string(key), std::to_string(value)});
            return result;
        }
        case SelectItem::Kind::Sum: {
            result.headers.push_back("sum(" + agg.column.text + ")");
            auto groups = engine.groupSum(query.tableName, query.groupBy.index, agg.column.index);
            std::vector<std::pair<ValueType, uint64_t>> rows(groups.begin(), groups.end());
            std::sort(rows.begin(), rows.end(), [](const auto& lhs, const auto& rhs) { return lhs.first < rhs.first; });
            for (const auto& [key, value] : rows)
                result.rows.push_back({std::to_string(key), std::to_string(value)});
            return result;
        }
        case SelectItem::Kind::Min: {
            result.headers.push_back("min(" + agg.column.text + ")");
            auto groups = engine.groupMin(query.tableName, query.groupBy.index, agg.column.index);
            std::vector<std::pair<ValueType, ValueType>> rows(groups.begin(), groups.end());
            std::sort(rows.begin(), rows.end(), [](const auto& lhs, const auto& rhs) { return lhs.first < rhs.first; });
            for (const auto& [key, value] : rows)
                result.rows.push_back({std::to_string(key), std::to_string(value)});
            return result;
        }
        case SelectItem::Kind::Max: {
            result.headers.push_back("max(" + agg.column.text + ")");
            auto groups = engine.groupMax(query.tableName, query.groupBy.index, agg.column.index);
            std::vector<std::pair<ValueType, ValueType>> rows(groups.begin(), groups.end());
            std::sort(rows.begin(), rows.end(), [](const auto& lhs, const auto& rhs) { return lhs.first < rhs.first; });
            for (const auto& [key, value] : rows)
                result.rows.push_back({std::to_string(key), std::to_string(value)});
            return result;
        }
        case SelectItem::Kind::Avg: {
            result.headers.push_back("avg(" + agg.column.text + ")");
            auto groups = engine.groupAvg(query.tableName, query.groupBy.index, agg.column.index);
            std::vector<std::pair<ValueType, double>> rows(groups.begin(), groups.end());
            std::sort(rows.begin(), rows.end(), [](const auto& lhs, const auto& rhs) { return lhs.first < rhs.first; });
            for (const auto& [key, value] : rows) {
                std::ostringstream out;
                out << value;
                result.rows.push_back({std::to_string(key), out.str()});
            }
            return result;
        }
        default:
            throw std::invalid_argument("unsupported GROUP BY aggregate");
    }
}

} // namespace

MiniSQLResult executeMiniSQL(Engine& engine, const std::string& sql) {
    const auto tokens = Tokenizer(sql).tokenize();
    const ParsedQuery query = Parser(tokens).parse();
    const std::string tablePath = query.tableName + ".mdb";
    if (access(tablePath.c_str(), F_OK) != 0)
        throw std::invalid_argument("table does not exist");
    Table& table = engine.openTable(query.tableName);
    validateQueryShape(table, query);

    if (query.hasGroupBy) return executeGroupByQuery(engine, query);

    bool aggregateQuery = false;
    for (const auto& item : query.selectItems) {
        if (item.kind != SelectItem::Kind::Column && item.kind != SelectItem::Kind::Star) {
            aggregateQuery = true;
            break;
        }
    }
    if (aggregateQuery) return executeScalarAggregateQuery(table, query);
    return executeProjectionQuery(table, query);
}
