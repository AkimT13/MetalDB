#include "WhereEval.hpp"

#include <algorithm>
#include <cerrno>
#include <cmath>
#include <cctype>
#include <cstdlib>
#include <limits>
#include <stdexcept>
#include <unordered_set>

#include "Predicate.hpp"
#include "Table.hpp"

extern "C" bool metalIsAvailable();

namespace sql {

namespace {

// ── literals ─────────────────────────────────────────────────────────────────

struct NumericLiteral {
    long double value = 0;
    bool isInteger = false;  // text had no fraction / exponent
    bool fitsInt64 = false;
    int64_t i64 = 0;
};

NumericLiteral parseNumeric(const Token& token) {
    NumericLiteral lit;
    const std::string& text = token.text;
    char* end = nullptr;
    errno = 0;
    lit.value = std::strtold(text.c_str(), &end);
    if (end != text.c_str() + text.size() || text.empty())
        throw std::invalid_argument("invalid numeric literal: " + text);
    lit.isInteger = text.find_first_of(".eE") == std::string::npos;
    if (lit.isInteger) {
        errno = 0;
        const long long v = std::strtoll(text.c_str(), &end, 10);
        lit.fitsInt64 = errno != ERANGE;
        lit.i64 = static_cast<int64_t>(v);
    }
    return lit;
}

// Three-way compare of a stored numeric value against a literal. INT64 against an
// integer literal is compared exactly (long double is only 53 bits on arm64).
int compareNumeric(const ColValue& v, const NumericLiteral& lit) {
    if (v.type == ColType::INT64 && lit.isInteger && lit.fitsInt64)
        return v.i64 < lit.i64 ? -1 : (v.i64 > lit.i64 ? 1 : 0);
    long double x = 0;
    switch (v.type) {
        case ColType::UINT32: x = v.u32; break;
        case ColType::INT64: x = static_cast<long double>(v.i64); break;
        case ColType::FLOAT: x = v.f32; break;
        case ColType::DOUBLE: x = v.f64; break;
        case ColType::STRING: break;
    }
    if (x < lit.value) return -1;
    if (x > lit.value) return 1;
    if (x == lit.value) return 0;
    return 2;  // NaN: unordered, matches nothing except !=
}

bool applyOp(WhereExpr::Op op, int cmp) {
    if (cmp == 2) return op == WhereExpr::Op::Ne;
    switch (op) {
        case WhereExpr::Op::Eq: return cmp == 0;
        case WhereExpr::Op::Ne: return cmp != 0;
        case WhereExpr::Op::Lt: return cmp < 0;
        case WhereExpr::Op::Le: return cmp <= 0;
        case WhereExpr::Op::Gt: return cmp > 0;
        case WhereExpr::Op::Ge: return cmp >= 0;
    }
    return false;
}

const char* opText(WhereExpr::Op op) {
    switch (op) {
        case WhereExpr::Op::Eq: return "=";
        case WhereExpr::Op::Ne: return "!=";
        case WhereExpr::Op::Lt: return "<";
        case WhereExpr::Op::Le: return "<=";
        case WhereExpr::Op::Gt: return ">";
        case WhereExpr::Op::Ge: return ">=";
    }
    return "?";
}

std::string literalText(const Token& t) {
    if (t.kind == TokenKind::Number) return t.text;
    std::string out = "'";
    for (char ch : t.text) {
        if (ch == '\'') out += "''";
        else out.push_back(ch);
    }
    return out + "'";
}

// ── row-ID set algebra (inputs sorted, outputs sorted) ─────────────────────────

void ensureSorted(std::vector<uint32_t>& ids) {
    if (!std::is_sorted(ids.begin(), ids.end())) std::sort(ids.begin(), ids.end());
}

std::vector<uint32_t> difference(const std::vector<uint32_t>& all, const std::vector<uint32_t>& minus) {
    std::vector<uint32_t> out;
    out.reserve(all.size() >= minus.size() ? all.size() - minus.size() : 0);
    std::set_difference(all.begin(), all.end(), minus.begin(), minus.end(), std::back_inserter(out));
    return out;
}

// Inclusive UINT32 range for a numeric comparison, or empty() when unsatisfiable.
struct U32Range {
    bool empty = false;
    uint32_t lo = 0;
    uint32_t hi = std::numeric_limits<uint32_t>::max();
};

U32Range clampRange(long double lo, long double hi) {
    constexpr long double kMax = std::numeric_limits<uint32_t>::max();
    U32Range r;
    lo = std::max<long double>(lo, 0);
    hi = std::min<long double>(hi, kMax);
    if (std::isnan(lo) || std::isnan(hi) || lo > hi) {
        r.empty = true;
        return r;
    }
    r.lo = static_cast<uint32_t>(lo);
    r.hi = static_cast<uint32_t>(hi);
    return r;
}

U32Range rangeFor(WhereExpr::Op op, long double v) {
    constexpr long double kInf = std::numeric_limits<long double>::infinity();
    switch (op) {
        case WhereExpr::Op::Eq:
            if (std::floor(v) != v) return U32Range{true};
            return clampRange(v, v);
        case WhereExpr::Op::Lt: return clampRange(-kInf, std::ceil(v) - 1);
        case WhereExpr::Op::Le: return clampRange(-kInf, std::floor(v));
        case WhereExpr::Op::Gt: return clampRange(std::floor(v) + 1, kInf);
        case WhereExpr::Op::Ge: return clampRange(std::ceil(v), kInf);
        case WhereExpr::Op::Ne: break;
    }
    return U32Range{true};
}

// ── evaluator ────────────────────────────────────────────────────────────────

class Evaluator {
public:
    Evaluator(Table& table, std::vector<std::string>* trace) : table_(table), trace_(trace) {}

    std::vector<uint32_t> eval(const WhereExpr& e) {
        switch (e.kind) {
            case WhereExpr::Kind::And: {
                std::vector<uint32_t> acc;
                bool first = true;
                for (const auto& child : e.children) {
                    auto ids = eval(*child);
                    acc = first ? std::move(ids) : Table::intersectRowIDs(acc, ids);
                    first = false;
                    if (acc.empty()) break;  // short-circuit
                }
                return acc;
            }
            case WhereExpr::Kind::Or: {
                std::vector<uint32_t> acc;
                for (const auto& child : e.children) acc = Table::unionRowIDs(acc, eval(*child));
                return acc;
            }
            case WhereExpr::Kind::Not: {
                const auto inner = eval(*e.children.front());
                note("NOT: complement against all live rows");
                return difference(allLive(), inner);
            }
            case WhereExpr::Kind::Compare:
            case WhereExpr::Kind::Between:
            case WhereExpr::Kind::In:
                return evalLeaf(e);
        }
        return {};
    }

private:
    std::vector<uint32_t> evalLeaf(const WhereExpr& e) {
        const ColType type = table_.columnFile(e.column.index).colType();
        std::vector<uint32_t> ids;
        if (type == ColType::UINT32) ids = evalU32(e);
        else if (type == ColType::STRING) ids = evalString(e);
        else ids = evalTypedNumeric(e);
        ensureSorted(ids);
        return ids;
    }

    // UINT32: lower =, <, <=, >, >=, BETWEEN onto the hybrid (GPU-capable) scans.
    std::vector<uint32_t> evalU32(const WhereExpr& e) {
        const std::string desc = whereToString(e);
        if (e.kind == WhereExpr::Kind::Compare && e.op == WhereExpr::Op::Ne) {
            WhereExpr eq = e;
            eq.op = WhereExpr::Op::Eq;
            const auto hits = evalU32(eq);
            note(desc + ": complement of equality scan");
            return difference(allLive(), hits);
        }
        if (e.kind == WhereExpr::Kind::In) {
            std::unordered_set<uint32_t> wanted;
            for (const auto& lit : e.literals) {
                const auto n = parseNumeric(lit);
                if (n.isInteger && n.value >= 0 && n.value <= std::numeric_limits<uint32_t>::max())
                    wanted.insert(static_cast<uint32_t>(n.value));
            }
            note(desc + ": CPU hash-set probe over materialized column");
            auto m = table_.materializeColumnWithRowIDs(e.column.index);
            std::vector<uint32_t> out;
            for (size_t i = 0; i < m.values.size(); ++i)
                if (wanted.count(m.values[i])) out.push_back(m.rowIDs[i]);
            return out;
        }

        U32Range range;
        if (e.kind == WhereExpr::Kind::Between) {
            const auto lo = parseNumeric(e.literals[0]);
            const auto hi = parseNumeric(e.literals[1]);
            range = clampRange(std::ceil(lo.value), std::floor(hi.value));
        } else {
            range = rangeFor(e.op, parseNumeric(e.literals[0]).value);
        }
        if (range.empty) {
            note(desc + ": unsatisfiable over UINT32, no scan");
            return {};
        }

        Predicate p;
        p.colIdx = e.column.index;
        if (range.lo == range.hi) {
            p.kind = Predicate::Kind::EQ;
            p.lo = p.hi = range.lo;
            note(desc + ": equality scan" + dispatchNote());
        } else {
            p.kind = Predicate::Kind::BETWEEN;
            p.lo = range.lo;
            p.hi = range.hi;
            note(desc + ": range scan [" + std::to_string(range.lo) + ", " + std::to_string(range.hi) +
                 "] with zone-map page pruning" + dispatchNote());
        }
        return table_.scanPredicate(p);
    }

    std::vector<uint32_t> evalString(const WhereExpr& e) {
        const std::string desc = whereToString(e);
        if (e.kind == WhereExpr::Kind::Compare && (e.op == WhereExpr::Op::Eq || e.op == WhereExpr::Op::Ne)) {
            note(desc + ": string equality scan" + dispatchNote());
            auto hits = table_.scanEqualsString(e.column.index, e.literals[0].text);
            if (e.op == WhereExpr::Op::Eq) return hits;
            ensureSorted(hits);
            return difference(allLive(), hits);
        }

        note(desc + ": CPU string scan");
        if (e.kind == WhereExpr::Kind::In) {
            std::unordered_set<std::string> wanted;
            for (const auto& lit : e.literals) wanted.insert(lit.text);
            return scanTyped(e.column.index, [&](const ColValue& v) { return wanted.count(v.str) > 0; });
        }
        if (e.kind == WhereExpr::Kind::Between) {
            const std::string& lo = e.literals[0].text;
            const std::string& hi = e.literals[1].text;
            return scanTyped(e.column.index, [&](const ColValue& v) { return lo <= v.str && v.str <= hi; });
        }
        const std::string& needle = e.literals[0].text;
        return scanTyped(e.column.index, [&](const ColValue& v) {
            const int c = v.str.compare(needle);
            return applyOp(e.op, c < 0 ? -1 : (c > 0 ? 1 : 0));
        });
    }

    std::vector<uint32_t> evalTypedNumeric(const WhereExpr& e) {
        note(whereToString(e) + ": CPU typed scan (" +
             colTypeName(table_.columnFile(e.column.index).colType()) + ")");
        if (e.kind == WhereExpr::Kind::In) {
            std::vector<NumericLiteral> lits;
            for (const auto& lit : e.literals) lits.push_back(parseNumeric(lit));
            return scanTyped(e.column.index, [&](const ColValue& v) {
                for (const auto& lit : lits)
                    if (compareNumeric(v, lit) == 0) return true;
                return false;
            });
        }
        if (e.kind == WhereExpr::Kind::Between) {
            const auto lo = parseNumeric(e.literals[0]);
            const auto hi = parseNumeric(e.literals[1]);
            return scanTyped(e.column.index, [&](const ColValue& v) {
                const int a = compareNumeric(v, lo);
                const int b = compareNumeric(v, hi);
                return a != 2 && b != 2 && a >= 0 && b <= 0;
            });
        }
        const auto lit = parseNumeric(e.literals[0]);
        return scanTyped(e.column.index, [&](const ColValue& v) { return applyOp(e.op, compareNumeric(v, lit)); });
    }

    template <typename Pred>
    std::vector<uint32_t> scanTyped(uint16_t col, Pred pred) {
        std::vector<uint32_t> out;
        const ColumnFile& cf = table_.columnFile(col);
        table_.rowIndexForEachLive([&](uint32_t rowID, const std::vector<uint32_t>& slots) {
            auto v = cf.fetchTypedSlot(slots[col]);
            if (v && pred(*v)) out.push_back(rowID);
        });
        return out;
    }

    const std::vector<uint32_t>& allLive() {
        if (!haveAllLive_) {
            allLive_ = allLiveRowIDs(table_);
            haveAllLive_ = true;
        }
        return allLive_;
    }

    std::string dispatchNote() const {
        const size_t n = table_.rowCount();
        if (!table_.useGPU()) return " (CPU: GPU disabled)";
        if (!metalIsAvailable()) return " (CPU: Metal unavailable)";
        if (n < table_.gpuThreshold())
            return " (CPU: " + std::to_string(n) + " rows < GPU threshold " +
                   std::to_string(table_.gpuThreshold()) + ")";
        return " (GPU: " + std::to_string(n) + " rows)";
    }

    void note(const std::string& line) {
        if (trace_) trace_->push_back(line);
    }

    Table& table_;
    std::vector<std::string>* trace_;
    std::vector<uint32_t> allLive_;
    bool haveAllLive_ = false;
};

}  // namespace

std::vector<uint32_t> allLiveRowIDs(Table& table) {
    std::vector<uint32_t> ids;
    ids.reserve(table.rowCount());
    table.rowIndexForEachLive([&](uint32_t rowID, const std::vector<uint32_t>&) { ids.push_back(rowID); });
    return ids;
}

ColValue coerceLiteral(const Token& token, ColType type, size_t colIdx) {
    const std::string where = " for column c" + std::to_string(colIdx);
    if (type == ColType::STRING) {
        if (token.kind != TokenKind::String && token.kind != TokenKind::Untyped)
            throw std::invalid_argument("expected string literal" + where);
        return ColValue(token.text);
    }
    if (token.kind != TokenKind::Number && token.kind != TokenKind::Untyped)
        throw std::invalid_argument("expected numeric literal" + where);
    if (token.text.empty() || !(std::isdigit(static_cast<unsigned char>(token.text[0])) || token.text[0] == '-' ||
                                token.text[0] == '.'))
        throw std::invalid_argument("invalid numeric value '" + token.text + "'" + where);

    const std::string& text = token.text;
    const bool isInteger = text.find_first_of(".eE") == std::string::npos;
    errno = 0;
    char* end = nullptr;
    switch (type) {
        case ColType::UINT32: {
            if (!isInteger || text[0] == '-')
                throw std::invalid_argument("expected non-negative integer" + where);
            const unsigned long long v = std::strtoull(text.c_str(), &end, 10);
            if (errno == ERANGE || v > 0xFFFFFFFFull)
                throw std::invalid_argument("value out of UINT32 range" + where);
            return ColValue(static_cast<uint32_t>(v));
        }
        case ColType::INT64: {
            if (!isInteger) throw std::invalid_argument("expected integer" + where);
            const long long v = std::strtoll(text.c_str(), &end, 10);
            if (errno == ERANGE) throw std::invalid_argument("value out of INT64 range" + where);
            return ColValue(static_cast<int64_t>(v));
        }
        case ColType::FLOAT: {
            const float v = std::strtof(text.c_str(), &end);
            if (end != text.c_str() + text.size() || errno == ERANGE)
                throw std::invalid_argument("invalid FLOAT literal" + where);
            return ColValue(v);
        }
        case ColType::DOUBLE: {
            const double v = std::strtod(text.c_str(), &end);
            if (end != text.c_str() + text.size() || errno == ERANGE)
                throw std::invalid_argument("invalid DOUBLE literal" + where);
            return ColValue(v);
        }
        case ColType::STRING:
            break;
    }
    throw std::invalid_argument("unsupported column type" + where);
}

void validateWhere(const Table& table, const WhereExpr& expr) {
    if (expr.kind == WhereExpr::Kind::And || expr.kind == WhereExpr::Kind::Or ||
        expr.kind == WhereExpr::Kind::Not) {
        for (const auto& child : expr.children) validateWhere(table, *child);
        return;
    }
    if (expr.column.index >= table.numColumns())
        throw std::invalid_argument("column " + expr.column.text + " out of bounds");
    const bool stringCol = table.columnFile(expr.column.index).colType() == ColType::STRING;
    for (const auto& lit : expr.literals) {
        if (lit.kind == TokenKind::Untyped) {  // bound parameter: takes the column's type
            if (!stringCol) (void)parseNumeric(lit);
            continue;
        }
        if (stringCol && lit.kind != TokenKind::String)
            throw std::invalid_argument("column " + expr.column.text + " is STRING; compare it with a string literal");
        if (!stringCol && lit.kind != TokenKind::Number)
            throw std::invalid_argument("column " + expr.column.text + " is numeric; compare it with a numeric literal");
        if (!stringCol) (void)parseNumeric(lit);  // rejects malformed numbers early
    }
}

std::vector<uint32_t> evaluateWhere(Table& table, const WhereExpr& expr, std::vector<std::string>* trace) {
    validateWhere(table, expr);
    return Evaluator(table, trace).eval(expr);
}

std::string whereToString(const WhereExpr& e) {
    switch (e.kind) {
        case WhereExpr::Kind::Compare:
            return e.column.text + " " + opText(e.op) + " " + literalText(e.literals[0]);
        case WhereExpr::Kind::Between:
            return e.column.text + " BETWEEN " + literalText(e.literals[0]) + " AND " + literalText(e.literals[1]);
        case WhereExpr::Kind::In: {
            std::string out = e.column.text + " IN (";
            for (size_t i = 0; i < e.literals.size(); ++i) {
                if (i) out += ", ";
                out += literalText(e.literals[i]);
            }
            return out + ")";
        }
        case WhereExpr::Kind::Not:
            return "NOT (" + whereToString(*e.children.front()) + ")";
        case WhereExpr::Kind::And:
        case WhereExpr::Kind::Or: {
            const char* sep = e.kind == WhereExpr::Kind::And ? " AND " : " OR ";
            std::string out;
            for (size_t i = 0; i < e.children.size(); ++i) {
                if (i) out += sep;
                out += "(" + whereToString(*e.children[i]) + ")";
            }
            return out;
        }
    }
    return "";
}

}  // namespace sql
