#include "Csv.hpp"

#include <stdexcept>

namespace csv {

bool readRecord(std::istream& in, std::vector<std::string>& fields, std::vector<bool>* quoted) {
    fields.clear();
    if (quoted) quoted->clear();
    if (in.peek() == std::char_traits<char>::eof()) return false;

    std::string field;
    bool inQuotes = false;
    bool wasQuoted = false;
    auto finishField = [&] {
        fields.push_back(std::move(field));
        if (quoted) quoted->push_back(wasQuoted);
        field.clear();
        wasQuoted = false;
    };

    char ch;
    while (in.get(ch)) {
        if (inQuotes) {
            if (ch == '"') {
                if (in.peek() == '"') {
                    in.get();
                    field.push_back('"');
                } else {
                    inQuotes = false;
                }
            } else {
                field.push_back(ch);
            }
            continue;
        }
        switch (ch) {
            case '"':
                if (!field.empty()) throw std::invalid_argument("CSV: quote inside unquoted field");
                inQuotes = true;
                wasQuoted = true;
                break;
            case ',':
                finishField();
                break;
            case '\r':
                if (in.peek() == '\n') in.get();
                finishField();
                return true;
            case '\n':
                finishField();
                return true;
            default:
                if (wasQuoted) throw std::invalid_argument("CSV: characters after closing quote");
                field.push_back(ch);
        }
    }
    if (inQuotes) throw std::invalid_argument("CSV: unterminated quoted field");
    finishField();
    return true;
}

void writeRecord(std::ostream& out, const std::vector<std::string>& fields) {
    for (size_t i = 0; i < fields.size(); ++i) {
        if (i) out.put(',');
        const std::string& f = fields[i];
        const bool needsQuotes = f.find_first_of(",\"\r\n") != std::string::npos ||
                                 (!f.empty() && (f.front() == ' ' || f.back() == ' '));
        if (!needsQuotes) {
            out << f;
            continue;
        }
        out.put('"');
        for (char ch : f) {
            if (ch == '"') out.put('"');
            out.put(ch);
        }
        out.put('"');
    }
    out.put('\n');
}

}  // namespace csv
