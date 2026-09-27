#pragma once
// Minimal RFC 4180 CSV reader / writer used by COPY.

#include <istream>
#include <ostream>
#include <string>
#include <vector>

namespace csv {

// Reads one record (which may span lines when a quoted field contains newlines).
// Returns false at end of input. Accepts LF and CRLF line endings. Throws
// std::invalid_argument on an unterminated quoted field.
// `quoted[i]` reports whether field i was quoted (so "" can be told apart from an
// empty unquoted field if a caller cares).
bool readRecord(std::istream& in, std::vector<std::string>& fields, std::vector<bool>* quoted = nullptr);

// Writes one record terminated by '\n', quoting fields that contain a comma,
// quote, CR, or LF, or that have leading/trailing spaces.
void writeRecord(std::ostream& out, const std::vector<std::string>& fields);

}  // namespace csv
