#pragma once
// Synthesized PostgreSQL catalog for `mdb pgserve`: lets psql meta-commands
// (\dt, \d, \d table, \l, \dn), driver probes (version(), current_database(), SHOW),
// and information_schema.tables / .columns work against MetalDB tables.
//
// MetalDB has no system catalog of its own, so catalog queries are recognized by
// the relations they read and answered from the table files under the server's
// data directory. Each select-list expression is mapped to a value by what it
// references (c.relname, format_type(...), aliases like "Owner"), which keeps the
// answers aligned with whatever column list the client version asks for. A catalog
// query that is not specifically understood gets a correctly shaped empty result.

#include <string>

#include "MiniSQL.hpp"

class Engine;

namespace pgcat {

// Cell value meaning SQL NULL in a catalog result (the wire layer sends NULL).
extern const char* const kNull;

// Returns true (filling `out`) if `sql` is a catalog / introspection query.
bool answer(const std::string& sql, Engine& engine, const std::string& user, MiniSQLResult& out);

}  // namespace pgcat
