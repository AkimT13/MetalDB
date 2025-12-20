// tests/test_persist_pages.cpp
#include "../Table.hpp"
#include "../ValueTypes.hpp"
#include <cassert>
#include <iostream>
#include <unistd.h>
#include <string>

int main() {
    char tmpl[] = "/tmp/table_persistXXXXXX";
    int fd = mkstemp(tmpl);
    assert(fd >= 0); close(fd);
    std::string idx = std::string(tmpl) + ".idx";

    {
        Table t(tmpl, /*pageSize=*/4096, /*numColumns=*/2);
        for (uint32_t i = 0; i < 5000; ++i) {
            t.insertRow({ static_cast<ValueType>(i % 97),
                          static_cast<ValueType>(i) });
        }

        auto r0 = t.fetchRow(0);
        assert(r0[0].has_value() && *r0[0] == 0);
        assert(r0[1].has_value() && *r0[1] == 0);

        auto r4999 = t.fetchRow(4999);
        assert(r4999[0].has_value() && *r4999[0] == (4999 % 97));
        assert(r4999[1].has_value() && *r4999[1] == 4999);
        // Table destructor will close and flush
    }

    {
        Table t(tmpl); // reopen from disk

        auto check = [&](uint32_t rowID){
            auto r = t.fetchRow(rowID);
            assert(r[0].has_value() && *r[0] == (rowID % 97));
            assert(r[1].has_value() && *r[1] == rowID);
        };
        check(0);
        check(1);
        check(123);
        check(4095);
        check(4096);
        check(4999);

        auto eq = t.scanEquals(0, /*val=*/42);
        for (auto rid : eq) {
            auto r = t.fetchRow(rid);
            assert(r[0].has_value() && *r[0] == 42);
        }
    }

    unlink(tmpl);
    unlink(idx.c_str());

    std::cout << "test_persist_pages: passed (page I/O persists)\n";
    return 0;
}