#pragma once

#include <cstdint>
#include <string>

#include "ValueTypes.hpp"

struct Predicate {
    enum class Kind {
        EQ,
        BETWEEN,
        EQ_STRING
    };

    uint16_t    colIdx = 0;
    Kind        kind = Kind::EQ;
    ValueType   lo = 0;
    ValueType   hi = 0;
    std::string needle;
};
