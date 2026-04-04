#pragma once
#include <unordered_map>
#include <vector>
#include <cstdint>
#include "Table.hpp"

namespace GroupBy {

// COUNT(*) GROUP BY keyCol
std::unordered_map<ValueType, uint64_t>
countByKey(Table& t, uint16_t keyCol);

// SUM(valCol) GROUP BY keyCol
std::unordered_map<ValueType, uint64_t>
sumByKey(Table& t, uint16_t keyCol, uint16_t valCol);

// AVG(valCol) GROUP BY keyCol
std::unordered_map<ValueType, double>
avgByKey(Table& t, uint16_t keyCol, uint16_t valCol);

// MIN(valCol) GROUP BY keyCol
std::unordered_map<ValueType, ValueType>
minByKey(Table& t, uint16_t keyCol, uint16_t valCol);

// MAX(valCol) GROUP BY keyCol
std::unordered_map<ValueType, ValueType>
maxByKey(Table& t, uint16_t keyCol, uint16_t valCol);

}