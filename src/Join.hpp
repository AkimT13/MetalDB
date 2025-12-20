#pragma once
#include <vector>
#include <utility>
#include <unordered_map>
#include "Table.hpp"

namespace Join {

// Return vector of (leftRowID, rightRowID) where left.col == right.col
std::vector<std::pair<uint32_t,uint32_t>>
hashJoinEq(Table& left, uint16_t leftCol, Table& right, uint16_t rightCol);

}