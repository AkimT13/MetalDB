#include "Engine.hpp"
#include <cassert>

std::string Engine::tablePath(const std::string& name) const {
    return name + ".mdb"; 
}

Table& Engine::createTable(const std::string& name, uint16_t numCols, uint16_t pageSize) {
    auto p = std::make_shared<Table>(tablePath(name), pageSize, numCols);
    tables_[name] = p;
    return *p;
}

Table& Engine::openTable(const std::string& name) {
    auto it = tables_.find(name);
    if (it != tables_.end()) return *(it->second);
    auto p = std::make_shared<Table>(tablePath(name));
    tables_[name] = p;
    return *p;
}

uint32_t Engine::insert(const std::string& name, const std::vector<ValueType>& row) {
    return openTable(name).insertRow(row);
}

std::vector<uint32_t> Engine::whereEq(const std::string& name, uint16_t col, ValueType v) {
    return openTable(name).scanEquals(col, v);
}

std::vector<uint32_t> Engine::whereBetween(const std::string& name, uint16_t col, ValueType lo, ValueType hi) {
    return openTable(name).whereBetween(col, lo, hi);
}

ValueType Engine::sum(const std::string& name, uint16_t col) {
    return openTable(name).sumColumnHybrid(col);
}