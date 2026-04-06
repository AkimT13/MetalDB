#include "Engine.hpp"
#include "GroupBy.hpp"
#include "Join.hpp"
#include <cassert>

std::string Engine::tablePath(const std::string& name) const {
    return name + ".mdb"; 
}

Table& Engine::createTable(const std::string& name, uint16_t numCols, uint16_t pageSize) {
    auto p = std::make_shared<Table>(tablePath(name), pageSize, numCols);
    tables_[name] = p;
    return *p;
}

Table& Engine::createTypedTable(const std::string& name,
                                const std::vector<ColType>& colTypes,
                                uint16_t pageSize) {
    auto p = std::make_shared<Table>(tablePath(name), pageSize, colTypes);
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

uint32_t Engine::insertTyped(const std::string& name, const std::vector<ColValue>& row) {
    return openTable(name).insertTypedRow(row);
}

std::vector<uint32_t> Engine::whereEq(const std::string& name, uint16_t col, ValueType v) {
    return openTable(name).scanEquals(col, v);
}

std::vector<uint32_t> Engine::whereEqString(const std::string& name, uint16_t col, const std::string& needle) {
    return openTable(name).scanEqualsString(col, needle);
}

std::vector<uint32_t> Engine::whereBetween(const std::string& name, uint16_t col, ValueType lo, ValueType hi) {
    return openTable(name).whereBetween(col, lo, hi);
}

ValueType Engine::sum(const std::string& name, uint16_t col) {
    return openTable(name).sumColumnHybrid(col);
}

ValueType Engine::minColumn(const std::string& name, uint16_t col) {
    return openTable(name).minColumn(col);
}

ValueType Engine::maxColumn(const std::string& name, uint16_t col) {
    return openTable(name).maxColumn(col);
}

std::unordered_map<ValueType, uint64_t>
Engine::groupCount(const std::string& name, uint16_t keyCol) {
    return GroupBy::countByKey(openTable(name), keyCol);
}

std::unordered_map<ValueType, uint64_t>
Engine::groupSum(const std::string& name, uint16_t keyCol, uint16_t valCol) {
    return GroupBy::sumByKey(openTable(name), keyCol, valCol);
}

std::unordered_map<ValueType, double>
Engine::groupAvg(const std::string& name, uint16_t keyCol, uint16_t valCol) {
    return GroupBy::avgByKey(openTable(name), keyCol, valCol);
}

std::unordered_map<ValueType, ValueType>
Engine::groupMin(const std::string& name, uint16_t keyCol, uint16_t valCol) {
    return GroupBy::minByKey(openTable(name), keyCol, valCol);
}

std::unordered_map<ValueType, ValueType>
Engine::groupMax(const std::string& name, uint16_t keyCol, uint16_t valCol) {
    return GroupBy::maxByKey(openTable(name), keyCol, valCol);
}

std::vector<std::pair<uint32_t,uint32_t>>
Engine::join(const std::string& left, uint16_t leftCol,
             const std::string& right, uint16_t rightCol) {
    return Join::hashJoinEq(openTable(left), leftCol, openTable(right), rightCol);
}