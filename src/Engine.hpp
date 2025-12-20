#pragma once
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>
#include "Table.hpp"

class Engine {
public:
    Engine() = default;

    Table& createTable(const std::string& name, uint16_t numCols, uint16_t pageSize = 4096);
    Table& openTable(const std::string& name);

    uint32_t insert(const std::string& name, const std::vector<ValueType>& row);
    std::vector<uint32_t> whereEq(const std::string& name, uint16_t col, ValueType v);
    std::vector<uint32_t> whereBetween(const std::string& name, uint16_t col, ValueType lo, ValueType hi);
    ValueType sum(const std::string& name, uint16_t col);

private:
    std::unordered_map<std::string, std::shared_ptr<Table>> tables_;
    std::string tablePath(const std::string& name) const; // simple local path scheme
};