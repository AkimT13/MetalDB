#pragma once
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>
#include "Table.hpp"
#include "Predicate.hpp"

class Engine {
public:
    Engine() = default;

    Table& createTable(const std::string& name, uint16_t numCols, uint16_t pageSize = 4096);
    Table& createTypedTable(const std::string& name,
                            const std::vector<ColType>& colTypes,
                            uint16_t pageSize = 4096);
    Table& openTable(const std::string& name);

    uint32_t insert(const std::string& name, const std::vector<ValueType>& row);
    uint32_t insertTyped(const std::string& name, const std::vector<ColValue>& row);
    std::vector<uint32_t> whereEq(const std::string& name, uint16_t col, ValueType v);
    std::vector<uint32_t> whereEqString(const std::string& name, uint16_t col, const std::string& needle);
    std::vector<uint32_t> whereBetween(const std::string& name, uint16_t col, ValueType lo, ValueType hi);
    std::vector<uint32_t> whereAnd(const std::string& name, const std::vector<Predicate>& predicates);
    std::vector<uint32_t> whereOr(const std::string& name, const std::vector<Predicate>& predicates);
    ValueType sum(const std::string& name, uint16_t col);
    ValueType minColumn(const std::string& name, uint16_t col);
    ValueType maxColumn(const std::string& name, uint16_t col);

    // GroupBy aggregations
    std::unordered_map<ValueType, uint64_t>  groupCount(const std::string& name, uint16_t keyCol);
    std::unordered_map<ValueType, uint64_t>  groupSum  (const std::string& name, uint16_t keyCol, uint16_t valCol);
    std::unordered_map<ValueType, double>    groupAvg  (const std::string& name, uint16_t keyCol, uint16_t valCol);
    std::unordered_map<ValueType, ValueType> groupMin  (const std::string& name, uint16_t keyCol, uint16_t valCol);
    std::unordered_map<ValueType, ValueType> groupMax  (const std::string& name, uint16_t keyCol, uint16_t valCol);

    // Hash-equi join: returns (leftRowID, rightRowID) pairs
    std::vector<std::pair<uint32_t,uint32_t>>
    join(const std::string& left, uint16_t leftCol,
         const std::string& right, uint16_t rightCol);

private:
    std::unordered_map<std::string, std::shared_ptr<Table>> tables_;
    std::string tablePath(const std::string& name) const;
};
