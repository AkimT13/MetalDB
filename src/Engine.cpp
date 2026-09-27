#include "Engine.hpp"
#include "GroupBy.hpp"
#include "Join.hpp"
#include <cassert>
#include <stdexcept>
#include <unistd.h>

std::string Engine::tablePath(const std::string& name) const {
    return resolveTableBase(name) + ".mdb";
}

namespace {
void rejectEscapes(const std::string& path, const char* what) {
    if (path.empty()) throw std::invalid_argument(std::string(what) + " must not be empty");
    if (path[0] == '/') throw std::invalid_argument(std::string(what) + " must be relative to the data directory");
    size_t start = 0;
    while (start <= path.size()) {
        size_t end = path.find('/', start);
        if (end == std::string::npos) end = path.size();
        if (path.compare(start, end - start, "..") == 0)
            throw std::invalid_argument(std::string(what) + " must not contain '..'");
        start = end + 1;
    }
}
}  // namespace

void Engine::setDataDir(const std::string& dir) {
    dataDir_ = dir;
    while (dataDir_.size() > 1 && dataDir_.back() == '/') dataDir_.pop_back();
}

std::string Engine::resolveTableBase(const std::string& name) const {
    if (dataDir_.empty()) return name;
    rejectEscapes(name, "table name");
    return dataDir_ + "/" + name;
}

std::string Engine::resolveFile(const std::string& path) const {
    if (dataDir_.empty()) return path;
    rejectEscapes(path, "file path");
    return dataDir_ + "/" + path;
}

bool Engine::tableExists(const std::string& name) const {
    return access(tablePath(name).c_str(), F_OK) == 0;
}

std::mutex& Engine::tableMutex(const std::string& name) {
    std::lock_guard<std::mutex> g(registryMutex_);
    auto& slot = tableLocks_[name];
    if (!slot) slot = std::make_unique<std::mutex>();
    return *slot;
}

void Engine::flushAll() {
    std::vector<std::pair<std::string, std::shared_ptr<Table>>> open;
    {
        std::lock_guard<std::mutex> g(registryMutex_);
        open.assign(tables_.begin(), tables_.end());
    }
    for (auto& [name, table] : open) {
        std::lock_guard<std::mutex> lk(tableMutex(name));
        table->flushDurable();
    }
}

void Engine::setSyncCommit(bool on) {
    std::lock_guard<std::mutex> g(registryMutex_);
    syncCommit_ = on;
    for (auto& [name, table] : tables_) table->setSyncCommit(on);
}

Table& Engine::createTable(const std::string& name, uint16_t numCols, uint16_t pageSize) {
    auto p = std::make_shared<Table>(tablePath(name), pageSize, numCols);
    std::lock_guard<std::mutex> g(registryMutex_);
    p->setSyncCommit(syncCommit_);
    tables_[name] = p;
    return *p;
}

Table& Engine::createTypedTable(const std::string& name,
                                const std::vector<ColType>& colTypes,
                                uint16_t pageSize) {
    auto p = std::make_shared<Table>(tablePath(name), pageSize, colTypes);
    std::lock_guard<std::mutex> g(registryMutex_);
    p->setSyncCommit(syncCommit_);
    tables_[name] = p;
    return *p;
}

Table& Engine::openTable(const std::string& name) {
    std::lock_guard<std::mutex> g(registryMutex_);
    auto it = tables_.find(name);
    if (it != tables_.end()) return *(it->second);
    auto p = std::make_shared<Table>(tablePath(name));
    p->setSyncCommit(syncCommit_);
    tables_[name] = p;
    return *p;
}

void Engine::flush(const std::string& name) {
    openTable(name).flushDurable();
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

std::vector<uint32_t> Engine::whereAnd(const std::string& name, const std::vector<Predicate>& predicates) {
    return openTable(name).whereAnd(predicates);
}

std::vector<uint32_t> Engine::whereOr(const std::string& name, const std::vector<Predicate>& predicates) {
    return openTable(name).whereOr(predicates);
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
    Table& t = openTable(name);
    return GroupBy::countByKey(t, keyCol, t.useGPU(), t.gpuThreshold());
}

std::unordered_map<ValueType, uint64_t>
Engine::groupSum(const std::string& name, uint16_t keyCol, uint16_t valCol) {
    Table& t = openTable(name);
    return GroupBy::sumByKey(t, keyCol, valCol, t.useGPU(), t.gpuThreshold());
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
