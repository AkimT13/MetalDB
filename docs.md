# MetalDB Prototype Documentation

This document provides detailed documentation for the current components in the MetalDB prototype, covering file layouts, classes, methods, and test harnesses. It will be updated as the project evolves.

---

## 1. ValueTypes.hpp

**Location:** `include/ValueTypes.hpp`

**Purpose:**

* Defines the canonical payload type for all column data.

**Contents:**

```cpp
#pragma once

#include <cstdint>

// The low-level storage type for column values
using Number    = uint32_t;
using ValueType = Number;

// Compile-time constant for the size of each value
static constexpr size_t VALUE_SIZE = sizeof(ValueType);
```

**Notes:**

* Change `Number` to another fixed-width type (e.g. `double`) here to affect the entire engine.
* `VALUE_SIZE` is `constexpr`, so it generates no duplicate symbols.

---

## 2. MasterPage

### Files

* `src/MasterPage.hpp`
* `src/MasterPage.cpp`
* `tests/test_masterpage.cpp`

### Role

* Manages **page 0** (the master page) of the database file.
* Stores global metadata:

  * **magic**: 4-byte identifier (`0x4D445042`).
  * **pageSize**: size in bytes of each page.
  * **numColumns**: number of column streams in the file.
  * **headPageIDs**: in-memory `std::vector<uint16_t>` of length `numColumns`, each entry pointing to the head of the free-page list for that column.

### API

```cpp
struct MasterPage {
  uint32_t magic;
  uint16_t pageSize;
  uint16_t numColumns;
  std::vector<uint16_t> headPageIDs;

  // Initialize a new master page in an empty/truncated file
  static MasterPage initnew(int fd, uint16_t pageSize, int numColumns);

  // Load an existing master page from disk
  static MasterPage load(int fd);

  // Write the in-memory master page back to disk (page 0)
  void flush(int fd) const;
};
```

#### `initnew(int fd, uint16_t pageSize, int numColumns)`

* Truncates file to `pageSize` bytes, sets fields, and writes page 0.
* Initializes each `headPageIDs[i] = UINT16_MAX`.
* Calls `fsync` to persist.

#### `load(int fd)`

* Seeks to offset 0, reads `magic`, `pageSize`, `numColumns`, then resizes and reads `headPageIDs`.

#### `flush(int fd) const`

* Seeks to offset 0 and writes all fields and the `headPageIDs` array, then `fsync`.

### Test: `test_masterpage.cpp`

* Uses `mkstemp` to create a temp file.
* Calls `initnew`, asserts correct initial values.
* Calls `load` to verify persistence.
* Mutates one `headPageIDs` entry, calls `flush`, reloads, and asserts the change.
* Prints **MasterPage tests passed!** on success.

---

## 3. ColumnPage

### Files

* `src/ColumnPage.hpp`
* `src/ColumnPage.cpp` (empty; all methods inline)
* `tests/test_columnpage.cpp`

### Role

* Represents an in-memory page of a fixed-size column.
* Manages a fixed number of **slots** for `ValueType` values and a **tombstone** bitmap to track used vs. free slots.

### Layout

```text
┌─────────────────────────────────────────┐
│ Header (meta-data)                     │
│ - pageID (uint16_t)                    │
│ - capacity (uint16_t): number of slots │
│ - count (uint16_t): used slots         │
│ - nextFreePage (uint16_t): link for free-page list (unused here)
└─────────────────────────────────────────┘
│ Data arrays:                           │
│ - ValueType values[capacity]           │
│ - bool tombstone[capacity]             │
└─────────────────────────────────────────┘
```

### API (in `ColumnPage.hpp`)

```cpp
class ColumnPage {
public:
  ColumnPage(uint16_t pageID, uint16_t slotCount);

  int16_t     findFreeSlot() const;
  void        markUsed(int slotIdx);
  void        markDeleted(int slotIdx);
  ValueType   readValue(int slotIdx) const;
  void        writeValue(int slotIdx, ValueType val);

  uint16_t    pageID;
  uint16_t    capacity;
  uint16_t    count;
  uint16_t    nextFreePage;
  std::vector<ValueType> values;
  std::vector<bool>       tombstone;
};
```

#### `findFreeSlot()`

* Scans `tombstone[]` for a `false` entry, returns its index or `-1` if none.

#### `markUsed(int slotIdx)`

* Sets `tombstone[slotIdx] = true` and increments `count` if it was previously free.

#### `markDeleted(int slotIdx)`

* Sets `tombstone[slotIdx] = false` and decrements `count` if it was previously used.

#### `readValue` / `writeValue`

* Access or assign the `values[slotIdx]` without changing `count`.

### Test: `test_columnpage.cpp`

* Instantiates `ColumnPage(1, 8)`.
* Asserts initial `count == 0` and all `tombstone` are false.
* Allocates all 8 slots, writes values, and asserts `tombstone[i] == true`.
* Deletes every second slot and asserts correct `count`.
* Ensures freed slots are reused when calling `findFreeSlot()` again.
* Prints **ColumnPage in-memory test passed!** on success.

---

## Next Steps

1. **`ColumnFile` implementation**: connect `MasterPage` and `ColumnPage` for on-disk CRUD.
2. **Unit tests for `ColumnFile`**: verify `allocSlot`, `fetchSlot`, and `deleteSlot`.
3. **Documentation updates**: keep this markdown in sync as new classes and methods are added.
4. **Integration with Metal kernels**: map column data into `MTLBuffer` and run compute shaders.

---

*Document last updated: July 8, 2025*
