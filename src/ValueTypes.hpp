#pragma once

#include <cstdint>
#include <cstring>
#include <limits>

// Legacy scalar type – kept so existing code compiles unchanged.
using Number    = uint32_t;
using ValueType = Number;
static constexpr size_t VALUE_SIZE = sizeof(ValueType);

// ── Per-column type tag ───────────────────────────────────────────────────────
enum class ColType : uint8_t {
    UINT32 = 0,   // 4-byte unsigned integer (existing default)
    INT64  = 1,   // 8-byte signed integer
    FLOAT  = 2,   // 4-byte IEEE-754 single
    DOUBLE = 3,   // 8-byte IEEE-754 double
};

/// Bytes per on-disk slot for the given ColType.
inline uint16_t colValueBytes(ColType t) {
    return (t == ColType::INT64 || t == ColType::DOUBLE) ? 8u : 4u;
}

// ── Tagged value at the API boundary ─────────────────────────────────────────
struct ColValue {
    ColType type = ColType::UINT32;
    union { uint32_t u32; int64_t i64; float f32; double f64; };

    ColValue() : type(ColType::UINT32), u32(0) {}
    explicit ColValue(uint32_t v) : type(ColType::UINT32), u32(v) {}
    explicit ColValue(int64_t  v) : type(ColType::INT64),  i64(v) {}
    explicit ColValue(float    v) : type(ColType::FLOAT),  f32(v) {}
    explicit ColValue(double   v) : type(ColType::DOUBLE), f64(v) {}

    // Widen to double for generic comparisons / arithmetic.
    double toDouble() const {
        switch (type) {
            case ColType::UINT32: return static_cast<double>(u32);
            case ColType::INT64:  return static_cast<double>(i64);
            case ColType::FLOAT:  return static_cast<double>(f32);
            case ColType::DOUBLE: return f64;
        }
        return 0.0;
    }

    // Truncate back to the legacy ValueType (uint32_t).
    ValueType asU32() const {
        switch (type) {
            case ColType::UINT32: return u32;
            case ColType::INT64:  return static_cast<uint32_t>(i64);
            case ColType::FLOAT:  return static_cast<uint32_t>(f32);
            case ColType::DOUBLE: return static_cast<uint32_t>(f64);
        }
        return 0;
    }

    bool operator==(const ColValue& o) const { return toDouble() == o.toDouble(); }
    bool operator< (const ColValue& o) const { return toDouble() <  o.toDouble(); }
    bool operator> (const ColValue& o) const { return toDouble() >  o.toDouble(); }
};
