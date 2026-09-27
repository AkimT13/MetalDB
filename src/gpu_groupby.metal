#include <metal_stdlib>
using namespace metal;

constant uint EMPTY_KEY  = 0xFFFFFFFFu;  // host routes real 0xFFFFFFFF keys to the CPU
constant uint TG_BUCKETS = 256u;         // threadgroup-local hash table size (power of two)
constant uint MAX_PROBES = 128u;         // global probe limit before reporting overflow

// ── 64-bit sums from 32-bit atomics ──────────────────────────────────────────
// Metal has no 64-bit atomic add on device memory, so each sum is a (lo, hi)
// pair. atomic_fetch_add returns the value immediately before this thread's add,
// so `old + v < old` detects exactly the adds that wrap lo; each such add carries
// one into hi. The carries sum to floor(total / 2^32) regardless of interleaving.
inline void add64(device atomic_uint* lo, device atomic_uint* hi, uint v) {
    const uint old = atomic_fetch_add_explicit(lo, v, memory_order_relaxed);
    if (old + v < old) atomic_fetch_add_explicit(hi, 1u, memory_order_relaxed);
}

inline void add64(threadgroup atomic_uint* lo, threadgroup atomic_uint* hi, uint v) {
    const uint old = atomic_fetch_add_explicit(lo, v, memory_order_relaxed);
    if (old + v < old) atomic_fetch_add_explicit(hi, 1u, memory_order_relaxed);
}

// Inserts (key, count, sumLo, sumHi) into the global table. Returns false if no
// slot was found within MAX_PROBES (the host then retries with a larger table).
inline bool globalInsert(uint key, uint cnt, uint sumLo, uint sumHi,
                         device atomic_uint* bucketKeys, device atomic_uint* bucketCnts,
                         device atomic_uint* bucketSumLo, device atomic_uint* bucketSumHi,
                         uint numBuckets) {
    uint slot = (key * 2654435761u) & (numBuckets - 1u);
    for (uint probe = 0; probe < MAX_PROBES; ++probe) {
        uint cur = atomic_load_explicit(&bucketKeys[slot], memory_order_relaxed);
        if (cur == EMPTY_KEY) {
            uint expected = EMPTY_KEY;
            atomic_compare_exchange_weak_explicit(&bucketKeys[slot], &expected, key,
                                                  memory_order_relaxed, memory_order_relaxed);
            cur = atomic_load_explicit(&bucketKeys[slot], memory_order_relaxed);
            if (cur == EMPTY_KEY) continue;  // spurious CAS failure: retry this slot
        }
        if (cur == key) {
            atomic_fetch_add_explicit(&bucketCnts[slot], cnt, memory_order_relaxed);
            add64(&bucketSumLo[slot], &bucketSumHi[slot], sumLo);
            if (sumHi) atomic_fetch_add_explicit(&bucketSumHi[slot], sumHi, memory_order_relaxed);
            return true;
        }
        slot = (slot + 1u) & (numBuckets - 1u);
    }
    return false;
}

// Two-level GPU group-by (COUNT + 64-bit SUM per key).
//
// Phase 1: initialise a per-threadgroup hash table in threadgroup memory.
// Phase 2: each thread inserts its (key, val) into the threadgroup table
//          (low contention for low-cardinality keys); if that table is full the
//          thread inserts directly into the global table.
// Phase 3: after a barrier, threads merge slices of the threadgroup table into
//          the global table.
// Any row that cannot be placed sets *overflow; the host discards the result and
// re-runs with more buckets, so rows are never silently dropped.
kernel void group_by(
    device const uint*   keys        [[buffer(0)]],
    device const uint*   vals        [[buffer(1)]],
    constant uint&       n           [[buffer(2)]],
    device atomic_uint*  bucketKeys  [[buffer(3)]],
    device atomic_uint*  bucketCnts  [[buffer(4)]],
    device atomic_uint*  bucketSumLo [[buffer(5)]],
    constant uint&       numBuckets  [[buffer(6)]],
    device atomic_uint*  bucketSumHi [[buffer(7)]],
    device atomic_uint*  overflow    [[buffer(8)]],
    uint gid    [[thread_position_in_grid]],
    uint lid    [[thread_position_in_threadgroup]],
    uint tgSize [[threads_per_threadgroup]])
{
    threadgroup atomic_uint tg_keys[TG_BUCKETS];
    threadgroup atomic_uint tg_cnts[TG_BUCKETS];
    threadgroup atomic_uint tg_lo[TG_BUCKETS];
    threadgroup atomic_uint tg_hi[TG_BUCKETS];

    // ── Phase 1 ──────────────────────────────────────────────────────────────
    for (uint i = lid; i < TG_BUCKETS; i += tgSize) {
        atomic_store_explicit(&tg_keys[i], EMPTY_KEY, memory_order_relaxed);
        atomic_store_explicit(&tg_cnts[i], 0u,        memory_order_relaxed);
        atomic_store_explicit(&tg_lo[i],   0u,        memory_order_relaxed);
        atomic_store_explicit(&tg_hi[i],   0u,        memory_order_relaxed);
    }
    threadgroup_barrier(mem_flags::mem_threadgroup);

    // ── Phase 2 ──────────────────────────────────────────────────────────────
    if (gid < n) {
        const uint key = keys[gid];
        const uint val = vals[gid];

        uint tg_slot = (key * 2654435761u) & (TG_BUCKETS - 1u);
        bool tg_done = false;
        for (uint probe = 0; probe < TG_BUCKETS; ++probe) {
            uint cur = atomic_load_explicit(&tg_keys[tg_slot], memory_order_relaxed);
            if (cur == EMPTY_KEY) {
                uint expected = EMPTY_KEY;
                atomic_compare_exchange_weak_explicit(&tg_keys[tg_slot], &expected, key,
                                                      memory_order_relaxed, memory_order_relaxed);
                cur = atomic_load_explicit(&tg_keys[tg_slot], memory_order_relaxed);
                if (cur == EMPTY_KEY) { --probe; continue; }  // spurious failure: retry slot
            }
            if (cur == key) {
                atomic_fetch_add_explicit(&tg_cnts[tg_slot], 1u, memory_order_relaxed);
                add64(&tg_lo[tg_slot], &tg_hi[tg_slot], val);
                tg_done = true;
                break;
            }
            tg_slot = (tg_slot + 1u) & (TG_BUCKETS - 1u);
        }

        if (!tg_done &&
            !globalInsert(key, 1u, val, 0u, bucketKeys, bucketCnts, bucketSumLo, bucketSumHi, numBuckets))
            atomic_store_explicit(overflow, 1u, memory_order_relaxed);
    }
    threadgroup_barrier(mem_flags::mem_threadgroup);

    // ── Phase 3 ──────────────────────────────────────────────────────────────
    for (uint i = lid; i < TG_BUCKETS; i += tgSize) {
        const uint k = atomic_load_explicit(&tg_keys[i], memory_order_relaxed);
        if (k == EMPTY_KEY) continue;
        const uint c  = atomic_load_explicit(&tg_cnts[i], memory_order_relaxed);
        const uint lo = atomic_load_explicit(&tg_lo[i],   memory_order_relaxed);
        const uint hi = atomic_load_explicit(&tg_hi[i],   memory_order_relaxed);
        if (!globalInsert(k, c, lo, hi, bucketKeys, bucketCnts, bucketSumLo, bucketSumHi, numBuckets))
            atomic_store_explicit(overflow, 1u, memory_order_relaxed);
    }
}
