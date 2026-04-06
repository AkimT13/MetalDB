#include <metal_stdlib>
using namespace metal;

constant uint EMPTY_KEY  = 0xFFFFFFFFu;
constant uint TG_BUCKETS = 256u;  // threadgroup-local hash table size (must be power-of-two)

// Two-level GPU group-by kernel.
//
// Phase 1: initialise a per-threadgroup partial hash table in threadgroup memory.
// Phase 2: each thread inserts its (key, val) into the threadgroup table using
//          threadgroup atomics (much lower contention than global atomics for
//          low-cardinality group-bys).
//          If the threadgroup table is full (high-cardinality edge case), the
//          thread falls back to inserting directly into the global table.
// Phase 3: one barrier, then each thread merges a slice of the threadgroup table
//          into the global table — O(TG_BUCKETS/tgSize) global atomic ops per thread
//          vs. O(1) global atomic op per thread in the naive single-pass kernel.

kernel void group_by(
    device const uint*   keys        [[buffer(0)]],
    device const uint*   vals        [[buffer(1)]],
    constant uint&       n           [[buffer(2)]],
    device atomic_uint*  bucketKeys  [[buffer(3)]],
    device atomic_uint*  bucketCnts  [[buffer(4)]],
    device atomic_uint*  bucketSums  [[buffer(5)]],
    constant uint&       numBuckets  [[buffer(6)]],
    uint gid    [[thread_position_in_grid]],
    uint lid    [[thread_position_in_threadgroup]],
    uint tgSize [[threads_per_threadgroup]])
{
    // ── Phase 1: init threadgroup-local partial hash table ────────────────
    threadgroup atomic_uint tg_keys[TG_BUCKETS];
    threadgroup atomic_uint tg_cnts[TG_BUCKETS];
    threadgroup atomic_uint tg_sums[TG_BUCKETS];

    for (uint i = lid; i < TG_BUCKETS; i += tgSize) {
        atomic_store_explicit(&tg_keys[i], EMPTY_KEY, memory_order_relaxed);
        atomic_store_explicit(&tg_cnts[i], 0u,        memory_order_relaxed);
        atomic_store_explicit(&tg_sums[i], 0u,        memory_order_relaxed);
    }
    threadgroup_barrier(mem_flags::mem_threadgroup);

    // ── Phase 2: insert into threadgroup table (global fallback on overflow) ──
    if (gid < n) {
        uint key = keys[gid];
        uint val = vals[gid];

        uint tg_slot = (key * 2654435761u) % TG_BUCKETS;
        bool tg_done = false;

        for (uint probe = 0; probe < TG_BUCKETS; ++probe) {
            uint cur = atomic_load_explicit(&tg_keys[tg_slot], memory_order_relaxed);
            if (cur == key) {
                atomic_fetch_add_explicit(&tg_cnts[tg_slot], 1u,  memory_order_relaxed);
                atomic_fetch_add_explicit(&tg_sums[tg_slot], val, memory_order_relaxed);
                tg_done = true;
                break;
            }
            if (cur == EMPTY_KEY) {
                uint expected = EMPTY_KEY;
                bool ok = atomic_compare_exchange_weak_explicit(
                    &tg_keys[tg_slot], &expected, key,
                    memory_order_relaxed, memory_order_relaxed);
                if (ok || expected == key) {
                    atomic_fetch_add_explicit(&tg_cnts[tg_slot], 1u,  memory_order_relaxed);
                    atomic_fetch_add_explicit(&tg_sums[tg_slot], val, memory_order_relaxed);
                    tg_done = true;
                    break;
                }
            }
            tg_slot = (tg_slot + 1u) % TG_BUCKETS;
        }

        // Threadgroup table full (> ~200 distinct keys in this group): go global.
        if (!tg_done) {
            uint slot = (key * 2654435761u) % numBuckets;
            for (uint probe = 0; probe < numBuckets; ++probe) {
                uint cur = atomic_load_explicit(&bucketKeys[slot], memory_order_relaxed);
                if (cur == key) {
                    atomic_fetch_add_explicit(&bucketCnts[slot], 1u,  memory_order_relaxed);
                    atomic_fetch_add_explicit(&bucketSums[slot], val, memory_order_relaxed);
                    break;
                }
                if (cur == EMPTY_KEY) {
                    uint expected = EMPTY_KEY;
                    bool ok = atomic_compare_exchange_weak_explicit(
                        &bucketKeys[slot], &expected, key,
                        memory_order_relaxed, memory_order_relaxed);
                    if (ok || expected == key) {
                        atomic_fetch_add_explicit(&bucketCnts[slot], 1u,  memory_order_relaxed);
                        atomic_fetch_add_explicit(&bucketSums[slot], val, memory_order_relaxed);
                        break;
                    }
                }
                slot = (slot + 1u) % numBuckets;
            }
        }
    }
    threadgroup_barrier(mem_flags::mem_threadgroup);

    // ── Phase 3: merge threadgroup results into global table ──────────────
    for (uint i = lid; i < TG_BUCKETS; i += tgSize) {
        uint k = atomic_load_explicit(&tg_keys[i], memory_order_relaxed);
        if (k == EMPTY_KEY) continue;

        uint c = atomic_load_explicit(&tg_cnts[i], memory_order_relaxed);
        uint s = atomic_load_explicit(&tg_sums[i], memory_order_relaxed);

        uint slot = (k * 2654435761u) % numBuckets;
        for (uint probe = 0; probe < numBuckets; ++probe) {
            uint cur = atomic_load_explicit(&bucketKeys[slot], memory_order_relaxed);
            if (cur == k) {
                atomic_fetch_add_explicit(&bucketCnts[slot], c, memory_order_relaxed);
                atomic_fetch_add_explicit(&bucketSums[slot], s, memory_order_relaxed);
                break;
            }
            if (cur == EMPTY_KEY) {
                uint expected = EMPTY_KEY;
                bool ok = atomic_compare_exchange_weak_explicit(
                    &bucketKeys[slot], &expected, k,
                    memory_order_relaxed, memory_order_relaxed);
                if (ok || expected == k) {
                    atomic_fetch_add_explicit(&bucketCnts[slot], c, memory_order_relaxed);
                    atomic_fetch_add_explicit(&bucketSums[slot], s, memory_order_relaxed);
                    break;
                }
            }
            slot = (slot + 1u) % numBuckets;
        }
    }
}
