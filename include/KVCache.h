#pragma once

#include <atomic>
#include <mutex>

#include "Filter.h"
#include "Common.h"

struct KVCacheEntry {
  TS ts;
  Value v;

  KVCacheEntry(TS ts, Value v):ts(ts), v(v) {}
};

struct KVTS {
  Key k;
  Value v;
  TS ts;

  KVTS() {}
  KVTS(Key k, Value v, TS ts): k(k), v(v), ts(ts) {}

  bool operator > (const KVTS &rhs) const {
    return k > rhs.k;
  }

  bool operator < (const KVTS &rhs) const {
    return k < rhs.k;
  }

  bool operator == (const KVTS &rhs) const {
    return k == rhs.k;
  }
};

constexpr int kMaxLevel = 11;

struct SkipListNode {
    KVTS kvts;
    uint16_t level;
    uint16_t next_ptrs[kMaxLevel];    
};

template <typename T>
class MonotonicBufferRing {
public:
    MonotonicBufferRing(int size): size(size), buffer(size * sizeof(T)) {}

    T * alloc() {
        int pos = offset.fetch_add(1);
        while (!have_space(pos, 1)) ;
        return &buffer[pos];
    }

    T * alloc(int n) {
        int pos = offset.fetch_add(n);
        while (!have_space(pos, n)) ;
        return &buffer[pos];
    }

    void release(int n) {
        start_mutex.lock();
        start += n;
        start_mutex.unlock();
    }

private:
    inline bool have_space(int old, int n) {
        start_mutex.lock_shared();
        bool ret = old + n - start < size;
        start_mutex.unlock_shared();
        return ret;
    }

    HugePages<T> buffer;
    int start{0};
    int size;
    std::atomic<size_t> offset{0};
    std::shared_mutex start_mutex;
};

constexpr int kMaxFilters = 1024;

// struct FilterPoolNode {
//     Bloomfilter *filter;
//     int start_offset{-1};

//     FilterPoolNode() {
//         filter = new Bloomfilter();
//     }
// };

// class FilterPool {
// public:
//     FilterPool() {}

//     int contain(Key key) {
//         int back = latest;
//         if (latest < oldest) {
//             back += kMaxFilters;
//         }
        
//         Bloomfilter::hash(key);
//         for (int i = back; i > oldest; i-- ) {
//             int idx = (i % kMaxFilters) - 1;
//             if (filters[i].filter->contains());
//                 return idx;
//         }

//         assert(false);
//         return -1; 
//     }

//     int contain(Key key, int start) {
//         int back = start;
//         if (latest < oldest) {
//             back += kMaxFilters;
//         }
        
//         Bloomfilter::hash(key);
//         for (int i = back; i > oldest; i-- ) {
//             int idx = (i % kMaxFilters) - 1;
//             if (filters[i].filter->contains());
//                 return idx;
//         }

//         assert(false);
//         return -1; 
//     }

// private:
//     int oldest{0};
//     int latest{0};
//     bool no_free{false};
//     FilterPoolNode filters[kMaxFilters];
// };

// class KVCache {
// public:
//     KVCache(int cache_size) {

//     }

// private:

// };