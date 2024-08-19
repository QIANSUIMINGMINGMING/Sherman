#if !defined(_INDEX_CACHE_H_)
#define _INDEX_CACHE_H_

#include <atomic>
#include <queue>
#include <vector>

#include "CacheEntry.h"
#include "HugePageAlloc.h"
#include "Timer.h"
#include "WRLock.h"
#include "mc.h"
#include "third_party/inlineskiplist.h"
// #include <oneapi/tbb/concurrent_hash_map.h>

extern bool enter_debug;

using CacheSkipList = InlineSkipList<CacheEntryComparator>;

// struct KVCacheEntry {
//   TS ts;
//   Value v;

//   KVCacheEntry(TS ts, Value v):ts(ts), v(v) {}
// };
// using CacheHashMap = oneapi::tbb::concurrent_hash_map<uint64_t,
// KVCacheEntry>;

// class KVCache {
// public:
//   // Bytes
//   KVCache(int cache_size);

//   void add_to_cache(uint64_t k, TS ts, Value v) {
//     CacheHashMap::accessor a;
//     if (ts_table.find(a, k)) {
//       if (ts > a->second.ts) {
//         a->second.ts = ts;
//       }
//     } else {
//       ts_table.insert(std::make_pair(k, KVCacheEntry(ts, v)));
//       //postsend
//       cur_size.fetch_add(1);
//       if (cur_size >= cache_size) {
//         batch_insert();
//       }
//     }
//   }

//   void search_from_cache(const Key &k, Value & v) {
//     CacheHashMap::const_accessor a;
//     if (ts_table.find(a, k)) {
//       v = a->second.v;
//     } else {

//     }
//   }

//   void batch_insert() {

//   }

// const CacheEntry *search_from_cache(const Key &k, GlobalAddress *addr,
//                                       bool is_leader = false);

//   void search_range_from_cache(const Key &from, const Key &to,
//                                std::vector<LeafPage *> &result);

//   bool add_entry(const Key &from, const Key &to, LeafPage *ptr);
//   const CacheEntry *find_entry(const Key &k);
//   const CacheEntry *find_entry(const Key &from, const Key &to);

//   bool invalidate(const CacheEntry *entry);

//   const CacheEntry *get_a_random_entry(uint64_t &freq);

//   void statistics();

//   void bench();

// private:

//   // struct KVCacheEntry {
//   //   TS ts;
//   //   Value v;

//   //   KVCacheEntry(TS ts, Value v):ts(ts), v(v) {}
//   // };

//   CacheHashMap ts_table;
//   int cache_size;

//   std::atomic<int> cur_size = 0;
//   std::unique_ptr<rdmacm::multicast::multicastCM> mcm;

// };

class IndexCache {
 public:
  IndexCache(int cache_size);

  bool add_to_cache(InternalPage *page);
  const CacheEntry *search_from_cache(const Key &k, GlobalAddress *addr,
                                      bool is_leader = false);
  const CacheEntry *search_from_cache(const Key &k, int &entry_idx,
                                      bool is_leader = false);

  void search_range_from_cache(const Key &from, const Key &to,
                               std::vector<InternalPage *> &result);

  bool add_entry(const Key &from, const Key &to, InternalPage *ptr);
  const CacheEntry *find_entry(const Key &k);
  const CacheEntry *find_entry(const Key &from, const Key &to);

  bool invalidate(const CacheEntry *entry);

  const CacheEntry *get_a_random_entry(uint64_t &freq);

  void statistics();

  void bench();

 private:
  uint64_t cache_size;  // MB;
  std::atomic<int64_t> free_page_cnt;
  std::atomic<int64_t> skiplist_node_cnt;
  int64_t all_page_cnt;

  std::queue<std::pair<void *, uint64_t>> delay_free_list;
  WRLock free_lock;

  // SkipList
  CacheSkipList *skiplist;
  CacheEntryComparator cmp;
  Allocator alloc;

  void evict_one();
};

inline IndexCache::IndexCache(int cache_size) : cache_size(cache_size) {
  skiplist = new CacheSkipList(cmp, &alloc, 20);
  uint64_t memory_size = define::MB * cache_size;

  all_page_cnt = memory_size / sizeof(InternalPage);
  free_page_cnt.store(all_page_cnt);
  skiplist_node_cnt.store(0);
}

// [from, to）
inline bool IndexCache::add_entry(const Key &from, const Key &to,
                                  InternalPage *ptr) {
  // TODO memory leak
  auto buf = skiplist->AllocateKey(sizeof(CacheEntry));
  auto &e = *(CacheEntry *)buf;
  e.from = from;
  e.to = to - 1;  // !IMPORTANT;
  e.ptr = ptr;

  return skiplist->InsertConcurrently(buf);
}

inline const CacheEntry *IndexCache::find_entry(const Key &from,
                                                const Key &to) {
  CacheSkipList::Iterator iter(skiplist);

  CacheEntry e;
  e.from = from;
  e.to = to - 1;
  iter.Seek((char *)&e);
  if (iter.Valid()) {
    auto val = (const CacheEntry *)iter.key();
    return val;
  } else {
    return nullptr;
  }
}

inline const CacheEntry *IndexCache::find_entry(const Key &k) {
  return find_entry(k, k + 1);
}

inline bool IndexCache::add_to_cache(InternalPage *page) {
  auto new_page = (InternalPage *)malloc(kInternalPageSize);
  memcpy(new_page, page, kInternalPageSize);
  new_page->index_cache_freq = 0;

  if (this->add_entry(page->hdr.lowest, page->hdr.highest, new_page)) {
    skiplist_node_cnt.fetch_add(1);
    auto v = free_page_cnt.fetch_add(-1);
    if (v <= 0) {
      evict_one();
    }

    return true;
  } else {  // conflicted
    auto e = this->find_entry(page->hdr.lowest, page->hdr.highest);
    if (e && e->from == page->hdr.lowest && e->to == page->hdr.highest - 1) {
      auto ptr = e->ptr;
      if (ptr == nullptr &&
          __sync_bool_compare_and_swap(&(e->ptr), 0ull, new_page)) {
        auto v = free_page_cnt.fetch_add(-1);
        if (v <= 0) {
          evict_one();
        }
        return true;
      }
    }

    free(new_page);
    return false;
  }
}

inline const CacheEntry *IndexCache::search_from_cache(const Key &k,
                                                       GlobalAddress *addr,
                                                       bool is_leader) {
  // notice: please ensure the thread 0 can make progress
  if (is_leader &&
      !delay_free_list.empty()) {  // try to free a page in the delay-free-list
    auto p = delay_free_list.front();
    if (asm_rdtsc() - p.second > 3000ull * 10) {
      free(p.first);
      free_page_cnt.fetch_add(1);

      free_lock.wLock();
      delay_free_list.pop();
      free_lock.wUnlock();
    }
  }

  auto entry = find_entry(k);

  InternalPage *page = entry ? entry->ptr : nullptr;

  if (page && entry->from <= k && entry->to >= k) {
    page->index_cache_freq++;

    auto cnt = page->hdr.last_index + 1;
    if (k < page->records[0].key) {
      *addr = page->hdr.leftmost_ptr;
    } else {
      bool find = false;
      for (int i = 1; i < cnt; ++i) {
        if (k < page->records[i].key) {
          find = true;
          *addr = page->records[i - 1].ptr;
          break;
        }
      }
      if (!find) {
        *addr = page->records[cnt - 1].ptr;
      }
    }

    compiler_barrier();
    if (entry->ptr) {  // check if it is freed.
      return entry;
    }
  }

  return nullptr;
}

inline const CacheEntry *IndexCache::search_from_cache(const Key &k,
                                                       int &entry_idx,
                                                       bool is_leader) {
  // notice: please ensure the thread 0 can make progress
  if (is_leader &&
      !delay_free_list.empty()) {  // try to free a page in the delay-free-list
    auto p = delay_free_list.front();
    if (asm_rdtsc() - p.second > 3000ull * 10) {
      free(p.first);
      free_page_cnt.fetch_add(1);

      free_lock.wLock();
      delay_free_list.pop();
      free_lock.wUnlock();
    }
  }

  auto entry = find_entry(k);

  InternalPage *page = entry ? entry->ptr : nullptr;

  if (page && entry->from <= k && entry->to >= k) {
    page->index_cache_freq++;

    auto cnt = page->hdr.last_index + 1;
    if (k < page->records[0].key) {
      entry_idx = -1;
    } else {
      bool find = false;
      for (int i = 1; i < cnt; ++i) {
        if (k < page->records[i].key) {
          find = true;
          entry_idx = i - 1;
          break;
        }
      }
      if (!find) {
        entry_idx = cnt - 1;
      }
    }

    compiler_barrier();
    if (entry->ptr) {  // check if it is freed.
      return entry;
    }
  }

  return nullptr;
}

inline void IndexCache::search_range_from_cache(
    const Key &from, const Key &to, std::vector<InternalPage *> &result) {
  CacheSkipList::Iterator iter(skiplist);

  result.clear();
  CacheEntry e;
  e.from = from;
  e.to = from;
  iter.Seek((char *)&e);

  while (iter.Valid()) {
    auto val = (const CacheEntry *)iter.key();
    if (val->ptr) {
      if (val->from > to) {
        return;
      }
      result.push_back(val->ptr);
    }
    iter.Next();
  }
}

inline bool IndexCache::invalidate(const CacheEntry *entry) {
  auto ptr = entry->ptr;

  if (ptr == nullptr) {
    return false;
  }

  if (__sync_bool_compare_and_swap(&(entry->ptr), ptr, 0)) {
    free_lock.wLock();
    delay_free_list.push(std::make_pair(ptr, asm_rdtsc()));
    free_lock.wUnlock();
    return true;
  }

  return false;
}

inline const CacheEntry *IndexCache::get_a_random_entry(uint64_t &freq) {
  uint32_t seed = asm_rdtsc();
  GlobalAddress tmp_addr;
retry:
  auto k = rand_r(&seed) % (1000ull * define::MB);
  auto e = this->search_from_cache(k, &tmp_addr);
  if (!e) {
    goto retry;
  }
  auto ptr = e->ptr;
  if (!ptr) {
    goto retry;
  }

  freq = ptr->index_cache_freq;
  if (e->ptr != ptr) {
    goto retry;
  }
  return e;
}

inline void IndexCache::evict_one() {
  uint64_t freq1, freq2;
  auto e1 = get_a_random_entry(freq1);
  auto e2 = get_a_random_entry(freq2);

  if (freq1 < freq2) {
    invalidate(e1);
  } else {
    invalidate(e2);
  }
}

inline void IndexCache::statistics() {
  printf("[skiplist node: %ld]  [page cache: %ld]\n", skiplist_node_cnt.load(),
         all_page_cnt - free_page_cnt.load());
}

inline void IndexCache::bench() {
  Timer t;
  t.begin();
  const int loop = 100000;

  for (int i = 0; i < loop; ++i) {
    uint64_t r = rand() % (5 * define::MB);
    this->find_entry(r);
  }

  t.end_print(loop);
}

#endif  // _INDEX_CACHE_H_
