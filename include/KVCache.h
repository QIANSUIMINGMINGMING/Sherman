#pragma once

#include <atomic>
#include <mutex>
#include <shared_mutex>
#include <random>
#include <thread>
#include <queue>

#include "Filter.h"
#include "Common.h"
#include "third_party/random.h"

#define RING_SUB(a, b, n) ((a + n - b ) % n)
#define RING_ADD(a, b, n) ((a + b) % n)

struct KVCacheEntry
{
  TS ts;
  Value v;

  KVCacheEntry(TS ts, Value v) : ts(ts), v(v) {}
};

struct KVTS
{
  Key k;
  Value v;
  TS ts;

  KVTS() {}
  KVTS(Key k, Value v, TS ts) : k(k), v(v), ts(ts) {}

  inline bool operator>(const KVTS &rhs) const
  {
    return k != rhs.k ? k > rhs.k : ts > rhs.ts;
  }

  inline bool operator<(const KVTS &rhs) const
  {
    return k != rhs.k ? k < rhs.k : ts < rhs.ts;
  }

  inline bool operator==(const KVTS &rhs) const
  {
    return k == rhs.k && ts == rhs.ts;
  }
};

template <typename T>
class MonotonicBufferRing
{
public:
  MonotonicBufferRing(int size) : size(size), buffer(size * sizeof(T)) {}

  T *alloc(int & start_offset)
  {
    int pos = offset.fetch_add(1);
    while (!have_space(pos, 1))
      ;
    return &buffer[pos % size];
    start_offset = pos % size;
  }

  T *alloc(int n, int & start_offset)
  {
    int pos = offset.fetch_add(n);
    while (!have_space(pos, n))
      ;
    return &buffer[pos % size];
    start_offset = pos % size;
  }

  int check_distance(int start_offset, int & cur_offset) {
    cur_offset = offset.load() % size;
    return RING_SUB(cur_offset, start_offset, size);
  }

  // void release(int n)
  // {
  // restart:
  //   bool need_restart = false;
  //   cutil::ull_t cur_v = cutil::read_lock_or_restart(start_mutex, need_restart);
  //   if (need_restart) {
  //     goto restart;
  //   }
  //   cutil::upgrade_to_write_lock_or_restart(start_mutex, cur_v, need_restart);
  //   if (need_restart) {
  //     goto restart;
  //   }
  //   start += n;
  //   cutil::write_unlock(start_mutex);
  // }

  void release(int start_offset) {
    restart:
    bool need_restart = false;
    cutil::ull_t cur_v = cutil::read_lock_or_restart(start_mutex, need_restart);
    if (need_restart) {
      goto restart;
    }
    cutil::upgrade_to_write_lock_or_restart(start_mutex, cur_v, need_restart);
    if (need_restart) {
      goto restart;
    }
    start += RING_SUB(start_offset, start, size);
    cutil::write_unlock(start_mutex);
  }

  //operator[]
  T &operator[](int i)
  {
    return buffer[i % size];
  }

  inline int getOffset() {
    return offset.load() % size;
  }

private:
  inline bool have_space(int old, int n)
  {
  restart:
    bool need_restart = false;
    cutil::ull_t cur_v = cutil::read_lock_or_restart(start_mutex, need_restart);
    if (need_restart) {
      goto restart;
    }
    bool ret = old + n - start < size;
    cutil::read_unlock_or_restart(start_mutex, cur_v, need_restart);
    if (need_restart) {
      goto restart;
    }
    return ret;
  }

  int size;
  HugePages<T> buffer;
  size_t start{0};
  std::atomic<size_t> offset{0};
  std::atomic<cutil::ull_t> start_mutex;
};

constexpr int kMaxFilters = 1024 * 1024;

constexpr TS NULL_TS = kTSMax;
constexpr TS START_TS = kTSMin;

struct FilterPoolNode
{
  Bloomfilter *filter;
  int start_offset{-1};
  TS endTS{NULL_TS};
  std::atomic<cutil::ull_t> skiplist_lock_;

  FilterPoolNode()
  {
    filter = new Bloomfilter();
  }
};

constexpr int kMaxLevel = 12;

struct SkipListNode
{
  KVTS kvts;
  uint16_t level;
  uint32_t next_ptrs[kMaxLevel]{0};
  //next_ptrs[0] is front
};

class FilterPool
{
public:
  FilterPool(MonotonicBufferRing<SkipListNode> * buffer) {
    create(buffer);
    filterCreator = std::thread(std::bind(&FilterPool::filter_thread, this, buffer));
    bufferClearer = std::thread(std::bind(&FilterPool::buffer_thread, this, buffer));
  }

  int contain(Key key, bool &is_latest, int &cur_oldest)
  {
    Bloomfilter::hash(key);
  restart:
    bool need_restart = false;
    auto cur_v = cutil::read_lock_or_restart(mutex_, need_restart);
    if (need_restart) {
      goto restart;
    }
    int back = latest;
    if (latest < oldest)
    {
      back += kMaxFilters;
    }

    if ( filters[(back % kMaxFilters) - 1].filter->contains()) {
      is_latest = true;
      cur_oldest = oldest;
      cutil::read_unlock_or_restart(mutex_, cur_v, need_restart);
      if (need_restart) {
        goto restart;
      }
      return (back % kMaxFilters) - 1;
    }
    for (int i = back - 1; i > oldest; i--)
    {
      int idx = (i % kMaxFilters) - 1;
      if (filters[idx].filter->contains())
      {
        cur_oldest = oldest;
        cutil::read_unlock_or_restart(mutex_, cur_v, need_restart);
        if (need_restart) {
          goto restart;
        }
        return idx;
      }
    }

    cutil::read_unlock_or_restart(mutex_, cur_v, need_restart);
    if (need_restart) {
      goto restart;
    }
    return -1;
  }

  int contain(Key key, int start, int pre_oldest)
  {
    Bloomfilter::hash(key);

    restart:
    bool need_restart = false;
    auto cur_v = cutil::read_lock_or_restart(mutex_, need_restart);
    if (need_restart) {
      goto restart;
    }

    int back = start;
    if (RING_SUB(start, pre_oldest, kMaxFilters) < RING_SUB(oldest, pre_oldest, kMaxFilters)) {
      return -1;
    } 
    if (start < oldest)
    {
      back += kMaxFilters;
    }

    for (int i = back; i > oldest; i--)
    {
      int idx = (i % kMaxFilters) - 1;
      if (filters[idx].filter->contains())
      {
        cutil::read_unlock_or_restart(mutex_, cur_v, need_restart);
        if (need_restart) {
          goto restart;
        }
        return idx;
      }
    }

    cutil::read_unlock_or_restart(mutex_, cur_v, need_restart);
    if (need_restart) {
      goto restart;
    }
    return -1;
  }

  FilterPoolNode *get_filter(TS ts, bool &is_latest)
  {
  restart:
    bool need_restart = false;
    auto cur_v = cutil::read_lock_or_restart(mutex_, need_restart);
    if (need_restart) {
      goto restart;
    }
    assert(!isEmpty());
    if (ts < filters[oldest].endTS)
    {
      cutil::read_unlock_or_restart(mutex_, cur_v, need_restart);
      if (need_restart) {
        goto restart;
      }
      return &filters[oldest];
    }
    int back = latest > oldest ? latest : latest + kMaxFilters;

    if (filters[back - 1].endTS < ts && ts <= filters[back].endTS)
    {
      cutil::read_unlock_or_restart(mutex_, cur_v, need_restart);
      if (need_restart) {
        goto restart;
      }
      is_latest = true;
      return &filters[back - 1];
    }

    for (int i = back - 1; i > oldest + 1; i--)
    {
      int idx = (i % kMaxFilters) - 1;
      if (filters[idx - 1].endTS < ts && ts <= filters[idx].endTS)
      {
        cutil::read_unlock_or_restart(mutex_, cur_v, need_restart);
        if (need_restart) {
          goto restart;
        }
        return &filters[idx];
      }
    }
    assert(false);
  }

  //operator []
  FilterPoolNode &operator[](int i)
  {
    return filters[i];
  } 

  void updateGlobalTS(TS ts) {
    TS_mutex_.lock();
    oldest_time = ts;
    TS_mutex_.unlock();
  }

private:
  static constexpr int filter_thread_interval = 1000000; //ns

  void create(MonotonicBufferRing<SkipListNode> * buffer)
  {
  restart:
    bool need_restart = false;
    auto cur_v = cutil::read_lock_or_restart(mutex_, need_restart);
    if (need_restart) {
      goto restart;
    }
    cutil::upgrade_to_write_lock_or_restart(mutex_, cur_v, need_restart);
    if (need_restart) {
      goto restart;
    }

    assert((latest + 1) % kMaxFilters != oldest);
    if (!isEmpty())
      filters[latest - 1].endTS = myClock::get_ts();
    // create first skiplist node
    int start_offset;
    buffer->alloc(start_offset);
    filters[latest].start_offset = start_offset;
    latest = RING_ADD(latest, 1, kMaxFilters);
    cutil::write_unlock(mutex_);
  }

  void release(int n)
  {
    skRestart:
    bool sk_need_restart = false;
    for (int i = 0; i < n; i++) {
      FilterPoolNode * fpn = &filters[(oldest + i) % kMaxFilters];
      auto sk_cur_v = cutil:: read_lock_or_restart(fpn->skiplist_lock_, sk_need_restart);
      if (sk_need_restart) {
        goto skRestart;
      }
      cutil::upgrade_to_write_lock_or_restart(fpn->skiplist_lock_, sk_cur_v, sk_need_restart);
      if (sk_need_restart) {
        goto skRestart;
      }
    }

    restart:
    bool need_restart = false;
    auto cur_v = cutil::read_lock_or_restart(mutex_, need_restart);
    if (need_restart) {
      goto restart;
    }
    cutil::upgrade_to_write_lock_or_restart(mutex_, cur_v, need_restart);
    if (need_restart) {
      goto restart;
    }
    assert (RING_SUB(latest, oldest, kMaxFilters) > n);
    for (int i = 0; i < n; i++)
    {
      assert(filters[(oldest + i) % kMaxFilters].endTS < oldest_time);
      filters[(oldest + i) % kMaxFilters].endTS = NULL_TS;
      filters[(oldest + i) % kMaxFilters].filter->clear();
      filters[(oldest + i) % kMaxFilters].start_offset = -1;
      filters[(oldest + i) % kMaxFilters].skiplist_lock_.store(0);
    }
    oldest = RING_ADD(oldest, n, kMaxFilters);
    delay_free_queue.push(filters[oldest].start_offset);
    cutil::write_unlock(mutex_);
  }

  static void filter_thread(FilterPool* fp, MonotonicBufferRing<SkipListNode>* buffer) {
    bindCore(filterCore);
    // FilterPool * fp = static_cast<FilterPool *>(args[0]);
    // MonotonicBufferRing<SkipListNode> * buffer = static_cast<MonotonicBufferRing<SkipListNode> *>(args[1]);
    struct timespec sleep_time;
    sleep_time.tv_nsec = filter_thread_interval;
    sleep_time.tv_sec = 0;
    while (true) {
      // sleep for 1 millisecond
      if (fp->isFull()) {
        fp->release(10);
      }
      int cur_offset;
      int cur_distance = buffer->check_distance(fp->filters[fp->latest].start_offset, cur_offset);
      if (cur_distance > 35000) {
        fp->create(buffer);
      }
      nanosleep(&sleep_time, nullptr); 
    }
  }

  static void buffer_thread(FilterPool* fp, MonotonicBufferRing<SkipListNode>* buffer) {
    bindCore(filterCore);
    // FilterPool *fp = static_cast<FilterPool *>(args[0]);
    // MonotonicBufferRing<SkipListNode> *buffer = static_cast<MonotonicBufferRing<SkipListNode>*>(args[1]);
    struct timespec sleep_time;
    sleep_time.tv_nsec = filter_thread_interval;
    sleep_time.tv_sec = 0;
    while (true) {
      int last = 0;
      while (!fp->delay_free_queue.empty()) {
        last = fp->delay_free_queue.front();
        fp->delay_free_queue.pop();
      }
      buffer->release(last);
      nanosleep(&sleep_time, nullptr);
    }
  }

  bool isEmpty()
  {
    return oldest == latest;
  }

  bool isFull() {
    return RING_ADD(latest, 1, kMaxFilters) == oldest;
  }

  int oldest{0};
  int latest{0};
  bool no_free{false};
  FilterPoolNode filters[kMaxFilters];
  std::atomic<cutil::ull_t> mutex_;
  std::thread bufferClearer;
  std::thread filterCreator;
  std::queue<int> delay_free_queue;
  std::shared_mutex TS_mutex_;
  TS oldest_time{START_TS};
};

constexpr int directFindBar = 100;

class KVCache
{
public:
  KVCache(int cache_size): cache_size(cache_size), buffer(cache_size), filter_pool(&buffer){
  }

  void insert(Key k, TS ts, Value v)
  {
    int node_offset;
    int sln_level = randomHeight();
    auto sln = buffer.alloc(1, node_offset);
    sln->kvts.k = k;
    sln->kvts.ts = ts;
    sln->kvts.v = v;
    sln->level = sln_level;
    bool filter_is_latest;
    auto filter = filter_pool.get_filter(ts, filter_is_latest);
    if (filter_is_latest) {
      skiplist_insert_latest(sln, node_offset, filter);
    } else {
      skiplist_insert(sln, node_offset, filter);
    }
    filter->filter->insert(k); 
  }

  bool search(Key k, Value & v) {
    bool filter_is_latest;
    int pre_oldest;
    int filter_idx = filter_pool.contain(k, filter_is_latest, pre_oldest);
    if (filter_idx == -1) {
      return false;
    }
    auto filter = &filter_pool[filter_idx];
    if (filter_is_latest) {
      if (skiplist_search_latest(k, v, filter))
        return true;
    }
    while (!skiplist_search(k, v, filter)) {
      filter_idx = filter_pool.contain(k, filter_idx, pre_oldest);
      if (filter_idx == -1) {
        return false;
      }
      filter = &filter_pool[filter_idx];
    }
    return true;
  }

  void insert_thread_run(int id) {
    while (true) {
      Key k = rand();
      Value v = rand();
      TS ts = myClock::get_ts();
      insert(k, ts, v);
    }
  }

  void clear_thread_run() {

  }

  void test_KV_cache() {
    // for (int i=0; i < 24; i++ ) {
    //   std::thread(insert_thread_run, i);
    // }
    // std::thread(clear_thread_run);
  }

  void clear_TS(TS oldest_TS) {
    filter_pool.updateGlobalTS(oldest_TS);
  }

private:
  void skiplist_insert(SkipListNode *sln, uint32_t node_offset, FilterPoolNode *filter)
  {
    //random int level
    int cur_pos = filter->start_offset;
  restart:
    bool need_restart = false;
    auto cur_v = cutil::read_lock_or_restart(filter->skiplist_lock_, need_restart);
    if (need_restart) {
      goto restart;
    }
    cutil::upgrade_to_write_lock_or_restart(filter->skiplist_lock_, cur_v, need_restart);
    if (need_restart) {
      goto restart;
    }
    auto skiplist_head = &buffer[filter->start_offset];
    auto p = skiplist_head;
    //search and insert;
    for (int i = kMaxLevel - 1; i >= 0; i--)
    {
      if (p->next_ptrs[i] == 0) {
        continue;
      }
      auto next_pos = p->next_ptrs[i];
      while (next_pos != 0 && buffer[next_pos].kvts.k < sln->kvts.k)
      {
        p = &buffer[next_pos];
        cur_pos = next_pos;
        next_pos = p->next_ptrs[i];
      } if (i < sln->level) {
        insert_buffer[i] = cur_pos;
      }
    }
    for (int i = 0; i< sln->level;i++) {
      auto pre_node = &buffer[insert_buffer[i]];
      sln->next_ptrs[i] = pre_node->next_ptrs[i];
      pre_node->next_ptrs[i] = node_offset;
    }
    cutil::write_unlock(filter->skiplist_lock_);
  }

  inline void unlock_previous(int start_pos, int start_level, int level) {
    for (int i = start_level; i< level; i++) {
      int pos = insert_buffer[i] - start_pos;
      cutil::write_unlock(Latest_lock_buffer[0][pos][i]);
    }
  }

  void skiplist_insert_latest(SkipListNode *sln, uint32_t node_offset, FilterPoolNode * filter) {
    int start_pos = filter->start_offset;
    int cur_pos = start_pos;
  restart:
    bool need_restart = false;
    auto skiplist_head = &buffer[filter->start_offset];
    auto cur_v = cutil::read_lock_or_restart(Latest_lock_buffer[0][cur_pos - start_pos][kMaxLevel - 1], need_restart);
    if (need_restart) {
      goto restart;
    }
    auto p = skiplist_head;
    for (int i = kMaxLevel - 1; i >= 0; i--)
    {
      auto next_pos = p->next_ptrs[i];
      while (next_pos != 0 && buffer[next_pos].kvts.k < sln->kvts.k)
      {
        p = &buffer[next_pos];
        auto f_v = cutil::read_lock_or_restart(Latest_lock_buffer[0][next_pos - start_pos][i], need_restart);
        if (need_restart) {
          unlock_previous(start_pos, i + 1, sln->level);
          goto restart;
        }
        cutil::read_unlock_or_restart(Latest_lock_buffer[0][cur_pos - start_pos][i], cur_v, need_restart);
        if (need_restart) {
          unlock_previous(start_pos, i + 1, sln->level);
          goto restart;
        }
        cur_pos = next_pos;
        next_pos = p->next_ptrs[i];
        cur_v = f_v;
      } if (i < sln->level) {
        cutil::upgrade_to_write_lock_or_restart(Latest_lock_buffer[0][cur_pos - start_pos][i], cur_v, need_restart);
        if (need_restart) {
          unlock_previous(start_pos, i+1, sln->level);
          goto restart;
        }
        insert_buffer[i] = cur_pos;
      } else {
        cutil::read_unlock_or_restart(Latest_lock_buffer[0][cur_pos - start_pos][i], cur_v, need_restart);
        if (need_restart) {
          unlock_previous(start_pos, i + 1, sln->level);
          goto restart;
        }
      } if (i > 0) {
        //lock next level
        int sig = int(i >= sln->level);
        auto f_v = cutil::read_lock_or_restart(Latest_lock_buffer[0][cur_pos - start_pos][i - 1], need_restart);
        if (need_restart) {
          unlock_previous(start_pos, i + sig, sln->level);
          goto restart;
        }
        cutil::read_unlock_or_restart(Latest_lock_buffer[0][cur_pos - start_pos][i], cur_v, need_restart);
        if (need_restart) {
          unlock_previous(start_pos, i + sig, sln->level);
          goto restart;
        }
        cur_v = f_v;
        next_pos = p->next_ptrs[i - 1];
      }
    } 
    for (int i = 0; i < sln->level; i++) {
      auto pre_node = &buffer[cur_pos];
      sln->next_ptrs[i] = pre_node->next_ptrs[i];
      pre_node->next_ptrs[i] = node_offset;
    }
  }

  bool skiplist_search(Key k, Value &v, FilterPoolNode * filter)
  {
  restart:
    bool need_restart = false;
    auto cur_v = cutil::read_lock_or_restart(filter->skiplist_lock_, need_restart);
    if (need_restart) {
      goto restart;
    }
    auto skiplist_head = &buffer[filter->start_offset];
    auto p = skiplist_head;
    for (int i = kMaxLevel - 1; i >= 0; i--)
    {
      if (p->next_ptrs[i] == 0) {
        continue;
      }
      auto next_pos = p->next_ptrs[i];
      while (next_pos != 0 && buffer[next_pos].kvts.k < k)
      {
        p = &buffer[next_pos];
        next_pos = p->next_ptrs[i];
      } if (next_pos != 0 && buffer[next_pos].kvts.k == k) {
        v = buffer[next_pos].kvts.v;
        cutil::read_unlock_or_restart(filter->skiplist_lock_, cur_v, need_restart);
        if (need_restart) {
          goto restart;
        }
        return true;
      } 
    }
    cutil::read_unlock_or_restart(filter->skiplist_lock_, cur_v, need_restart);
    if (need_restart) {
      goto restart;
    }
    return false;
  }

  bool skiplist_search_latest(Key k, Value & v, FilterPoolNode * filter) {
    int start_pos = filter->start_offset;
    int cur_pos = start_pos;
    int cur_offset;
    if (buffer.check_distance(start_pos, cur_offset) < directFindBar) {
      for (int i = start_pos; i < cur_offset;i++) {
        if (buffer[i].kvts.k == k) {
          v = buffer[i].kvts.v;
          return true;
        }
        return false;
      }
    }
  restart:
    bool need_restart = false;
    auto skiplist_head = &buffer[filter->start_offset];
    auto cur_v = cutil::read_lock_or_restart(Latest_lock_buffer[0][cur_pos - start_pos][kMaxLevel - 1], need_restart);
    if (need_restart) {
      goto restart;
    }
    auto p = skiplist_head;
    for (int i = kMaxLevel - 1; i >= 0; i--)
    {
      if (p->next_ptrs[i] == 0) {
        continue;
      }
      auto next_pos = p->next_ptrs[i];
      while (next_pos != 0 && buffer[next_pos].kvts.k < k)
      {
        p = &buffer[next_pos];
        auto f_v = cutil::read_lock_or_restart(Latest_lock_buffer[0][next_pos - start_pos][i], need_restart);
        if (need_restart) {
          goto restart;
        }
        cutil::read_unlock_or_restart(Latest_lock_buffer[0][cur_pos - start_pos][i], cur_v, need_restart);
        if (need_restart) {
          goto restart;
        }
        cur_pos = next_pos;
        next_pos = p->next_ptrs[i];
        cur_v = f_v;
      } if (next_pos != 0 && buffer[next_pos].kvts.k == k) {
        v = buffer[next_pos].kvts.v;
        cutil::read_unlock_or_restart(Latest_lock_buffer[0][cur_pos - start_pos][i], cur_v, need_restart);
        if (need_restart) {
          goto restart;
        }
        return true;
      } 
    }
  }

  int randomHeight() {
    auto rnd = Random::GetTLSInstance();

  // Increase height with probability 1 in kBranching
    int height = 1;
    while (height < kMaxLevel &&
          rnd->Next() < (Random::kMaxNext + 1)) {
      height++;
    }
    assert(height > 0);
    assert(height <= kMaxLevel);
    return height;
  }

  int cache_size;
  MonotonicBufferRing<SkipListNode> buffer;
  std::atomic<cutil::ull_t> Latest_lock_buffer[2][65536][kMaxLevel];
  std::atomic<uint8_t> fliper{0};
  FilterPool filter_pool;
  static thread_local uint32_t insert_buffer[kMaxLevel];
};