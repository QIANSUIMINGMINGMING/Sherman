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

  inline void self_print() {
    printf("KVTS with K %lu, TS %lu, V %lu\n", k, ts, v);
  }

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
}__attribute__((packed));

template <typename T>
class MonotonicBufferRing
{
public:
  MonotonicBufferRing(size_t size) : size(size), buffer(size * sizeof(T)) {}

  T *alloc(size_t & start_offset)
  {
    size_t pos = offset.fetch_add(1);
    while (!have_space(pos, 1))
      ;
    start_offset = pos % size;
    return &buffer[pos % size];
  }

  T *alloc(size_t n, size_t & start_offset)
  {
    size_t pos = offset.fetch_add(n);
    while (!have_space(pos, n))
      ;
    start_offset = pos % size;
    return &buffer[pos % size];
  }

  size_t check_distance(size_t start_offset, size_t & cur_offset) {
    cur_offset = offset.load() % size;
    return RING_SUB(cur_offset, start_offset, size);
  }

  void release(size_t start_offset) {
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
  T &operator[](size_t i)
  {
    return buffer[i % size];
  }

  inline size_t getOffset() {
    return offset.load() % size;
  }

private:
  inline bool have_space(size_t old, size_t n)
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

  size_t size;
  HugePages<T> buffer;
  size_t start{0};
  std::atomic<size_t> offset{0};
  std::atomic<cutil::ull_t> start_mutex;
};

constexpr int kMaxFilters = 1024 * 1024;

constexpr TS NULL_TS = kTSMax;
constexpr TS START_TS = kTSMin;

constexpr int kMaxSkipListData = 2 * 65536;
constexpr float kCreateDataRatio = 0.9;

struct FilterPoolNode
{
  Bloomfilter *filter;
  size_t start_offset{kKeyMax};
  TS endTS{NULL_TS};
  std::atomic<cutil::ull_t> skiplist_lock_;
  uint8_t my_fliper;
  std::atomic<int> olc_thread_count{0};

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
  size_t next_ptrs[kMaxLevel];
  //next_ptrs[0] is front
};

class FilterPool
{
public:
  FilterPool(MonotonicBufferRing<SkipListNode> * buffer) {
    create(buffer);
    filterCreator = std::thread(std::bind(&FilterPool::filter_thread, this, buffer));
    bufferClearer = std::thread(std::bind(&FilterPool::buffer_thread, this, buffer));
    releaser = std::thread(std::bind(&FilterPool::clear_thread, this));
  }

  FilterPool() {}

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

    if (back - 1 == oldest) {
      return &filters[oldest];
    }

    if (filters[back - 2].endTS < ts && ts <= filters[back - 1].endTS)
    {
      cutil::read_unlock_or_restart(mutex_, cur_v, need_restart);
      if (need_restart) {
        goto restart;
      }
      is_latest = true;
      return &filters[back - 1];
    }

    for (int i = back - 2; i > oldest; i--)
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
  inline FilterPoolNode &operator[](int i)
  {
    return filters[i];
  } 

  inline void updateGlobalTS(TS ts) {
    TS_mutex_.lock();
    oldest_time = ts;
    TS_mutex_.unlock();
  }

  inline void RlockGlobalTS() {
    TS_mutex_.lock_shared();
  }

  inline void RunlockGlobalTS() {
    TS_mutex_.unlock_shared();
  }

  inline void start_clearer() {
    clear_blocker.store(1);
  }

  inline  TS getGlobalTS() {
    return oldest_time;
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
      filters[RING_SUB(latest, 1, kMaxFilters)].endTS = myClock::get_ts();
    // create first skiplist node
    size_t start_offset;
    auto skp = buffer->alloc(start_offset);
    std::fill_n(skp->next_ptrs, kMaxLevel, kKeyMax);
    filters[latest].start_offset = start_offset;
    filters[latest].my_fliper = fliper.fetch_xor(1);
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
      filters[(oldest + i) % kMaxFilters].start_offset = kKeyMax;
      filters[(oldest + i) % kMaxFilters].skiplist_lock_.store(0);
    }
    oldest = RING_ADD(oldest, n, kMaxFilters);
    delay_free_queue.push(filters[oldest].start_offset);
    cutil::write_unlock(mutex_);
  }

  // a test function, will be replaced by a 
  static void clear_thread(FilterPool* fp) {
    bindCore(filterCore);
    struct timespec sleep_time;
    sleep_time.tv_nsec = filter_thread_interval;
    sleep_time.tv_sec = 0;
    while (!(fp->clear_blocker.load())) {
      nanosleep(&sleep_time, nullptr);
    }
    while (true) {
      sleep(1);
      TS ts = myClock::get_ts();
      int i = 0;
      while (fp->filters[RING_ADD(fp->oldest, i, kMaxFilters)].endTS < fp->oldest_time) {
        i++;
      }
      if (i > 0) { 
        fp->release(i); 
      }
      sleep(1);
      fp->updateGlobalTS(ts);
    }
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
      size_t cur_offset;
      size_t cur_distance = buffer->check_distance(fp->filters[fp->latest].start_offset, cur_offset);
      if (cur_distance > kCreateDataRatio * kMaxSkipListData) {
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
      size_t last = kKeyMax;
      while (!fp->delay_free_queue.empty()) {
        last = fp->delay_free_queue.front();
        fp->delay_free_queue.pop();
      }
      if (last != kKeyMax) {
        buffer->release(last);
      }
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
  std::atomic<uint8_t> fliper{0};
  std::atomic<cutil::ull_t> mutex_;
  std::thread bufferClearer;
  std::thread filterCreator;
  std::thread releaser;
  std::queue<size_t> delay_free_queue;
  std::shared_mutex TS_mutex_;
  std::atomic<int> clear_blocker{0};
  TS oldest_time{START_TS};
};

constexpr int directFindBar = 100;

class KVCache
{
public:
  KVCache(size_t cache_size): cache_size(cache_size), buffer(cache_size), filter_pool(&buffer){
  }

  KVCache(size_t cache_size, bool no_fp): cache_size(cache_size), buffer(cache_size), filter_pool(){
    size_t start_offset;
    auto skp = buffer.alloc(start_offset);
    std::fill_n(skp->next_ptrs, kMaxLevel, kKeyMax);
    filter_pool[0].start_offset = start_offset;
  }

  void insert(KVTS *kvts) {
    insert(kvts->k, kvts->ts, kvts->v); 
  }

  void insert(Key k, TS ts, Value v)
  {
    filter_pool.RlockGlobalTS();
    assert (ts > filter_pool.getGlobalTS()); 
    size_t node_offset;
    int sln_level = randomHeight();
    auto sln = buffer.alloc(1, node_offset);
    sln->kvts.k = k;
    sln->kvts.ts = ts;
    sln->kvts.v = v;
    sln->level = sln_level;
    std::fill_n(sln->next_ptrs, kMaxLevel, kKeyMax);
    bool filter_is_latest;
    auto filter = filter_pool.get_filter(ts, filter_is_latest);
    if (filter_is_latest) {
      filter->olc_thread_count.fetch_add(1);
      skiplist_insert_latest(sln, node_offset, filter);
      filter->olc_thread_count.fetch_sub(1);
    } else {
      while (filter->olc_thread_count.load() != 0){}
      skiplist_insert(sln, node_offset, filter);
    }
    filter->filter->insert(k);
    filter_pool.RunlockGlobalTS(); 
  }

  void test_buffer_insert(Key k, TS ts, Value v, bool is_latest) {
    size_t node_offset = 0;
    int sln_level = randomHeight();
    // printf("random height:%d\n", sln_level);
    auto sln = buffer.alloc(1, node_offset);
    sln->kvts.k = k;
    sln->kvts.ts = ts;
    sln->kvts.v = v;
    sln->level = sln_level;
    std::fill_n(sln->next_ptrs, kMaxLevel, kKeyMax);
    FilterPoolNode * filter = &filter_pool[0];
    // printf("insert %lu", k);
    if (is_latest) {
      skiplist_insert_latest(sln, node_offset, filter);
    } else {
      skiplist_insert(sln, node_offset, filter);
    }
  }

  bool search(Key k, Value & v) {
    filter_pool.RlockGlobalTS();
    bool filter_is_latest;
    int pre_oldest;
    int filter_idx = filter_pool.contain(k, filter_is_latest, pre_oldest);
    if (filter_idx == -1) {
      filter_pool.RunlockGlobalTS();
      return false;
    }
    assert(filter_pool[filter_idx].endTS > filter_pool.getGlobalTS());
    auto filter = &filter_pool[filter_idx];
    if (filter_is_latest) {
      filter->olc_thread_count.fetch_add(1);
      if (skiplist_search_latest(k, v, filter)) {
        filter->olc_thread_count.fetch_sub(1);
        filter_pool.RunlockGlobalTS();
        return true;
      }
    }
    while (filter->olc_thread_count.load()!= 0) {}
    while (!skiplist_search(k, v, filter)) {
      filter_idx = filter_pool.contain(k, filter_idx, pre_oldest);
      if (filter_idx == -1) {
        filter_pool.RunlockGlobalTS();
        return false;
      }
      assert(filter_pool[filter_idx].endTS > filter_pool.getGlobalTS());
      filter = &filter_pool[filter_idx];
    }
    filter_pool.RunlockGlobalTS();
    return true;
  }

  void test_buffer_search(Key k, Value & v, bool is_latest) {
    FilterPoolNode * filter = &filter_pool[0];
    if (is_latest) {
      skiplist_search_latest(k, v, filter);
    } else {
      skiplist_search(k, v, filter);
    }
  }

  void clear_TS(TS oldest_TS) {
    filter_pool.updateGlobalTS(oldest_TS);
  }

private:
  void skiplist_insert(SkipListNode *sln, size_t node_offset, FilterPoolNode *filter)
  {
    //random int level
    size_t cur_pos = filter->start_offset;
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
    auto next_pos = p->next_ptrs[kMaxLevel-1];
    //search and insert;
    for (int i = kMaxLevel - 1; i >= 0; i--)
    {
      while (next_pos != kKeyMax && buffer[next_pos].kvts.k < sln->kvts.k)
      {
        p = &buffer[next_pos];
        cur_pos = next_pos;
        next_pos = p->next_ptrs[i];
      } if (i < sln->level) {
        insert_buffer[i] = cur_pos;
      } if (i > 0) {
        next_pos = p->next_ptrs[i-1];
      }
    }
    for (int i = 0; i< sln->level;i++) {
      auto pre_node = &buffer[insert_buffer[i]];
      sln->next_ptrs[i] = pre_node->next_ptrs[i];
      pre_node->next_ptrs[i] = node_offset;
    }
    cutil::write_unlock(filter->skiplist_lock_);
  }

  inline void unlock_previous(size_t start_pos, int start_level, int level, uint8_t fliper) {
    for (int i = start_level; i< level; i++) {
      size_t pos = insert_buffer[i] - start_pos;
      cutil::write_unlock(Latest_lock_buffer[fliper][pos][i]);
    }
  }

  void skiplist_insert_latest(SkipListNode *sln, size_t node_offset, FilterPoolNode * filter) {
    size_t start_pos = filter->start_offset;
    size_t cur_pos = start_pos;
  restart:
    bool need_restart = false;
    auto skiplist_head = &buffer[filter->start_offset];
    auto cur_v = cutil::read_lock_or_restart(Latest_lock_buffer[filter->my_fliper][cur_pos - start_pos][kMaxLevel - 1], need_restart);
    if (need_restart) {
      goto restart;
    }
    auto p = skiplist_head;
    auto next_pos = p->next_ptrs[kMaxLevel - 1];
    for (int i = kMaxLevel - 1; i >= 0; i--)
    {
      while (next_pos != kKeyMax && buffer[next_pos].kvts.k < sln->kvts.k)
      {
        assert(next_pos - start_pos < kMaxSkipListData);
        p = &buffer[next_pos];
        auto f_v = cutil::read_lock_or_restart(Latest_lock_buffer[filter->my_fliper][next_pos - start_pos][i], need_restart);
        if (need_restart) {
          unlock_previous(start_pos, i + 1, sln->level, filter->my_fliper);
          goto restart;
        }
        cutil::read_unlock_or_restart(Latest_lock_buffer[filter->my_fliper][cur_pos - start_pos][i], cur_v, need_restart);
        if (need_restart) {
          unlock_previous(start_pos, i + 1, sln->level, filter->my_fliper);
          goto restart;
        }
        cur_pos = next_pos;
        next_pos = p->next_ptrs[i];
        cur_v = f_v;
      } 
      if (i < sln->level) {
        cutil::upgrade_to_write_lock_or_restart(Latest_lock_buffer[filter->my_fliper][cur_pos - start_pos][i], cur_v, need_restart);
        if (need_restart) {
          unlock_previous(start_pos, i+1, sln->level, filter->my_fliper);
          goto restart;
        }
        insert_buffer[i] = cur_pos;
      } if (i > 0) {
        //lock next level
        int sig = int(i >= sln->level);
        auto f_v = cutil::read_lock_or_restart(Latest_lock_buffer[filter->my_fliper][cur_pos - start_pos][i - 1], need_restart);
        if (need_restart) {
          unlock_previous(start_pos, i + sig, sln->level, filter->my_fliper);
          goto restart;
        }
        cutil::read_unlock_or_restart(Latest_lock_buffer[filter->my_fliper][cur_pos - start_pos][i], cur_v, need_restart);
        if (need_restart) {
          unlock_previous(start_pos, i + sig, sln->level, filter->my_fliper);
          goto restart;
        }
        cur_v = f_v;
        next_pos = p->next_ptrs[i - 1];
      }
    } 
    for (int i = 0; i < sln->level; i++) {
      auto pre_node = &buffer[insert_buffer[i]];
      sln->next_ptrs[i] = pre_node->next_ptrs[i];
      pre_node->next_ptrs[i] = node_offset;
      cutil::write_unlock(Latest_lock_buffer[filter->my_fliper][insert_buffer[i]][i]);
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
    auto next_pos = p->next_ptrs[kMaxLevel -1];
    for (int i = kMaxLevel - 1; i >= 0; i--)
    {
      while (next_pos != kKeyMax && buffer[next_pos].kvts.k < k)
      {
        p = &buffer[next_pos];
        next_pos = p->next_ptrs[i];
      } if (next_pos != kKeyMax && buffer[next_pos].kvts.k == k) {
        v = buffer[next_pos].kvts.v;
        cutil::read_unlock_or_restart(filter->skiplist_lock_, cur_v, need_restart);
        if (need_restart) {
          goto restart;
        }
        return true;
      } if ( i > 0) {
        next_pos = p->next_ptrs[i-1];
      }
    }
    cutil::read_unlock_or_restart(filter->skiplist_lock_, cur_v, need_restart);
    if (need_restart) {
      goto restart;
    }
    return false;
  }

  bool skiplist_search_latest(Key k, Value & v, FilterPoolNode * filter) {
    size_t start_pos = filter->start_offset;
    size_t cur_pos = start_pos;
    size_t cur_offset;
    if (buffer.check_distance(start_pos, cur_offset) < directFindBar) {
      for (size_t i = start_pos; i < cur_offset;i++) {
        if (buffer[i].kvts.k == k) {
          v = buffer[i].kvts.v;
          return true;
        }
      }
      return false;
    }
  restart:
    bool need_restart = false;
    auto skiplist_head = &buffer[filter->start_offset];
    auto cur_v = cutil::read_lock_or_restart(Latest_lock_buffer[filter->my_fliper][cur_pos - start_pos][kMaxLevel - 1], need_restart);
    if (need_restart) {
      goto restart;
    }
    auto p = skiplist_head;
    auto next_pos = p->next_ptrs[kMaxLevel - 1];
    for (int i = kMaxLevel - 1; i >= 0; i--)
    {
      while (next_pos != kKeyMax && buffer[next_pos].kvts.k < k)
      {
        p = &buffer[next_pos];
        auto f_v = cutil::read_lock_or_restart(Latest_lock_buffer[filter->my_fliper][next_pos - start_pos][i], need_restart);
        if (need_restart) {
          goto restart;
        }
        cutil::read_unlock_or_restart(Latest_lock_buffer[filter->my_fliper][cur_pos - start_pos][i], cur_v, need_restart);
        if (need_restart) {
          goto restart;
        }
        cur_pos = next_pos;
        next_pos = p->next_ptrs[i];
        cur_v = f_v;
      } if (next_pos != kKeyMax && buffer[next_pos].kvts.k == k) {
        v = buffer[next_pos].kvts.v;
        cutil::read_unlock_or_restart(Latest_lock_buffer[filter->my_fliper][cur_pos - start_pos][i], cur_v, need_restart);
        if (need_restart) {
          goto restart;
        }
        return true;
      } if (i > 0) {
        auto f_v = cutil::read_lock_or_restart(Latest_lock_buffer[filter->my_fliper][cur_pos - start_pos][i - 1], need_restart);
        if (need_restart) {
          goto restart;
        }
        cutil::read_unlock_or_restart(Latest_lock_buffer[filter->my_fliper][cur_pos - start_pos][i], cur_v, need_restart);
        if (need_restart) {
          goto restart;
        }
        cur_v = f_v;
        next_pos = p->next_ptrs[i - 1]; 
      }
    }
    return false;
  }

  int randomHeight() {
    auto rnd = Random::GetTLSInstance();

  // Increase height with probability 1 in kBranching
    int height = 1;
    while (height < kMaxLevel &&
          rnd->Next() < (Random::kMaxNext + 1) / 4) {
      height++;
    }
    assert(height > 0);
    assert(height <= kMaxLevel);
    return height;
  }

  size_t cache_size;
  MonotonicBufferRing<SkipListNode> buffer;
  std::atomic<cutil::ull_t> Latest_lock_buffer[2][kMaxSkipListData][kMaxLevel];
  FilterPool filter_pool;
  static thread_local size_t insert_buffer[kMaxLevel];
};