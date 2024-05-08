#include "KVCache.h"
#include "randomUtil/ScrambledZipfGenerator.hpp"
#include <iostream>

uint64_t kKeySpace = 64 * define::MB;
double kWarmRatio = 0.1;

KVCache *cache;
MonotonicBufferRing<SkipListNode> *buffer;
FilterPool *fp;

std::atomic<int64_t> warmup_cnt{0};

void thread_run(int id) {

  int core_id;
  if (id < 24) {
    core_id = id + 24;
  } else {
    core_id = id - 24 + 72;
  }
  bindCore(core_id);

  printf("I am thread %d on compute nodes\n", id);

  uint64_t end_warm_key = kWarmRatio * kKeySpace;
  for (uint64_t i = 1; i < end_warm_key; ++i) {
    
    if (i % kMaxRwCoreNum == (uint64_t) id) {
      cache->insert(i, myClock::get_ts(), end_warm_key - i);
    }
  }
  warmup_cnt.fetch_add(1);
  while (warmup_cnt.load() != kMaxRwCoreNum){}

  for (uint64_t i = 1; i < end_warm_key; ++i) {
    if ((i+1) % kMaxRwCoreNum == (uint64_t) id) {      
      Value v;
      cache->search(i, v);
      assert(v == end_warm_key - i);
      // printf("expected v: %lu, actual v: %lu\n", end_warm_key - i, v);
    }
  }
}

void test_buffer_thread(int id) {
  int core_id;
  if (id < 24) {
    core_id = id + 24;
  } else {
    core_id = id - 24 + 72;
  }
  bindCore(core_id);
  bool il = true;

  printf("I am thread %d on compute nodes\n", id);

  uint64_t end_warm_key = kWarmRatio * kKeySpace;
  for (uint64_t i = 1; i < 1000; ++i) {
    
    if (i % kMaxRwCoreNum == (uint64_t)id) {
      cache->test_buffer_insert(i, 0, end_warm_key - i, il);
    }
  }

  warmup_cnt.fetch_add(1);
  while (warmup_cnt.load() != kMaxRwCoreNum){}

  // for (size_t i =0 ;i< 300; ++i) {
  // }
  for (uint64_t i = 1; i < 1000; ++i) {
    if ((i+1) % kMaxRwCoreNum == (uint64_t)id) {      
      Value v;
      cache->test_buffer_search(i, v, il);
      assert(v == end_warm_key - i);
      // printf("expected v: %lu, actual v: %lu\n", end_warm_key - i, v);
    }
  }

  printf("test end\n");
}

int main() {
  int buffer_size = (int)(((uint64_t(1) << (uint64_t)(32))) / sizeof(SkipListNode));  // 4GB
  std::thread th[kMaxRwCoreNum];
  //Create First FPN
  cache = new KVCache(buffer_size);
  for (int i = 0; i < kMaxRwCoreNum; i++) {
    th[i] = std::thread(thread_run, i);
  }

  for (int i = 0; i < kMaxRwCoreNum; i++) {
    th[i].join();
  }

  while (true) {
    
  }

  return 0;
}