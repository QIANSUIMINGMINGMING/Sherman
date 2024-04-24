#include "KVCache.h"
#include "randomUtil/ScrambledZipfGenerator.hpp"
#include <iostream>

uint64_t kKeySpace = 64 * define::MB;
double kWarmRatio = 0.8;

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
    
    // if (i % kMaxRwCoreNum == id) {
    //   cache->insert();
    // }
  }

//   warmup_cnt.fetch_add(1);

//   if (id == 0) {
//     while (warmup_cnt.load() != kThreadCount)
//       ;
//     printf("node %d finish\n", dsm->getMyNodeID());
//     // dsm->barrier("warm_finish");
//     dsm->barrier("warm_finish", BarrierType::COMPUTE);

//     uint64_t ns = bench_timer.end();
//     printf("warmup time %lds\n", ns / 1000 / 1000 / 1000);

//     tree->index_cache_statistics();
//     tree->clear_statistics();

//     ready = true;

//     warmup_cnt.store(0);
//   }

//   while (warmup_cnt.load() != 0)
//     ;

// #ifdef USE_CORO
//   tree->run_coroutine(coro_func, id, kCoroCnt);
// #else

//   /// without coro
//   unsigned int seed = rdtsc();
//   struct zipf_gen_state state;
//   mehcached_zipf_init(&state, kKeySpace, zipfan,
//                       (rdtsc() & (0x0000ffffffffffffull)) ^ id);

//   Timer timer;
//   while (true) {

//     uint64_t dis = mehcached_zipf_next(&state);
//     uint64_t key = to_key(dis);

//     Value v;
//     timer.begin();

//     if (rand_r(&seed) % 100 < kReadRatio) { // GET
//       tree->search(key, v);
//     } else {
//       v = 12;
//       tree->insert(key, v);
//     }

//     auto us_10 = timer.end() / 100;
//     if (us_10 >= LATENCY_WINDOWS) {
//       us_10 = LATENCY_WINDOWS - 1;
//     }
//     latency[id][us_10]++;

//     tp[id][0]++;
//   }
// #endif

}

void test_buffer_thread(int id) {
  int core_id;
  if (id < 24) {
    core_id = id + 24;
  } else {
    core_id = id - 24 + 72;
  }
  bindCore(core_id);
  bool il = false;

  printf("I am thread %d on compute nodes\n", id);

  uint64_t end_warm_key = kWarmRatio * kKeySpace;
  for (uint64_t i = 1; i < 300; ++i) {
    
    if (i % kMaxRwCoreNum == (uint64_t)id) {
      cache->test_buffer_insert(i, 0, end_warm_key - i, il);
    }
  }

  warmup_cnt.fetch_add(1);
  while (warmup_cnt.load() != kMaxRwCoreNum){}

  for (uint64_t i = 1; i < end_warm_key; ++i) {
    if ((i+1) % kMaxRwCoreNum == (uint64_t)id) {      
      Value v;
      cache->test_buffer_search(i, v, il);
      // assert(v == end_warm_key - i);
      printf("expected v: %lu, actual v: %lu\n", end_warm_key - i, v);
    }
  }

  printf("test end\n");
}

int main() {
  int buffer_size = (int)(((uint64_t(1) << (uint64_t)(32))) / sizeof(SkipListNode));  // 4GB
  std::thread th[kMaxRwCoreNum];
  //Create First FPN
  cache = new KVCache(buffer_size, true);
  for (int i = 0; i < kMaxRwCoreNum; i++) {
    th[i] = std::thread(test_buffer_thread, i);
  }

  for (int i = 0; i < kMaxRwCoreNum; i++) {
    th[i].join();
  }
  

  return 0;
}