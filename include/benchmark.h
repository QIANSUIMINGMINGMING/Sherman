#pragma once
#include "Common.h"
#include "randomUtil/RandomGenerator.hpp"
#include "randomUtil/ScrambledZipfGenerator.hpp"
#include "randomUtil/ZipfGenerator.hpp"

#include <city.h>
#include <zipf.h>

namespace bench
{

uint64_t kKeySpace = 64 * define::MB;

struct Request {
  bool is_search;
  Key k;
  Value v;
};

class RequstGen {
public:
  RequstGen() = default;
  virtual Request next() { return Request{}; }
};

inline Key to_key(uint64_t k) {
  return (CityHash64((char *)&k, sizeof(k)) + 1) % kKeySpace;
}

class RequsetGenBench : public RequstGen {

public:
  RequsetGenBench(int id, int zipfan = 0, int readRatio = 50): id(id), zipfan(zipfan), readRatio(readRatio) {
    seed = rdtsc();
    mehcached_zipf_init(&state, kKeySpace, zipfan,
                        (rdtsc() & (0x0000ffffffffffffull)) ^ id);
  }

  Request next() override {
    Request r;
    uint64_t dis = mehcached_zipf_next(&state);

    r.k = to_key(dis);
    r.v = 23;
    r.is_search = rand_r(&seed) % 100 < readRatio;

    return r;
  }

private:
  int id;
  unsigned int seed;
  double zipfan;
  int readRatio;
  struct zipf_gen_state state;
};

class Metrics {

public:
void cal_latency() {
  uint64_t all_lat = 0;
  for (int i = 0; i < LATENCY_WINDOWS; ++i) {
    latency_th_all[i] = 0;
    for (int k = 0; k < MAX_APP_THREAD; ++k) {
      latency_th_all[i] += latency[k][i];
    }
    all_lat += latency_th_all[i];
  }

  uint64_t th50 = all_lat / 2;
  uint64_t th90 = all_lat * 9 / 10;
  uint64_t th95 = all_lat * 95 / 100;
  uint64_t th99 = all_lat * 99 / 100;
  uint64_t th999 = all_lat * 999 / 1000;

  uint64_t cum = 0;
  for (int i = 0; i < LATENCY_WINDOWS; ++i) {
    cum += latency_th_all[i];

    if (cum >= th50) {
      printf("p50 %f\t", i / 10.0);
      th50 = -1;
    }
    if (cum >= th90) {
      printf("p90 %f\t", i / 10.0);
      th90 = -1;
    }
    if (cum >= th95) {
      printf("p95 %f\t", i / 10.0);
      th95 = -1;
    }
    if (cum >= th99) {
      printf("p99 %f\t", i / 10.0);
      th99 = -1;
    }
    if (cum >= th999) {
      printf("p999 %f\n", i / 10.0);
      th999 = -1;
      return;
    }
  }
}

void record_latency(int tid, uint64_t us_10) {
  if (us_10 >= LATENCY_WINDOWS) {
    us_10 = LATENCY_WINDOWS - 1;
  }
  latency[tid][us_10]++;
}

private:
  int thread_num;
  uint64_t latency[MAX_APP_THREAD][LATENCY_WINDOWS];
  uint64_t latency_th_all[LATENCY_WINDOWS];
};


} // namespace bench
