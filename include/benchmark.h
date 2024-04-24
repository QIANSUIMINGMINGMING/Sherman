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

} // namespace bench
