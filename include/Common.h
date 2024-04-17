#ifndef __COMMON_H__
#define __COMMON_H__

#include <cassert>
#include <cstdint>
#include <cstdlib>
#include <cstring>

#include <atomic>
#include <bitset>
#include <limits>
#include <cmath>
#include "Debug.h"
#include "HugePageAlloc.h"
#include "Rdma.h"

#include "WRLock.h"

#define NUMA_1_CPU_s1 24
#define NUMA_1_CPU_s2 72

// CONFIG_ENABLE_EMBEDDING_LOCK and CONFIG_ENABLE_CRC
// **cannot** be ON at the same time

// #define CONFIG_ENABLE_EMBEDDING_LOCK
// #define CONFIG_ENABLE_CRC

#define LATENCY_WINDOWS 1000000

#define STRUCT_OFFSET(type, field)                                             \
  (char *)&((type *)(0))->field - (char *)((type *)(0))

#define MAX_COMP 6
#define MAX_MEMORY 2
#define MAX_MACHINE 8

#define ADD_ROUND(x, n) ((x) = ((x) + 1) % (n))

#define MESSAGE_SIZE 96 // byte

#define POST_RECV_PER_RC_QP 128

#define RAW_RECV_CQ_COUNT 128

// { app thread
#define MAX_APP_THREAD 96

#define APP_MESSAGE_NR 96

// }

// { dir thread
#define NR_DIRECTORY 1

#define DIR_MESSAGE_NR 128
// }

void bindCore(uint16_t core);
char *getIP();
char *getMac();

inline int bits_in(std::uint64_t u) {
  auto bs = std::bitset<64>(u);
  return bs.count();
}

#include <boost/coroutine/all.hpp>

using CoroYield = boost::coroutines::symmetric_coroutine<void>::yield_type;
using CoroCall = boost::coroutines::symmetric_coroutine<void>::call_type;

struct CoroContext {
  CoroYield *yield;
  CoroCall *master;
  int coro_id;
};

namespace define {

constexpr uint64_t MB = 1024ull * 1024;
constexpr uint64_t GB = 1024ull * MB;
constexpr uint16_t kCacheLineSize = 64;

// for remote allocate
constexpr uint64_t kChunkSize = 32 * MB;

// for store root pointer
constexpr uint64_t kRootPointerStoreOffest = kChunkSize / 2;
static_assert(kRootPointerStoreOffest % sizeof(uint64_t) == 0, "XX");

// lock on-chip memory
constexpr uint64_t kLockStartAddr = 0;
constexpr uint64_t kLockChipMemSize = 256 * 1024;

// number of locks
// we do not use 16-bit locks, since 64-bit locks can provide enough concurrency.
// if you want to use 16-bit locks, call *cas_dm_mask*
constexpr uint64_t kNumOfLock = kLockChipMemSize / sizeof(uint64_t);

// level of tree
constexpr uint64_t kMaxLevelOfTree = 7;

constexpr uint16_t kMaxCoro = 8;
constexpr int64_t kPerCoroRdmaBuf = 128 * 1024;

constexpr uint8_t kMaxHandOverTime = 8;

constexpr int kIndexCacheSize = 1000; // MB
} // namespace define

static inline unsigned long long asm_rdtsc(void) {
  unsigned hi, lo;
  __asm__ __volatile__("rdtsc" : "=a"(lo), "=d"(hi));
  return ((unsigned long long)lo) | (((unsigned long long)hi) << 32);
}

// For Tree
using Key = uint64_t;
using Value = uint64_t;
using TS = uint64_t;

constexpr Key kKeyMin = std::numeric_limits<Key>::min();
constexpr Key kKeyMax = std::numeric_limits<Key>::max();

constexpr TS kTSMax = std::numeric_limits<TS>::max();
constexpr TS kTSMin = std::numeric_limits<TS>::min();
constexpr Value kValueNull = 0;

// Note: our RNICs can read 1KB data in increasing address order (but not for 4KB)
constexpr uint32_t kInternalPageSize = 1024;
constexpr uint32_t kLeafPageSize = 1024;
constexpr uint32_t kMcPageSize = 1024;

// for core binding
constexpr uint16_t mcCmaCore = 95;
constexpr uint16_t filterCore = 94;
constexpr uint16_t rpcCore = 93;
constexpr uint16_t kMaxRpcCoreNum = 8;

//for Rpc
constexpr int kMcMaxPostList = 128;
constexpr int kpostlist = 32;

__inline__ unsigned long long rdtsc(void) {
  unsigned hi, lo;
  __asm__ __volatile__("rdtsc" : "=a"(lo), "=d"(hi));
  return ((unsigned long long)lo) | (((unsigned long long)hi) << 32);
}

inline void mfence() { asm volatile("mfence" ::: "memory"); }

inline void compiler_barrier() { asm volatile("" ::: "memory"); }

constexpr bool is_prime(int n) {
    if (n <= 1)
        return false;
    if (n <= 3)
        return true;
    if (n % 2 == 0 || n % 3 == 0)
        return false;
    for (int i = 5; i * i <= n; i += 6) {
        if (n % i == 0 || n % (i + 2) == 0)
            return false;
    }
    return true;
}

constexpr int closest_prime(int num) {
    if (is_prime(num))
        return num;
    
    int lower_prime = num - 1;
    while (!is_prime(lower_prime))
        lower_prime--;

    int upper_prime = num + 1;
    while (!is_prime(upper_prime))
        upper_prime++;

    return (num - lower_prime < upper_prime - num) ? lower_prime : upper_prime;
}

template <typename T>
class HugePages
{
   T* memory;
   size_t size; // in bytes
   size_t highWaterMark;  // max index
  public:
   HugePages(size_t size) : size(size)
   {
      void* p = mmap(NULL, size, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS | MAP_HUGETLB, -1, 0);
      if (p == MAP_FAILED)
         throw std::runtime_error("mallocHugePages failed");
      memory = static_cast<T*>(p);
      highWaterMark = (size / sizeof(T));
   }

   size_t get_size(){
      return highWaterMark;
   }
   
   inline operator T*() { return memory; }

   
   inline T& operator[](size_t index) const
   {
      return memory[index];
   }
   ~HugePages() { munmap(memory, size); }
};

struct myClock
{
    using duration   = std::chrono::nanoseconds;
    using rep        = duration::rep;
    using period     = duration::period;
    static constexpr bool is_steady = false;

    static uint64_t get_ts()
    {
        timespec ts;
        if (clock_gettime(CLOCK_REALTIME, &ts))
            throw 1;
        uint64_t timestamp = (uint64_t)(ts.tv_sec * 1000000000) + (uint64_t)(ts.tv_nsec); 
        return timestamp;
    }
};

// olc lock
namespace cutil{
using ull_t = unsigned long long;

inline bool is_locked(ull_t version) {
  return ((version & 0b10) == 0b10);
}
inline bool is_obsolete(ull_t version) {
  return ((version & 1) == 1);
}

// the following API may be reimplemented in node.cuh
inline cutil::ull_t read_lock_or_restart(
    const std::atomic<cutil::ull_t> &version_lock_obsolete, bool &need_restart) {
  cutil::ull_t version = version_lock_obsolete.load();
  if (cutil::is_locked(version) || cutil::is_obsolete(version)) {
    need_restart = true;
  }
  return version;
}

inline void read_unlock_or_restart(
    const std::atomic<cutil::ull_t> &version_lock_obsolete, cutil::ull_t start_read,
    bool &need_restart) {
  // TODO: should we use spinlock to await?
  need_restart = (start_read != version_lock_obsolete.load());
}

inline  void check_or_restart(
    const std::atomic<cutil::ull_t> &version_lock_obsolete, cutil::ull_t start_read,
    bool &need_restart) {
  read_unlock_or_restart(version_lock_obsolete, start_read, need_restart);
}

inline void upgrade_to_write_lock_or_restart(
    std::atomic<cutil::ull_t> &version_lock_obsolete, cutil::ull_t &version,
    bool &need_restart) {
  // if (version == atomicCAS(&version_lock_obsolete, version, version + 0b10)) {
  bool success = version_lock_obsolete.compare_exchange_strong(version, version + 0b10);
  if (success) {
    version = version + 0b10;
  } else {
    need_restart = true;
  }
}

inline void write_unlock(
    std::atomic<cutil::ull_t> &version_lock_obsolete) {
  version_lock_obsolete.fetch_add(0b10);
}

inline void write_unlock(
    std::atomic<cutil::ull_t> * version_lock_obsolete) {
  version_lock_obsolete->fetch_add(0b10);
}

inline void write_unlock_obsolete(
    std::atomic<cutil::ull_t> &version_lock_obsolete) {
  version_lock_obsolete.fetch_add(0b11);
}
}

#endif /* __COMMON_H__ */
