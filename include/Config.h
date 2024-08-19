#ifndef __CONFIG_H__
#define __CONFIG_H__

#include "Common.h"

class CacheConfig {
 public:
  uint32_t cacheSize;

  CacheConfig(uint32_t cacheSize = 1) : cacheSize(cacheSize) {}
};

class DSMConfig {
 public:
  CacheConfig cacheConfig;
  // uint32_t machineNR;

  uint32_t computeNR;
  uint32_t memoryNR;
  uint32_t machineNR;
  uint64_t dsmSize;  // G

  // DSMConfig(const CacheConfig &cacheConfig = CacheConfig(),
  //           uint32_t machineNR = 2, uint64_t dsmSize = 8)
  //     : cacheConfig(cacheConfig), machineNR(machineNR), dsmSize(dsmSize) {}

  DSMConfig(const CacheConfig &cacheConfig = CacheConfig(),
            uint32_t computeNR = 1, uint32_t memoryNR = 1, uint64_t dsmSize = 8)
      : cacheConfig(cacheConfig),
        computeNR(computeNR),
        memoryNR(memoryNR),
        dsmSize(dsmSize) {
    machineNR = computeNR + memoryNR;
  }
};

#endif /* __CONFIG_H__ */
