#ifndef __KEEPER__H__
#define __KEEPER__H__

#include <assert.h>
#include <infiniband/verbs.h>
#include <libmemcached/memcached.h>
#include <stdint.h>
#include <stdlib.h>
#include <time.h>
#include <unistd.h>

#include <functional>
#include <string>
#include <thread>

#include "Config.h"
#include "Debug.h"
#include "Rdma.h"

class Keeper {
 private:
  static const char *SERVER_NUM_KEY;
  static const char *MEMORY_NUM_KEY;
  static const char *COMP_NUM_KEY;

  uint32_t maxServer;
  uint16_t curServer;

  uint32_t maxMem;
  uint16_t curMem;

  uint32_t maxComp;
  uint16_t curComp;

  uint16_t myNodeID;
  std::string myIP;
  uint16_t myPort;

  memcached_st *memc;

 protected:
  bool connectMemcached();
  bool disconnectMemcached();
  void serverConnect();
  void serverEnter();
  virtual bool connectNode(uint16_t remoteID) = 0;

 public:
  Keeper(uint32_t maxServer = 12, uint32_t maxMem = 6, uint32_t maxComp = 6);
  ~Keeper();

  uint16_t getMyNodeID() const { return this->myNodeID; }
  uint16_t getServerNR() const { return this->maxServer; }
  uint16_t getCompNR() const { return this->maxComp; }
  uint16_t getMemNR() const { return this->maxMem; }

  uint16_t getMyPort() const { return this->myPort; }

  std::string getMyIP() const { return this->myIP; }

  void memSet(const char *key, uint32_t klen, const char *val, uint32_t vlen);
  char *memGet(const char *key, uint32_t klen, size_t *v_size = nullptr);
  uint64_t memFetchAndAdd(const char *key, uint32_t klen);
};

#endif
