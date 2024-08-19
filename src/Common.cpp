#include "Common.h"

#include <arpa/inet.h>
#include <net/if.h>
#include <netinet/in.h>
#include <sys/ioctl.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>

void bindCore(uint16_t core) {
  cpu_set_t cpuset;
  CPU_ZERO(&cpuset);
  CPU_SET(core, &cpuset);
  int rc = pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset);
  if (rc != 0) {
    Debug::notifyError("can't bind core!");
  }
}

char *getIP() {
  struct ifreq ifr;
  int fd = socket(AF_INET, SOCK_DGRAM, 0);

  ifr.ifr_addr.sa_family = AF_INET;
  strncpy(ifr.ifr_name, "ib0", IFNAMSIZ - 1);

  ioctl(fd, SIOCGIFADDR, &ifr);
  close(fd);

  return inet_ntoa(((struct sockaddr_in *)&ifr.ifr_addr)->sin_addr);
}

char *getMac() {
  static struct ifreq ifr;
  int fd = socket(AF_INET, SOCK_DGRAM, 0);

  ifr.ifr_addr.sa_family = AF_INET;
  strncpy(ifr.ifr_name, "ens2", IFNAMSIZ - 1);

  ioctl(fd, SIOCGIFHWADDR, &ifr);
  close(fd);

  return (char *)ifr.ifr_hwaddr.sa_data;
}

namespace memcached_util {

std::string trim(const std::string &s) {
  std::string res = s;
  if (!res.empty()) {
    res.erase(0, res.find_first_not_of(" "));
    res.erase(res.find_last_not_of(" ") + 1);
  }
  return res;
}

void memcached_Connect(memcached_st *&memc) {
  memcached_server_st *servers = NULL;
  memcached_return rc;

  std::string addr = SERVER_ADDR;
  std::string port = SERVER_PORT;

  memc = memcached_create(NULL);
  servers = memcached_server_list_append(servers, trim(addr).c_str(),
                                         std::stoi(trim(port)), &rc);
  rc = memcached_server_push(memc, servers);

  if (rc != MEMCACHED_SUCCESS) {
    fprintf(stderr, "Counld't add server:%s\n", memcached_strerror(memc, rc));
    sleep(1);
  }

  memcached_behavior_set(memc, MEMCACHED_BEHAVIOR_BINARY_PROTOCOL, 1);
}

void memcachedSet(struct memcached_st *memc, const char *key, uint32_t klen,
                  const char *val, uint32_t vlen) {
  memcached_return rc;
  while (true) {
    rc = memcached_set(memc, key, klen, val, vlen, (time_t)0, (uint32_t)0);
    if (rc == MEMCACHED_SUCCESS) {
      break;
    }
    usleep(400);
  }
}

char *memcachedGet(struct memcached_st *memc, const char *key, uint32_t klen,
                   size_t *v_size) {
  size_t l;
  char *res;
  uint32_t flags;
  memcached_return rc;

  while (true) {
    res = memcached_get(memc, key, klen, &l, &flags, &rc);
    if (rc == MEMCACHED_SUCCESS) {
      break;
    }
    usleep(400 * FLAGS_cnodeId);
  }

  if (v_size != nullptr) {
    *v_size = l;
  }

  return res;
}

uint64_t memcachedFetchAndAdd(struct memcached_st *memc, const char *key,
                              uint32_t klen) {
  uint64_t res;
  while (true) {
    memcached_return rc = memcached_increment(memc, key, klen, 1, &res);
    if (rc == MEMCACHED_SUCCESS) {
      return res;
    }
    usleep(10000);
  }
}

void memcached_barrier(struct memcached_st *memc, const std::string &barrierKey,
                       uint64_t num_server) {
  std::string key = std::string("barrier-") + barrierKey;
  if (FLAGS_cnodeId == 0) {
    memcachedSet(memc, key.c_str(), key.size(), "0", 1);
  }
  memcachedFetchAndAdd(memc, key.c_str(), key.size());
  while (true) {
    uint64_t v = std::stoull(memcachedGet(memc, key.c_str(), key.size()));
    if (v == num_server) {
      return;
    }
  }
}
};  // namespace memcached_util