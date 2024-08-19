#include "flags.h"

#include "gflags/gflags.h"
DEFINE_double(
    rdmaMemoryFactor, 1.1,
    "Factor to be multiplied by dramGB");  // factor to be multiplied by dramGB
DEFINE_uint32(port, 20886, "port");
DEFINE_uint32(mcport, 20887, "mcport");

DEFINE_string(ownIp, "172.18.94.80", "own IP server");
DEFINE_string(mcIp, "226.0.0.1", "multicast group IP");
DEFINE_uint32(mcGroups, 10, "multicast group number");

DEFINE_bool(storageNode, false, "storage node");
DEFINE_uint64(storageNodes, 1, "Number storage nodes participating");

DEFINE_double(dramGB, 1, "DRAM buffer pool size");

DEFINE_uint64(computeNodes, 2, "Number compute nodes participating");

DEFINE_uint64(worker, 1, "Number worker threads");

DEFINE_string(memcachedIp, "10.16.70.16", "memcached server Ip");
DEFINE_uint32(memcachedPort, 2378, "memcached server port");

DEFINE_bool(testmachineOn, false, "leafcache or not on test machine");

DEFINE_uint32(nodeId, 0, "");
DEFINE_uint64(cnodeId, 0, "node id for compute nodes");
DEFINE_uint64(all_worker, 1,
              "number of all worker threads in the cluster for barrier");

DEFINE_bool(mcIsSender, false, "cmIsSender");

DEFINE_int32(internalPageSize, 1024, "internal page size");
DEFINE_int32(leafPageSize, 1024, "leaf page size");

DEFINE_int32(KVCacheSize, 1024 * 1024 * 256, "KV Cache Size B");

DEFINE_bool(usingStringKey, false, "using string keys");