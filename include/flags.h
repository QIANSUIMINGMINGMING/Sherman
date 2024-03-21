#pragma once
#include<gflags/gflags.h>

using u64 = uint64_t;
using s32 = int32_t;

DECLARE_double(dramGB);

DECLARE_uint32(port);
DECLARE_uint32(mcport);

DECLARE_string(ownIp);

DECLARE_string(mcIp);
DECLARE_uint32(mcGroups);

DECLARE_double(rdmaMemoryFactor); // factor to be multiplied by dramGB
DECLARE_bool(storageNode);
DECLARE_uint64(storageNodes);

DECLARE_uint64(computeNodes);
DECLARE_uint64(worker);

DECLARE_string(memcachedIp);
DECLARE_uint32(memcachedPort);

DECLARE_bool(testmachineOn);

DECLARE_uint32(nodeId);
DECLARE_uint64(all_worker);

DECLARE_bool(mcIsSender);

DECLARE_int32(internalPageSize);
DECLARE_int32(leafPageSize);

DECLARE_int32(KVCacheSize);