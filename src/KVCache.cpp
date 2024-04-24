#include "KVCache.h"

thread_local size_t KVCache::insert_buffer[kMaxLevel];