#include "Filter.h"

thread_local uint64_t Bloomfilter::hash_result_buffer[hash_fuction_count + 1];