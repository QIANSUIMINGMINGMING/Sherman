#pragma once

#include <bitset>
#include <memory>

// Size of the bloom filter state in bits (2^16).
constexpr int bloomfilter_store_size = 65536 * 2;
constexpr int hash_fuction_count = 3;

// thread local hash buffer


class Bloomfilter {
public:
    Bloomfilter() {}
    inline void insert(uint64_t object) {
        hash(object);
        for (size_t i = 0; i < hash_fuction_count; i++) {
            bloomfilter_store_[hash_result_buffer[i] % bloomfilter_store_size] = true;
        }
    }
    inline bool contains(uint64_t object) const {
        hash(object);
        for (size_t i = 0; i < hash_fuction_count; i++) {
            if (!bloomfilter_store_[hash_result_buffer[i] % bloomfilter_store_size]) {
                return false;
            }
        }
        return true;
    }

    inline bool contains() const {
        for (size_t i = 0; i < hash_fuction_count; i++) {
            if (!bloomfilter_store_[hash_result_buffer[i] % bloomfilter_store_size]) {
                return false;
            }
        }
        return true; 
    } 
    static inline void hash(uint64_t key) {
        hash_result_buffer[0] = key;
        for (size_t i = 0; i < hash_fuction_count; i++) {
            hash_result_buffer[i + 1] = (hash_result_buffer[i] >> 32) ^ hash_result_buffer[i];
        }
    }

    inline void clear() {
        bloomfilter_store_.reset();
    }

private:
    std::bitset<bloomfilter_store_size> bloomfilter_store_;
    static thread_local uint64_t hash_result_buffer[hash_fuction_count + 1];                                                                                  
};