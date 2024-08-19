#pragma once

#include <cstdint>
#include <iostream>
#include <vector>

#include "Common.h"
#include "Tree.h"

struct KeyValuePair {
  uint64_t key;
  int value;
  KeyValuePair(uint64_t k, int v) : key(k), value(v) {}
};

// Define a structure for a hash table
class HashTable {
 private:
  constexpr static int TABLE_SIZE =
      closest_prime(int(kLeafCardinality / 0.7));  // Chosen prime number
  std::vector<KeyValuePair> table;

  // Hash function
  int hash(uint64_t key) const {
    const double A = 0.6180339887;
    return static_cast<int>(TABLE_SIZE *
                            (key * A - static_cast<uint64_t>(key * A)));
  }

  // Probe function (linear probing)
  int probe(int index, int attempt) const {
    return (index + attempt) % TABLE_SIZE;
  }

 public:
  // Constructor
  HashTable() : table(TABLE_SIZE, KeyValuePair(0, -1)) {}

  // Insert a key-value pair into the hash table
  void insert(uint64_t key, int value) {
    int index = hash(key);
    int attempt = 0;
    while (table[index].key != 0 && table[index].key != key) {
      // Collision: Linear probing
      index = probe(index, ++attempt);
    }
    table[index] = KeyValuePair(key, value);
  }

  // Retrieve a value from the hash table
  int retrieve(uint64_t key) const {
    int index = hash(key);
    int attempt = 0;
    while (table[index].key != key && table[index].key != 0) {
      // Collision: Linear probing
      index = probe(index, ++attempt);
    }
    if (table[index].key == key) {
      return table[index].value;
    }
    return -1;  // Key not found
  }

  // clear the memory with memset
  void clear() { memset(table.data(), 0, sizeof(KeyValuePair) * TABLE_SIZE); }
};