#pragma once

#include <iostream>

#include "Common.h"
#include "GlobalAddress.h"
#include "KVCache.h"
#include "IndexCache.h"
#include "Directory.h"

#include <boost/unordered/unordered_map.hpp>

namespace forest {

struct BSearchResult {
  bool is_leaf;
  uint8_t level;
  GlobalAddress slibing;
  GlobalAddress next_level;
  Value val;
  bool corrupted = false;
};

class BHeader {
private:
  GlobalAddress parent_ptr;
  GlobalAddress leftmost_ptr;
  GlobalAddress sibling_ptr;
  uint8_t level;
  int16_t last_index;
  Key lowest;
  Key highest;
  uint8_t invalidate;

  friend class BInternalPage;
  friend class BLeafPage;
  friend class BForest;
  friend class IndexCache;

public:
  BHeader() {
    leftmost_ptr = GlobalAddress::Null();
    sibling_ptr = GlobalAddress::Null();
    last_index = -1;
    lowest = kKeyMin;
    highest = kKeyMax;
  }

  void debug() const {
    std::cout << "leftmost=" << leftmost_ptr << ", "
              << "sibling=" << sibling_ptr << ", "
              << "level=" << (int)level << ","
              << "cnt=" << last_index + 1 << ","
              << "range=[" << lowest << " - " << highest << "]";
  }
} __attribute__((packed));
;

class BInternalEntry {
public:
  Key key;
  GlobalAddress ptr;

  BInternalEntry() {
    ptr = GlobalAddress::Null();
    key = 0;
  }
} __attribute__((packed));

class BLeafEntry {
public:
  // TODO: modify to checksum later
  uint8_t f_version : 4;
  Key key;
  Value value;
  uint8_t r_version : 4;

  BLeafEntry() {
    f_version = 0;
    r_version = 0;
    value = kValueNull;
    key = 0;
  }
} __attribute__((packed));

constexpr int kBInternalCardinality = (kInternalPageSize - sizeof(BHeader)) / sizeof(BInternalEntry);

constexpr int kBLeafCardinality =
    (kLeafPageSize - sizeof(BHeader)) /
    sizeof(BLeafEntry);

class BInternalPage {
private:
  BHeader hdr;
  BInternalEntry records[kInternalCardinality];

  friend class BForest;
  friend class IndexCache;

public:
  // this is called when tree grows
  BInternalPage(GlobalAddress left, const Key &key, GlobalAddress right,
               uint32_t level = 0) {
    hdr.leftmost_ptr = left;
    hdr.level = level;
    records[0].key = key;
    records[0].ptr = right;
    records[1].ptr = GlobalAddress::Null();

    hdr.last_index = 0;
  }

  BInternalPage(uint32_t level = 0) {
    hdr.level = level;
    records[0].ptr = GlobalAddress::Null();
  }
  void debug() const {
    std::cout << "InternalPage@ ";
    hdr.debug();
  }

  void verbose_debug() const {
    this->debug();
    for (int i = 0; i < this->hdr.last_index + 1; ++i) {
      printf("[%lu %lu] ", this->records[i].key, this->records[i].ptr.val);
    }
    printf("\n");
  }

} __attribute__((packed));

class BLeafPage {
private:
  BHeader hdr;
  BLeafEntry records[kLeafCardinality];

  friend class BForest;
public:
  BLeafPage(uint32_t level = 0) {
    hdr.level = level;
    records[0].value = kValueNull;
  }

  void debug() const {
    std::cout << "LeafPage@ ";
    hdr.debug();
  }

} __attribute__((packed));


class BForest {

public:
  BForest(DSM *dsm, int CNs, uint16_t tree_id);

  bool search(const Key &k, Value &v, CoroContext *cxt = nullptr, int coro_id = 0);

  void batch_insert(KVTS *kvs, int cnt, CoroContext *cxt = nullptr,
                    int coro_id = 0);

private:
  DSM *dsm;
  int tree_num;
  uint16_t tree_id;
  GlobalAddress root_ptr_ptr[MAX_COMP];
  IndexCache *indexCaches[MAX_COMP];
  int cache_sizes[MAX_COMP];
  std::atomic<int> cur_cache_sizes[MAX_COMP];
  GlobalAddress allocator_starts[MAX_MEMORY];

  BInternalPage stack_page_buffer[define::kMaxLevelOfTree];
  

  // thread_local static stack

  // struct node_meta {
  //   BHeader hdr;
  //   int start;
  //   int end
  // };
  
  boost::unordered_map<GlobalAddress, int> tree_meta;
  struct internal_split_page {
    GlobalAddress ptr;
    InternalPage *new_pages;
    int num;
  };

  struct leaf_split_page {
    
  };

private:
  GlobalAddress get_root_ptr_ptr(uint16_t id); 
  GlobalAddress get_root_ptr(CoroContext *cxt, int coro_id, uint16_t id);

  void set_stack_buffer(int level, const Key &k);

  void set_leaf(BLeafPage *page, const Key &k, const Value &v, bool & need_split);
  void set_internal(BInternalPage *page, const Key &k, GlobalAddress v, bool & need_split);

  bool page_search(GlobalAddress page_addr, const Key &k, BSearchResult &result, Key expect_lowest, Key expect_highest, uint8_t expect_level, 
                   CoroContext *cxt, int coro_id, bool from_cache = false);

  void internal_page_search(BInternalPage *page, const Key &k, BSearchResult &result);

  void leaf_page_search(BLeafPage *page, const Key &k, BSearchResult &result);

  bool check_ga(GlobalAddress ga);
};
}; 
// namespace forest
