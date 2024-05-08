#include "Forest.h"
#include "IndexCache.h"
#include "RdmaBuffer.h"

#include <algorithm>
#include <city.h>
#include <iostream>
#include <queue>
#include <utility>
#include <vector>

namespace forest {

BForest::BForest(DSM *dsm, int CNs, uint16_t tree_id): dsm(dsm), tree_num(CNs), tree_id(tree_id)  {

  // for (int i = 0; i < tree_num; i++) {
  //   root_ptr_ptr[i] = GlobalAddress::Null();
  // }
  assert(dsm->is_register());
  // print_verbose();

  for (int i = 0; i < tree_num; i++) {
    indexCaches[i] = new IndexCache(define::kCacheLineSize / tree_num);
  }

  for (uint16_t i = 0; i< CNs; i++) {
    root_ptr_ptr[i] = get_root_ptr_ptr(i);
  }

  // try to init tree and install root pointer
  auto page_buffer = (dsm->get_rbuf(0)).get_page_buffer();
  auto my_root_addr = dsm->alloc(kLeafPageSize);

  dsm->write_sync(page_buffer, my_root_addr, kLeafPageSize);

  char * char_addr = (char *) &(my_root_addr.val);
  dsm->write_sync(char_addr, root_ptr_ptr[tree_id], sizeof(GlobalAddress));
}

GlobalAddress BForest::get_root_ptr_ptr(uint16_t id) {
    GlobalAddress addr;
    addr.nodeID = 0;
    addr.offset =
        define::kRootPointerStoreOffest + sizeof(GlobalAddress) * id;

    return addr;
}

extern RootCache *rootCaches;
GlobalAddress BForest::get_root_ptr(CoroContext *cxt, int coro_id, uint16_t id) {
    if (rootCaches[id].g_root_ptr != GlobalAddress::Null()) {
        auto page_buffer = (dsm->get_rbuf(coro_id)).get_page_buffer();
        dsm->read_sync(page_buffer, root_ptr_ptr[id], sizeof(GlobalAddress), cxt);
        GlobalAddress root_ptr = *(GlobalAddress *)page_buffer;
        return root_ptr;
    } else {
        return rootCaches[id].g_root_ptr;
    }
}

void BForest::batch_insert(KVTS *kvs, int cnt, CoroContext *cxt,
            int coro_id) 
{
  assert(dsm->is_register());
  auto root = get_root_ptr(cxt, coro_id, tree_id);
  GlobalAddress p = root;

  // auto page_buffer = (dsm->get_rbuf(coro_id)).get_page_buffer();
  // auto header = (BHeader *)(page_buffer + (STRUCT_OFFSET(BLeafPage, hdr)));
  // dsm->read_sync(page_buffer, p, kLeafPageSize, cxt);

  // memcpy(&stack_page_buffer[header->level], page_buffer, kLeafPageSize);

  int j = 0;
  while (j < cnt) {
    Key k = kvs[j].k;
    Value v = kvs[j].v;
    for (int i = 0; i < define::kMaxLevelOfTree ; i++) {
      auto page = &stack_page_buffer[i];
      auto header = (BHeader *) (page + (STRUCT_OFFSET(BLeafPage, hdr)));
      if (k < page->hdr.lowest || k >= page->hdr.highest) {
        continue;
      } else {
        set_stack_buffer(i, k);
      }
    }

    // // set v 
    auto leaf = (BLeafPage *)&stack_page_buffer[0];
    bool need_split = false;
    int split_level = 1;
    set_leaf(leaf, k, v, need_split);
    GlobalAddress p1;
    while (need_split) {
      set_internal(&stack_page_buffer[split_level], k, p1, need_split);
      split_level++;
    }
    j++;
    // stack_page_buffer
  }
}

void BForest::set_stack_buffer(int level, const Key & k) {
  assert(level > 0);
  for (int i = 0; i < level; i++) {
    
  }
}

bool BForest::search(const Key &k, Value &v, CoroContext *cxt,
            int coro_id) {
  assert(dsm->is_register());

  uint16_t t_id = k % tree_num;

  GlobalAddress tree_version_address;
  tree_version_address.nodeID = 0;
  tree_version_address.offset = sizeof(uint64_t) * t_id;

  auto &rbuf = dsm->get_rbuf(coro_id);
  char *version_buffer = (char *)rbuf.get_cas_buffer();//TODO: may change it later;

restart:
  dsm->read_dm_sync(version_buffer, tree_version_address, sizeof(uint64_t), cxt);
  auto root = get_root_ptr(cxt, coro_id, t_id);

  BSearchResult result;
  GlobalAddress p = root;
  bool from_cache = false;

  Key expect_low = kKeyMin;
  Key expect_high = kKeyMax;
  uint8_t expect_level = rootCaches[t_id].g_root_level;

next:
  if (!page_search(p, k, result, expect_low, expect_high, expect_level, cxt, coro_id, from_cache)) {
    sleep(1);
    goto next;
  } 

  if (result.corrupted) {
    goto restart;
  }
  
  if (result.is_leaf) {
    if (result.val != kValueNull) { // find
      v = result.val;
      return true;
    }
    if (result.slibing != GlobalAddress::Null()) { // turn right
      p = result.slibing;
      goto next;
    }
    return false; // not found
  } else {        // internal
    p = result.slibing != GlobalAddress::Null() ? result.slibing
                                                : result.next_level;
    goto next;
  }
}

bool BForest::page_search(GlobalAddress page_addr, const Key &k, BSearchResult &result, Key expect_lowest, Key expect_highest, uint8_t expect_level,
                CoroContext *cxt, int coro_id, bool from_cache) 
{
  if (!check_ga(page_addr)) {
    result.corrupted = true;
    return false;
  }

  auto page_buffer = (dsm->get_rbuf(coro_id)).get_page_buffer();
  auto header = (BHeader *)(page_buffer + (STRUCT_OFFSET(BLeafPage, hdr)));

  dsm->read_sync(page_buffer, page_addr, kLeafPageSize, cxt);

  memset(&result, 0, sizeof(result));

  if (header->level != expect_level) {
    result.corrupted = true;
    return true;
  }

  if (header->lowest < expect_lowest || header->highest > expect_highest) {
    result.corrupted = true;
    return true;
  }

  result.is_leaf = header->leftmost_ptr == GlobalAddress::Null();
  result.level = header->level;

  if (result.is_leaf) {
    auto page = (BLeafPage *)page_buffer;

    if (from_cache &&
        (k < page->hdr.lowest || k >= page->hdr.highest)) { // cache is stale
      return false;
    }

    assert(result.level == 0);
    if (k >= page->hdr.highest) { // should turn right
      result.slibing = page->hdr.sibling_ptr;
      return true;
    }
    if (k < page->hdr.lowest) {
      assert(false);
      return false;
    }
    leaf_page_search(page, k, result);
  } else {
    assert(result.level != 0);
    assert(!from_cache);
    auto page = (BInternalPage *)page_buffer;

    // if (result.level == 1 && enable_cache) {
    //   index_cache->add_to_cache(page);
    // }

    if (k >= page->hdr.highest) { // should turn right
      result.slibing = page->hdr.sibling_ptr;
      return true;
    }
    if (k < page->hdr.lowest) {
      printf("key %ld error in level %d\n", k, page->hdr.level);
      sleep(10);
      // print_and_check_tree();
      assert(false);
      return false;
    }
    internal_page_search(page, k, result);
  }

  return true;
}

void BForest::internal_page_search(BInternalPage *page, const Key &k, BSearchResult &result) 
{
  assert(k >= page->hdr.lowest);
  assert(k < page->hdr.highest);

  auto cnt = page->hdr.last_index + 1;
  // page->debug();
  if (k < page->records[0].key) {
    result.next_level = page->hdr.leftmost_ptr;
    return;
  }

  for (int i = 1; i < cnt; ++i) {
    if (k < page->records[i].key) {
      result.next_level = page->records[i - 1].ptr;
      return;
    }
  }
  result.next_level = page->records[cnt - 1].ptr;
}

void BForest::leaf_page_search(BLeafPage *page, const Key &k, BSearchResult &result) 
{
  for (int i = 0; i < kBLeafCardinality; ++i) {
    auto &r = page->records[i];
    if (r.key == k && r.value != kValueNull && r.f_version == r.r_version) {
      result.val = r.value;
      break;
    }
  }
}

bool BForest::check_ga(GlobalAddress ga) {
  GlobalAddress start = allocator_starts[ga.nodeID];
  if ((ga.offset - start.offset) % kLeafPageSize != 0) {
    return false;
  } else {
    return true;
  }
}
};