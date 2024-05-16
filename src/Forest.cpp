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

thread_local BInternalPage * BForest::stack_page_buffer[define::kMaxLevelOfTree];
thread_local GlobalAddress BForest::stack_page_addr_buffer[define::kMaxLevelOfTree];

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
  auto root_page = new (page_buffer) BLeafPage;

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
  auto page_buffer = (dsm->get_rbuf(coro_id)).get_page_buffer();
  auto header = (BHeader *)(page_buffer + (STRUCT_OFFSET(BLeafPage, hdr)));
  dsm->read_sync(page_buffer, root, kLeafPageSize, cxt);
  if (tree_meta.find(root.val) == tree_meta.end()) {
    uint64_t addr = root.val;
    tree_meta.insert(std::make_pair(addr, (int)(header->last_index)));
  }
  int cur_level = header->level; 
  stack_page_buffer[cur_level] = (BInternalPage *)page_buffer;
  stack_page_addr_buffer[cur_level] = root;
  set_stack_buffer(cur_level, kvs[1].k, cxt, coro_id);  
  BatchInsertFlag l_flag = kvs[0].k < stack_page_buffer[0]->hdr.lowest ? BatchInsertFlag::LEFT_DIFF_PAGE : BatchInsertFlag::LEFT_SAME_PAGE;
  GlobalAddress first_leaf_page_addr = stack_page_addr_buffer[0];
  BatchInsertFlag r_flag;
  GlobalAddress last_leaf_page_addr;

  // leaf splitting
  int split_start = 1;
  int split_num = 0;

  //buffer statement
  BInternalPage * page;
  BHeader *pheader;

  int j = 0;
  while (j < cnt - 1) {
    Key k = kvs[j].k;
    Value v = kvs[j].v;
    for (int i = 0; i < cur_level; i++) {
      page = stack_page_buffer[i];
      if (i == 0) {
        header = (BHeader *)(page + (STRUCT_OFFSET(BLeafPage, hdr)));
      } else {
        header = (BHeader *)(page + (STRUCT_OFFSET(BInternalPage, hdr)));
      }
      if (k < page->hdr.lowest || k >= page->hdr.highest) {
        continue;
      } else {
        if (i != 0 && split_num > 0) {
          // split
          split_leaf(kvs, split_start, split_num, l_flag, BatchInsertFlag::RIGHT_DIFF_PAGE, cxt, coro_id);
        } 
        set_stack_buffer(i, k, cxt, coro_id);
        break;
      }   
    }
    // // set v 
    auto leaf = (BLeafPage *)stack_page_buffer[0];
    GlobalAddress leaf_addr = stack_page_addr_buffer[0];
    bool need_split = false;
    int split_level = 0;
    set_leaf(leaf, leaf_addr, k, v, need_split);
    if (need_split) {
      if (split_num == 0) {
        split_start = j;

      }
      split_num++;
    }
    GlobalAddress p1;
    while (need_split) {
      set_internal(stack_page_buffer[split_level], stack_page_addr_buffer[split_level], k, p1, need_split);
      split_level++;
    }
    j++;
    // stack_page_buffer
  }

  // r_flag = kvs[cnt - 1].k >= stack_page_buffer[0].hdr.highest ? BatchInsertFlag::RIGHT_DIFF_PAGE : BatchInsertFlag::RIGHT_SAME_PAGE;
  // last_leaf_page_addr = stack_page_addr_buffer[0];
}

void BForest::go_in_leaf(BLeafPage *old_page, int start, Key lowest, Key highest, int &next) {
  for (int i = start; i < kBLeafCardinality; i++) {
    if (old_page->records[i].key >= lowest && old_page->records[i].key < highest && old_page->records[i].value != kValueNull) {
      next = i;
      return;
    }
  }
  next = kBLeafCardinality;
}

void BForest::split_leaf(KVTS *kvs, int start, int num, BatchInsertFlag l_flag, BatchInsertFlag r_flag, CoroContext *cxt, int coro_id) {
  Key n_lowest = l_flag == BatchInsertFlag::LEFT_SAME_PAGE ? kvs[start].k : stack_page_buffer[0]->hdr.lowest;
  Key n_highest = r_flag == BatchInsertFlag::RIGHT_SAME_PAGE ? kvs[start + num - 1].k + 1 : stack_page_buffer[0]->hdr.highest;

  int actual_num = num;
  auto old_page = (BLeafPage *)&stack_page_buffer[0];
  std::sort(old_page->records, old_page->records + kBLeafCardinality,
      [](const BLeafEntry &a, const BLeafEntry &b) { return a.key < b.key; });

  for (int i = 0; i < kBLeafCardinality; i ++) {
    if (old_page->records[i].key >= n_lowest && old_page->records[i].key < n_highest && old_page->records[i].value != kValueNull) {
      actual_num++; 
    }
  }
  uint64_t split_num = ((actual_num - 1) / (kBLeafCardinality / 2)) + 1;
  int per_page_num = ( actual_num / split_num ) + 1;
  int cur_pos_kvts = 0, cur_pos_old = 0;
  go_in_leaf(old_page, cur_pos_old, n_lowest, n_highest, cur_pos_old);
  while (split_num != 0) {
    int page_num = std::min(split_num, define::kMaxLeafSplit);
    auto store_addr = dsm->alloc(kLeafPageSize * page_num);
    auto pages = (dsm->get_rbuf(coro_id)).get_split_page_buffer(page_num);
    for (int i = 0; i < page_num; i++) {
      BLeafPage * page = &((BLeafPage *)pages)[i];
      BHeader * page_header = (BHeader *)(page + (STRUCT_OFFSET(BInternalPage, hdr))); 
      int cur_pos_new = 0;
      for (int j = 0; j < per_page_num; j++) {
        if (cur_pos_kvts < num && cur_pos_old < kBLeafCardinality) {
          if (kvs[cur_pos_kvts].k < old_page->records[cur_pos_old].key) {
            page->records[j].key = kvs[cur_pos_kvts].k;
            page->records[j].value = kvs[cur_pos_kvts].v;
            cur_pos_kvts++;
          } else if (kvs[cur_pos_kvts].k > old_page->records[cur_pos_old].key) {
            page->records[j].key = old_page->records[cur_pos_old].key;
            page->records[j].value = old_page->records[cur_pos_old].value;
            go_in_leaf(old_page, cur_pos_old + 1, n_lowest, n_highest, cur_pos_old);
          } else {
            page->records[j].key = kvs[cur_pos_kvts].k;
            page->records[j].value = kvs[cur_pos_kvts].v;
            cur_pos_kvts++;
            go_in_leaf(old_page, cur_pos_old + 1, n_lowest, n_highest, cur_pos_old); 
          }
        } else if (cur_pos_kvts >= num && cur_pos_old < kBLeafCardinality) {
          assert(cur_pos_kvts == num);
          page->records[j].key = old_page->records[cur_pos_old].key;
          page->records[j].value = old_page->records[cur_pos_old].value;
          go_in_leaf(old_page, cur_pos_old + 1, n_lowest, n_highest, cur_pos_old);
        } else if (cur_pos_kvts < num && cur_pos_old >= kBLeafCardinality) {
          assert(cur_pos_old == kBLeafCardinality);
          page->records[j].key = kvs[cur_pos_kvts].k;
          page->records[j].value = kvs[cur_pos_kvts].v;
          cur_pos_kvts++; 
        } else {
          break;
        }
        cur_pos_new++;
      }
      // set header

      // write to parent


    }
    dsm->write_sync(pages, store_addr, page_num * kLeafPageSize, cxt);
    split_num -= page_num;
  }
}

void BForest::set_leaf(BLeafPage *page, GlobalAddress addr, const Key &k, const Value &v, bool & need_split) {
  int old = tree_meta[addr.val].fetch_add(1);
  if (old >= kBLeafCardinality) {
    need_split = true;
    return;
  } else {
    // rdma_write kv
    need_split = false;
  }
}

void BForest::set_internal(BInternalPage *page, GlobalAddress addr, const Key &k, GlobalAddress v, bool & need_split) {

}

void BForest::set_stack_buffer(int level, const Key & k, CoroContext *cxt, int coro_id) {
  if (level == 0) {
    return;
  }

  GlobalAddress next_level_ptr;
  char* page_buffer; 
  BHeader *buffer_header;

  for (int i = level; i > 0; i--) {
    search_stack_buffer(i, k, next_level_ptr);
    page_buffer = (dsm->get_rbuf(coro_id)).get_stack_page_buffer(i - 1);
    if (i == 1) {
      buffer_header = (BHeader *)(page_buffer + (STRUCT_OFFSET(BLeafPage, hdr)));
    } else {
      buffer_header = (BHeader *)(page_buffer + (STRUCT_OFFSET(BInternalPage, hdr)));
    }
    dsm->read_sync(page_buffer, next_level_ptr, kLeafCardinality, cxt); 
    stack_page_addr_buffer[i - 1] = next_level_ptr;
    stack_page_buffer[i - 1] = (BInternalPage *)page_buffer; 
    if (i > 1 && tree_meta.find(next_level_ptr.val) == tree_meta.end()) {
      uint64_t addr = next_level_ptr.val;
      tree_meta.insert(std::make_pair(addr, (int)(buffer_header->last_index)));
    }
  }
}

void BForest::search_stack_buffer(int level, const Key &k, GlobalAddress & result) {
  auto page = stack_page_buffer[level];
  assert(k >= page->hdr.lowest);
  assert(k < page->hdr.highest);

  auto cnt = page->hdr.last_index + 1;
  // page->debug();
  if (k < page->records[0].key) {
    result = page->hdr.leftmost_ptr;
    return;
  }

  for (int i = 1; i < cnt; ++i) {
    if (k < page->records[i].key) {
      result = page->records[i - 1].ptr;
      return;
    }
  }
  result = page->records[cnt - 1].ptr;             
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
    if (r.key == k && r.value != kValueNull) {
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