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

thread_local uintptr_t BForest::stack_page_buffer[define::kMaxLevelOfTree];
thread_local GlobalAddress BForest::stack_page_addr_buffer[define::kMaxLevelOfTree];

BInsertedEntry new_inserted_entries[define::kMaxNumofInternalInsert*kMaxBatchInsertCoreNum];
int new_inserted_entries_cnts[kMaxBatchInsertCoreNum];

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

  auto next_level_page_buffer = (dsm->get_rbuf(0)).get_next_level_buffer();
  next_level_page_addr = dsm->alloc(kInternalPageSize);
  next_level_page = new (next_level_page_buffer) BInternalPage;
  next_level_page->hdr.level = 1;
  assert(next_level_page != nullptr);
  dsm->write_sync((char*) next_level_page, next_level_page_addr, kInternalPageSize);

  // try to init tree and install root pointer
  auto page_buffer = (dsm->get_rbuf(0)).get_page_buffer();
  auto my_root_addr = dsm->alloc(kLeafPageSize);
  auto root_page = new (page_buffer) BLeafPage;
  root_page->hdr.parent_ptr = next_level_page_addr;
  assert(root_page != nullptr);

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

void BForest::update_root(int level) {
  auto page_buffer = (dsm->get_rbuf(0)).get_next_level_buffer();
  auto cas_buffer = dsm->get_rbuf(0).get_cas_buffer();
  auto addr = dsm->alloc(kInternalPageSize);
  auto page = new (page_buffer) BInternalPage;
  page->hdr.level = level;

  dsm->write_sync(page_buffer, addr, kInternalPageSize);
}

//[kKeyMin, kvs, kKeyMax]
void BForest::batch_insert(KVTS *kvs, int full_number, int thread_num) {
  int per_thread_num = (full_number - 2)/ thread_num;
  for (int i = 0; i < thread_num; i++) {
    batch_insert_threads[i] = std::thread(std::bind(&BForest::batch_insert_leaf, this, kvs, i * (per_thread_num + 2) - 1, per_thread_num + 2, full_number, i, nullptr, 0));
  }
  for (int i = 0; i < thread_num; i++) {
    batch_insert_threads->join();
  }
  batch_insert_internal(thread_num);
}

void BForest::batch_insert_internal(int thread_num) {
  bool need_new_root = false;
  uint64_t cur_level = next_level_page->hdr.level - 1; 
  for (uint64_t i = 0; i < cur_level; i++) {
    new_inserted_entries[i].visit_all([&](auto &x) {
      std::sort(x.second.begin(), x.second.end(), [](const BInternalEntry &a, const BInternalEntry &b) { return a.key < b.key; });
      bool need_split = x.second.size() > kBInternalCardinality;
      uint64_t split_num = ((x.second.size() - 1) / (kBInternalCardinality / 2)) + 1;

      if (!need_split) {
        auto page_buffer = (dsm->get_rbuf(0)).get_page_buffer();
        auto page_header = (BHeader *)(page_buffer + (STRUCT_OFFSET(BInternalPage, hdr)));
        auto page = (BInternalPage *)page_buffer;
        //set page
        assert(new_inserted_headers.find(x.first) != new_inserted_headers.end());
        (*page_header) = new_inserted_headers[x.first];
        page_header->last_index = x.second.size();
        page_header->level = i + 1;
        for (size_t j = 0; j < x.second.size(); j++) {
          page->records[j].key = x.second[j].key;
          page->records[j].ptr = x.second[j].ptr;
        }
        GlobalAddress write_pos;
        write_pos.val = x.first;
        dsm->write_sync(page_buffer, write_pos, kInternalPageSize, nullptr);
      } else {
        // write first page
        size_t per_page_num = (x.second.size() / split_num) + 1; 
        auto page_buffer = (dsm->get_rbuf(0)).get_page_buffer();
        auto page_header = (BHeader *)(page_buffer + (STRUCT_OFFSET(BInternalPage, hdr)));
        auto page = (BInternalPage *)page_buffer;
        //set page
        
        assert(new_inserted_headers.find(x.first) != new_inserted_headers.end());
        (*page_header) = new_inserted_headers[x.first];

        page_header->last_index = per_page_num;
        page_header->level = i + 1;
        GlobalAddress cur_parent_addr_ptr = page_header->parent_ptr;
        if (cur_parent_addr_ptr == next_level_page_addr) {
          need_new_root = true;
          BInternalEntry o_be;
          assert(page_header->lowest == kKeyMin);
          next_level_page->hdr.leftmost_ptr.val = x.first;
        }

        Key split_highest = page_header->highest;
        page_header->highest = x.second[per_page_num].key;
  
        for (size_t j = 0; j < per_page_num; j++) {
          page->records[j].key = x.second[j].key;
          page->records[j].ptr = x.second[j].ptr;
        }

        GlobalAddress write_pos;
        write_pos.val = x.first;
        dsm->write_sync(page_buffer, write_pos, kInternalPageSize, nullptr); 

        uint64_t remain_num = split_num - 1;
        size_t cur_kv_pos = per_page_num;

        //TODO:set sibling  

        while (remain_num > 0) {
          int page_num = std::min(remain_num, define::kMaxLeafSplit);
          auto store_addr = dsm->alloc(kLeafPageSize * page_num);
          auto pages = (dsm->get_rbuf(0)).get_split_page_buffer(page_num);
          for (int cur_page_i = 0; cur_page_i < page_num; cur_page_i++) {
            BInternalPage * cur_new_page = &((BInternalPage *)pages)[cur_page_i];
            int last_index = 0;
            cur_new_page->hdr.lowest = x.second[cur_kv_pos].key; 
            cur_new_page->hdr.leftmost_ptr = x.second[cur_kv_pos].ptr;
            cur_kv_pos++;
            for (size_t cur_page_pos = 0; cur_page_pos < per_page_num - 1; cur_page_pos ++) {
              cur_new_page->records[cur_page_pos].key = x.second[cur_kv_pos].key;
              cur_new_page->records[cur_page_pos].ptr = x.second[cur_kv_pos].ptr; 
              assert(cur_new_page->records[cur_page_pos].ptr != GlobalAddress::Null());
              cur_kv_pos++;
              last_index++;
              if (cur_kv_pos == x.second.size()) {
                break;
              }
            }
            cur_new_page->hdr.last_index = last_index;
            cur_new_page->hdr.highest = (cur_page_i == page_num - 1 && remain_num - page_num == 0) ? split_highest : x.second[cur_kv_pos].key;
            cur_new_page->hdr.level = i + 1;

            BInternalEntry be;
            be.key = cur_new_page->hdr.lowest;
            be.ptr = GADD(store_addr, cur_page_i * kInternalPageSize);
            new_inserted_entries[i+1].visit(cur_parent_addr_ptr.val, [&](auto &y){
              y.second.push_back(be);
            }); 
          }
          dsm->write_sync(pages, store_addr, page_num * kLeafPageSize, nullptr);
          remain_num -= page_num;
        }
      }
    }); 
  }

  while (need_new_root) {
    new_inserted_entries[cur_level].visit(next_level_page_addr.val, [&](auto &x){
      std::sort(x.second.begin(), x.second.end(), [](const BInternalEntry &a, const BInternalEntry &b) { return a.key < b.key; });
      bool need_split = x.second.size() > kBInternalCardinality;
      uint64_t split_num = ((x.second.size() - 1) / (kBInternalCardinality / 2)) + 1;

      if (!need_split) {
        need_new_root = false;
        next_level_page->hdr.last_index = x.second.size();
        next_level_page->hdr.level = cur_level + 1;
        assert(next_level_page->hdr.leftmost_ptr != GlobalAddress::Null());
        for (size_t j = 0; j < x.second.size(); j++) {
          next_level_page->records[j].key = x.second[j].key;
          next_level_page->records[j].ptr = x.second[j].ptr;
        }
        GlobalAddress write_pos;
        write_pos.val = x.first;
        assert(write_pos == next_level_page_addr.val);
        dsm->write_sync((char *)next_level_page, write_pos, kInternalPageSize, nullptr);
        update_root(cur_level + 1);
      } else {
        // assign next next level page
        auto next_level_parent_page_buffer = dsm->get_rbuf(0).get_next_level_buffer();
        auto next_level_parent_page_addr = dsm->alloc(kInternalPageSize);
        BInternalPage * next_level_parent_page = new (next_level_parent_page_buffer) BInternalPage;

        size_t per_page_num = (x.second.size() / split_num) + 1; 
        auto next_level_header = &(next_level_page->hdr);
        next_level_header->last_index = per_page_num;
        next_level_header->level = cur_level + 1;

        GlobalAddress cur_parent_addr_ptr = next_level_parent_page_addr;

        uint64_t next_level_parent_page_addr_val = next_level_parent_page_addr.val;
        new_inserted_entries->emplace(next_level_parent_page_addr_val, std::vector<BInternalEntry>());

        next_level_parent_page->hdr.leftmost_ptr.val = x.first;

        Key split_highest = next_level_header->highest;
        assert(split_highest == kKeyMax);
        next_level_header->highest = x.second[per_page_num].key;
  
        for (size_t j = 0; j < per_page_num; j++) {
          next_level_page->records[j].key = x.second[j].key;
          next_level_page->records[j].ptr = x.second[j].ptr;
        }

        GlobalAddress write_pos;
        write_pos.val = x.first;
        dsm->write_sync((char *)next_level_page, write_pos, kInternalPageSize, nullptr); 

        uint64_t remain_num = split_num - 1;
        size_t cur_kv_pos = per_page_num;

        //TODO:set sibling  

        while (remain_num > 0) {
          int page_num = std::min(remain_num, define::kMaxLeafSplit);
          auto store_addr = dsm->alloc(kLeafPageSize * page_num);
          auto pages = (dsm->get_rbuf(0)).get_split_page_buffer(page_num);
          for (int cur_page_i = 0; cur_page_i < page_num; cur_page_i++) {
            BInternalPage * cur_new_page = &((BInternalPage *)pages)[cur_page_i];
            int last_index = 0;
            cur_new_page->hdr.lowest = x.second[cur_kv_pos].key; 
            cur_new_page->hdr.leftmost_ptr = x.second[cur_kv_pos].ptr;
            cur_kv_pos++;
            for (size_t cur_page_pos = 0; cur_page_pos < per_page_num - 1; cur_page_pos ++) {
              cur_new_page->records[cur_page_pos].key = x.second[cur_kv_pos].key;
              cur_new_page->records[cur_page_pos].ptr = x.second[cur_kv_pos].ptr; 
              assert(cur_new_page->records[cur_page_pos].ptr != GlobalAddress::Null());
              cur_kv_pos++;
              last_index++;
              if (cur_kv_pos == x.second.size()) {
                break;
              }
            }
            cur_new_page->hdr.last_index = last_index;
            cur_new_page->hdr.highest = (cur_page_i == page_num - 1 && remain_num - page_num == 0) ? split_highest : x.second[cur_kv_pos].key;
            cur_new_page->hdr.level = cur_level + 1;

            BInternalEntry be;
            be.key = cur_new_page->hdr.lowest;
            be.ptr = GADD(store_addr, cur_page_i * kInternalPageSize);
            new_inserted_entries[cur_level+1].visit(cur_parent_addr_ptr.val, [&](auto &y){
              y.second.push_back(be);
            }); 
          }
          dsm->write_sync(pages, store_addr, page_num * kLeafPageSize, nullptr);
          remain_num -= page_num;
        }
        next_level_page = next_level_parent_page;
        next_level_page_addr = next_level_parent_page_addr;
      } 
    });
  cur_level++;
  }
}

void BForest::batch_insert_leaf(BForest * forest, KVTS *global_kvs, int start, int cnt, int full_number, int thread_id, CoroContext *cxt,
            int coro_id) 
{
  KVTS * kvs = global_kvs + start;
  DSM * dsm = forest->dsm;
  assert(dsm->is_register());
  auto root = forest->get_root_ptr(cxt, coro_id, forest->tree_id);
  auto page_buffer = (dsm->get_rbuf(coro_id)).get_page_buffer();
  auto header = (BHeader *)(page_buffer + (STRUCT_OFFSET(BLeafPage, hdr)));
  dsm->read_sync(page_buffer, root, kLeafPageSize, cxt);
  // tree_meta[root.val] = (int)(header->last_index);
  forest->tree_meta.try_emplace(root.val, (int)(header->last_index));
  int cur_level = header->level; 
  stack_page_buffer[cur_level] = (uintptr_t)page_buffer;
  stack_page_addr_buffer[cur_level] = root;

  // kMin ... kMax
  forest->set_stack_buffer(cur_level, kvs[cnt - 2].k, cxt, coro_id);
  BatchInsertFlag r_flag = kvs[cnt - 1].k >= ((BLeafPage *)(stack_page_buffer[0]))->hdr.highest ? BatchInsertFlag::RIGHT_DIFF_PAGE : BatchInsertFlag::RIGHT_SAME_PAGE;
  GlobalAddress last_leaf_page_addr = stack_page_addr_buffer[0];
  Key last_leaf_highest = ((BLeafPage *)(stack_page_buffer[0]))->hdr.highest;
  forest->set_stack_buffer(cur_level, kvs[1].k, cxt, coro_id);
  BatchInsertFlag l_flag = (kvs[0].k < ((BLeafPage *)(stack_page_buffer[0]))->hdr.lowest || start == 0) ? BatchInsertFlag::LEFT_DIFF_PAGE : BatchInsertFlag::LEFT_SAME_PAGE;
  GlobalAddress first_leaf_page_addr = stack_page_addr_buffer[0];
  Key start_leaf_lowest = ((BLeafPage *)(stack_page_buffer[0]))->hdr.lowest;

  int pre_same_page_kv = 0;
  int pro_same_page_kv = 0;
  if (l_flag == BatchInsertFlag::LEFT_SAME_PAGE) {
    while ( pre_same_page_kv < kBLeafCardinality ) {
      if (global_kvs[start - pre_same_page_kv].k < start_leaf_lowest) {
        break;
      }
      pre_same_page_kv ++;
    }
  }

  if (r_flag == BatchInsertFlag::RIGHT_SAME_PAGE) {
    while ( pre_same_page_kv < kBLeafCardinality) {
      if ( global_kvs[start + cnt - 1 + pro_same_page_kv].k >= last_leaf_highest ) {
        break;
      }
      pro_same_page_kv ++;
    }
  }

  // leaf splitting
  int split_start = 1;
  int split_num = 0;
  int split_range = 0;

  //buffer statement
  uintptr_t page;
  BInternalPage * i_page;
  BLeafPage * l_page;

  int j = 1;

  while (j < cnt - 1) {
    Key k = kvs[j].k;
    Value v = kvs[j].v;
    for (int i = 0; i < cur_level; i++) {
      page = stack_page_buffer[i];
      if (i == 0) {
        l_page = (BLeafPage *) page;
        header = (BHeader *)(l_page + (STRUCT_OFFSET(BLeafPage, hdr)));
        if (k < l_page->hdr.lowest || k >= l_page->hdr.highest) {
          continue;
        } else {
          break;
        }   
      } else {
        i_page = (BInternalPage *) page;
        header = (BHeader *)(i_page + (STRUCT_OFFSET(BInternalPage, hdr)));
        if (k < i_page->hdr.lowest || k >= i_page->hdr.highest) {
          assert(i < cur_level - 1);
          continue;
        } else {
          if (split_num > 0) {
            forest->split_leaf(kvs, split_start, split_num, split_range, stack_page_addr_buffer[0] == first_leaf_page_addr ? l_flag : BatchInsertFlag::LEFT_DIFF_PAGE, BatchInsertFlag::RIGHT_DIFF_PAGE, thread_id, cxt, coro_id);
            split_num = 0;
            split_range = 0;
          } else {
            forest->set_stack_buffer(i, k, cxt, coro_id);
          }
        }
      }
    }
    // // set v 
    // if (split_num == 0) {
    GlobalAddress leaf_addr = stack_page_addr_buffer[0];
    bool need_split = false;
    BatchInsertFlag set_lflag = leaf_addr == first_leaf_page_addr ? l_flag : BatchInsertFlag::LEFT_DIFF_PAGE;
    BatchInsertFlag set_rflag = leaf_addr == last_leaf_page_addr ? r_flag : BatchInsertFlag::RIGHT_DIFF_PAGE; 
    forest->set_leaf(kvs + j, k, v, need_split, set_lflag, set_rflag, pre_same_page_kv, pro_same_page_kv, cxt, coro_id);
    if (need_split) {
      // set start of split range
      if (split_range == 0) {
        split_start = j;
      } 
      split_range++;
      split_num++;
    } else if (split_range != 0) {
      split_range++;
    }
    j++;
  }
  if (split_num != 0) {
    BatchInsertFlag llflag = stack_page_addr_buffer[0] == first_leaf_page_addr ? l_flag : BatchInsertFlag::LEFT_DIFF_PAGE;
    forest->split_leaf(kvs, split_start, split_num, split_range, llflag, r_flag, thread_id, cxt, 0);
  }
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

void BForest::go_in_kvts(KVTS *kvs, int start, int range, int &next) {
  for (int i = start; i < range; i++) {
    if (kvs[i].v != kValueNull) {
      next = i;
      return;
    }
  }
  next = range;
}

void BForest::calculate_meta(int split_num, BatchInsertFlag l_flag) {
  BInternalPage * next_parent_page;
  GlobalAddress next_parent_page_addr;
  
  int level = 1;
  next_parent_page = (BInternalPage *) stack_page_buffer[level];
  next_parent_page_addr = stack_page_addr_buffer[level];
  int next_level_insert_number = l_flag == BatchInsertFlag::LEFT_SAME_PAGE ? split_num : split_num - 1;

  while (next_parent_page_addr != GlobalAddress::Null() && next_level_insert_number > 0) {
    uint64_t next_parent_addr_val = next_parent_page_addr.val;
    int meta = 0;
    tree_meta.visit(next_parent_addr_val, [&](auto &x) {
      x.second += next_level_insert_number;    
      meta = x.second;  
    });
    int old_split_page_num = (meta - next_level_insert_number) / kBInternalCardinality;
    next_level_insert_number = (meta / kBInternalCardinality) - old_split_page_num;

    if (new_inserted_entries[level - 1].try_emplace(next_parent_addr_val, std::vector<BInternalEntry>())) {
      new_inserted_headers[next_parent_page_addr.val] = next_parent_page->hdr;
      new_inserted_entries[level - 1].visit(next_parent_addr_val, [&](auto &x){
        for (int i = 0; i < next_parent_page->hdr.last_index; i++) {
          x.second.push_back(next_parent_page->records[i]);
        }  
      }); 
    }
    level++;
    next_parent_page = (BInternalPage *) stack_page_buffer[level];
    next_parent_page_addr = stack_page_addr_buffer[level];
  } 

  if (next_level_insert_number > 0) {
    new_inserted_entries[level - 1].try_emplace(next_level_page_addr.val, std::vector<BInternalEntry>());
  }
}

void BForest::split_leaf(KVTS *kvs, int start, int insert_split_num, int range, BatchInsertFlag l_flag, BatchInsertFlag r_flag, int thread_id, CoroContext *cxt, int coro_id) {
  Key n_lowest = l_flag == BatchInsertFlag::LEFT_SAME_PAGE ? kvs[start].k : ((BLeafPage *)(stack_page_buffer[0]))->hdr.lowest;
  Key n_highest = r_flag == BatchInsertFlag::RIGHT_SAME_PAGE ? kvs[start + range].k : ((BLeafPage *)(stack_page_buffer[0]))->hdr.highest;

  // calculate the number of all entries 
  int actual_num = insert_split_num;
  auto old_page = (BLeafPage *)stack_page_buffer[0];
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

  calculate_meta(split_num, l_flag);

  Key uplayer_key = kKeyMax;
  auto parent_page_addr = stack_page_addr_buffer[1];

  int uplayer_key_pos = kBInternalCardinality;
  if (parent_page_addr == GlobalAddress::Null()) {
    parent_page_addr = next_level_page_addr;
    if (new_inserted_entries[0].try_emplace(next_level_page_addr.val, std::vector<BInternalEntry>())) {
      new_inserted_headers[next_level_page_addr.val] = next_level_page->hdr;
    }
  } else {  
    if (BatchInsertFlag::LEFT_DIFF_PAGE == l_flag) {
      for (int i = 0; i < next_level_page->hdr.last_index; i++) {
        if (next_level_page->records[i].ptr == stack_page_addr_buffer[0]) {
          uplayer_key = next_level_page->records[i].key;
          uplayer_key_pos = i;
          break;
        }
      }
      assert(uplayer_key != kKeyMax);
      assert(uplayer_key_pos != kBInternalCardinality);
    }
  }
  uint64_t remain_num = split_num;
  while (remain_num != 0) {
    int page_num = std::min(remain_num, define::kMaxLeafSplit);
    auto store_addr = dsm->alloc(kLeafPageSize * page_num);
    auto pages = (dsm->get_rbuf(coro_id)).get_split_page_buffer(page_num);
    for (int i = 0; i < page_num; i++) {
      BLeafPage * page = &((BLeafPage *)pages)[i];
      BHeader * page_header = (BHeader *)(page + (STRUCT_OFFSET(BLeafPage, hdr))); 
      int last_index = 0;
      for (int j = 0; j < per_page_num; j++) {
        if (cur_pos_kvts < range && cur_pos_old < kBLeafCardinality) {
          if (kvs[cur_pos_kvts].k < old_page->records[cur_pos_old].key) {
            page->records[j].key = kvs[cur_pos_kvts].k;
            page->records[j].value = kvs[cur_pos_kvts].v;
            go_in_kvts(kvs, cur_pos_kvts + 1, range, cur_pos_kvts);
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
        } else if (cur_pos_kvts >= range && cur_pos_old < kBLeafCardinality) {
          assert(cur_pos_kvts == range);
          page->records[j].key = old_page->records[cur_pos_old].key;
          page->records[j].value = old_page->records[cur_pos_old].value;
          go_in_leaf(old_page, cur_pos_old + 1, n_lowest, n_highest, cur_pos_old);
        } else if (cur_pos_kvts < range && cur_pos_old >= kBLeafCardinality) {
          assert(cur_pos_old == kBLeafCardinality);
          page->records[j].key = kvs[cur_pos_kvts].k;
          page->records[j].value = kvs[cur_pos_kvts].v;
          go_in_kvts(kvs, cur_pos_kvts + 1, range, cur_pos_kvts);
        } else {
          break;
        }
        if (page->records[j].value != kValueNull) {
          last_index++;
        }
      }
      // set header
      page_header->level = 0;
      page_header->last_index = last_index;
      page_header->lowest = i == 0 ? n_lowest : ((BLeafPage *)pages)[i - 1].hdr.highest;
      page_header->highest = (i == page_num - 1 && remain_num - page_num == 0) ? n_highest : page->records[last_index - 1].key + 1;
      page_header->last_index = last_index;
      // write to parent
      if (i == 0 && remain_num == split_num && l_flag == BatchInsertFlag::LEFT_DIFF_PAGE && uplayer_key != kKeyMax) {
        new_inserted_entries[0].visit(parent_page_addr.val, [&](auto& x) {
          assert(x.second[uplayer_key_pos].key == uplayer_key);
          x.second[uplayer_key_pos].ptr = store_addr;
        }); 
      } else {
        BInternalEntry be;
        be.key = page_header->lowest;
        be.ptr = GADD(store_addr, kLeafPageSize * i);
        new_inserted_entries[0].visit(parent_page_addr.val, [&](auto& x) {
          x.second.push_back(be);
        }); 
      }
    }
    n_lowest = ((BLeafPage *)pages)[page_num - 1].hdr.highest;
    dsm->write_sync(pages, store_addr, page_num * kLeafPageSize, cxt);
    remain_num -= page_num;
  }
}

void BForest::set_leaf(KVTS *kvs, const Key &k, const Value &v, bool & need_split, BatchInsertFlag l_flag, BatchInsertFlag r_flag, int pre_kv, int pro_kv, CoroContext *cxt, int coro_id) {

  auto page = (BLeafPage *)stack_page_buffer[0];
  auto addr = stack_page_addr_buffer[0];
  // update or delete
  for (int i = 0; i < kBLeafCardinality; i++) {
    if (page->records[i].key == k && page->records[i].value != kValueNull) {
      if (page->records[i].value != v) {
        page->records[i].value = v;
        // set kvs to null so we know we dont need to deal with it after
        kvs->v = kValueNull;
        // rdma_write kv
        char * update_addr = (char *)page + (STRUCT_OFFSET(BLeafPage, records) + i * sizeof(BLeafEntry));
        GlobalAddress remote_addr = GADD(addr, STRUCT_OFFSET(BLeafPage, records) + i * sizeof(BLeafEntry)); 
        dsm->write_sync(update_addr, remote_addr, sizeof(BLeafEntry), cxt);
      }
      need_split = false;
      return;
    }
  }

  // insert
  int old = -1;
  tree_meta.visit(addr.val, [&](auto &x) {
    old = x.second;
    x.second++;
  });
  if (l_flag == BatchInsertFlag::LEFT_SAME_PAGE && r_flag == BatchInsertFlag::RIGHT_SAME_PAGE) {
    need_split = true;
    return;
  }
  assert(old != -1);
  if (old >= kBLeafCardinality || (old + pre_kv >= kBLeafCardinality && l_flag == BatchInsertFlag::LEFT_SAME_PAGE) || (old + pro_kv >= kBLeafCardinality && r_flag == BatchInsertFlag::RIGHT_SAME_PAGE)) {
    need_split = true;
    return;
  } else {
    // rdma_write kv
    need_split = false;
    char * update_addr = nullptr;
    GlobalAddress remote_addr = GlobalAddress::Null();
    if (l_flag == BatchInsertFlag::LEFT_SAME_PAGE) {
      for (int i = kBLeafCardinality - 1; i >= 0; i--) {
        if (page->records[i].value == kValueNull) {
          update_addr = (char *)page + (STRUCT_OFFSET(BLeafPage, records) + i * sizeof(BLeafEntry));
          remote_addr = GADD(addr, STRUCT_OFFSET(BLeafPage, records) + i * sizeof(BLeafEntry));
          break;
        }
      }
    } else {
      for (int i = 0; i < kBLeafCardinality; i++) {
        if (page->records[i].value == kValueNull) {
          update_addr = (char *)page + (STRUCT_OFFSET(BLeafPage, records) + i * sizeof(BLeafEntry));
          remote_addr = GADD(addr, STRUCT_OFFSET(BLeafPage, records) + i * sizeof(BLeafEntry));
          break;
        }
      }
    }
    assert(update_addr != nullptr);
    assert(remote_addr != GlobalAddress::Null());
    dsm->write_sync(update_addr, remote_addr, sizeof(BLeafEntry), cxt);
  }
}

void BForest::set_stack_buffer(int level, const Key & k, CoroContext *cxt, int coro_id) {
  assert(level != 0);

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
    dsm->read_sync(page_buffer, next_level_ptr, kLeafPageSize, cxt); 
    stack_page_addr_buffer[i - 1] = next_level_ptr;
    stack_page_buffer[i - 1] = (uintptr_t)page_buffer;
    buffer_header->parent_ptr = stack_page_addr_buffer[i];
    uint64_t next_level_ptr_val = next_level_ptr.val;
    tree_meta.try_emplace_or_visit(next_level_ptr_val, (int)(buffer_header->last_index), [&](auto &x) {
      x.second = (int)(buffer_header->last_index); 
    });
  }
}

void BForest::search_stack_buffer(int level, const Key &k, GlobalAddress & result) {
  assert(level != 0);
  auto page = (BInternalPage *)stack_page_buffer[level];
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

  // GlobalAddress tree_version_address;
  // tree_version_address.nodeID = 0;
  // tree_version_address.offset = sizeof(uint64_t) * t_id;
  auto root = get_root_ptr(cxt, coro_id, t_id);
  BSearchResult result;
  GlobalAddress p = root;

next:
  if (!page_search(p, k, result, cxt, coro_id)) {
    sleep(1);
    goto next;
  } 
  if (result.is_leaf) {
    if (result.val != kValueNull) { // find
      v = result.val;
      return true;
    }
    return false; // not found
  } else {        // internal
    p = result.next_level;
    goto next;
  }
}

bool BForest::page_search(GlobalAddress page_addr, const Key &k, BSearchResult &result, 
                CoroContext *cxt, int coro_id) 
{
  auto page_buffer = (dsm->get_rbuf(coro_id)).get_page_buffer();
  auto header = (BHeader *)(page_buffer + (STRUCT_OFFSET(BLeafPage, hdr)));

  dsm->read_sync(page_buffer, page_addr, kLeafPageSize, cxt);
  memset(&result, 0, sizeof(result));

  result.is_leaf = header->leftmost_ptr == GlobalAddress::Null();
  result.level = header->level;

  if (result.is_leaf) {
    auto page = (BLeafPage *)page_buffer;

    if ((k < page->hdr.lowest || k >= page->hdr.highest)) { // cache is stale
      return false;
    }

    assert(result.level == 0);
    // if (k >= page->hdr.highest) { // should turn right
    //   result.slibing = page->hdr.sibling_ptr;
    //   return true;
    // }
    if (k < page->hdr.lowest) {
      assert(false);
      return false;
    }
    leaf_page_search(page, k, result);
  } else {
    assert(result.level != 0);
    auto page = (BInternalPage *)page_buffer;

    // if (result.level == 1 && enable_cache) {
    //   index_cache->add_to_cache(page);
    // }

    // if (k >= page->hdr.highest) { // should turn right
    //   result.slibing = page->hdr.sibling_ptr;
    //   return true;
    // }
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