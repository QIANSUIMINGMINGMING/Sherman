#if !defined(_RDMA_BUFFER_H_)
#define _RDMA_BUFFER_H_

#include "Common.h"

// abstract rdma registered buffer
class RdmaBuffer {
 private:
  static const int kLevelStackPageBufferCnt = 8;
  static const int kStackPageBufferCnt =
      kLevelStackPageBufferCnt * define::kMaxLevelOfTree;
  static const int kSplitPageBufferCnt = 8 * define::kMaxLeafSplit;

  static const int kPageBufferCnt = 8;     // async, buffer safty
  static const int kSiblingBufferCnt = 8;  // async, buffer safty
  static const int kCasBufferCnt = 8;      // async, buffer safty
  static const int kNextLevelBufferCnt = 8;
  // static const int kBatchPageBufferCnt = 99;
  // static const int k

  char *buffer;

  uint64_t *cas_buffer;
  uint64_t *unlock_buffer;
  uint64_t *zero_64bit;
  char *page_buffer;
  char *sibling_buffer;
  char *entry_buffer;
  char *stack_page_buffer;
  char *split_page_buffer;
  char *next_level_page_buffer;

  int page_buffer_cur;
  int sibling_buffer_cur;
  int cas_buffer_cur;
  int stack_page_buffer_cur[define::kMaxLevelOfTree];
  int split_page_buffer_cur;
  int next_level_page_buffer_cur;

  int kPageSize;

 public:
  RdmaBuffer(char *buffer) {
    set_buffer(buffer);

    page_buffer_cur = 0;
    sibling_buffer_cur = 0;
    cas_buffer_cur = 0;
    memset(stack_page_buffer, 0, sizeof(int) * define::kMaxLevelOfTree);
    split_page_buffer_cur = 0;
    next_level_page_buffer_cur = 0;
  }

  RdmaBuffer() = default;

  void set_buffer(char *buffer) {
    // printf("set buffer %p\n", buffer);

    kPageSize = std::max(kLeafPageSize, kInternalPageSize);
    this->buffer = buffer;
    cas_buffer = (uint64_t *)buffer;
    unlock_buffer =
        (uint64_t *)((char *)cas_buffer + sizeof(uint64_t) * kCasBufferCnt);
    zero_64bit = (uint64_t *)((char *)unlock_buffer + sizeof(uint64_t));
    stack_page_buffer = (char *)zero_64bit + sizeof(uint64_t);
    split_page_buffer =
        (char *)stack_page_buffer + kPageSize * kStackPageBufferCnt;
    // page_buffer = (char *)zero_64bit + sizeof(uint64_t);
    page_buffer = (char *)split_page_buffer + kPageSize * kSplitPageBufferCnt;
    sibling_buffer = (char *)page_buffer + kPageSize * kPageBufferCnt;
    next_level_page_buffer =
        (char *)sibling_buffer + kPageSize * kSiblingBufferCnt;
    entry_buffer =
        (char *)next_level_page_buffer + kPageSize * kNextLevelBufferCnt;
    *zero_64bit = 0;

    // assert((char *)zero_64bit + 8 - buffer < define::kPerCoroRdmaBuf);
    assert(entry_buffer - buffer < define::kPerCoroRdmaBuf);
  }
  char *get_next_level_buffer() {
    next_level_page_buffer_cur =
        (next_level_page_buffer_cur + 1) % kNextLevelBufferCnt;
    return next_level_page_buffer + next_level_page_buffer_cur * kPageSize;
  }

  uint64_t *get_cas_buffer() {
    cas_buffer_cur = (cas_buffer_cur + 1) % kCasBufferCnt;
    return cas_buffer + cas_buffer_cur;
  }

  uint64_t *get_unlock_buffer() const { return unlock_buffer; }

  uint64_t *get_zero_64bit() const { return zero_64bit; }

  char *get_stack_page_buffer(int level) {
    stack_page_buffer_cur[level] =
        (stack_page_buffer_cur[level] + 1) %
        (kStackPageBufferCnt / define::kMaxLevelOfTree);
    auto level_stack_page_buffer =
        stack_page_buffer + level * kPageSize * kLevelStackPageBufferCnt;
    return level_stack_page_buffer + stack_page_buffer_cur[level] * kPageSize;
  }

  char *get_split_page_buffer(int n) {
    assert(n <= (int)define::kMaxLeafSplit);
    split_page_buffer_cur = (split_page_buffer_cur + n) % kSplitPageBufferCnt;
    return split_page_buffer + kPageSize * split_page_buffer_cur;
  }

  char *get_page_buffer() {
    page_buffer_cur = (page_buffer_cur + 1) % kPageBufferCnt;
    return page_buffer + (page_buffer_cur * kPageSize);
  }

  char *get_range_buffer() { return page_buffer; }

  char *get_sibling_buffer() {
    sibling_buffer_cur = (sibling_buffer_cur + 1) % kSiblingBufferCnt;
    return sibling_buffer + (sibling_buffer_cur * kPageSize);
  }

  char *get_entry_buffer() const { return entry_buffer; }
};

#endif  // _RDMA_BUFFER_H_
