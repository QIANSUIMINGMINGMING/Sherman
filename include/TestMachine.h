#pragma once
#include "Common.h"
#include "McPage.h"
#include "KVCache.h"
#include "mc.h"
#include "randomUtil/ScrambledZipfGenerator.hpp"

constexpr int buffer_size = (1 << 31) / sizeof(SkipListNode);

class TestMachine {
public:
    TestMachine(int sender_num, int recv_num): sender_num(sender_num), recevier_num(recv_num) {
        cm = new rdmacm::multicast::multicastCM();
        kvCache = new KVCache(buffer_size);

        // for (int i = 0; i < sender_num; i++) {
        //     rw_threads[i] = std::thread(rw_thread_run, i);
        // }

        for (int i = 0; i < recv_num; i++) {
            recv_insertion_threads[i] = std::thread(std::bind(&TestMachine::recv_insertion_thread_run, this, new int(i)));
        } 
    } 
    
    void insert(Key k, Value v, TransferObj * page, int i) {
        TS cur = myClock::get_ts();
        page->elements[i].k = k;
        page->elements[i].v = v;
        page->elements[i].ts = cur;
        kvCache->insert(k, cur, v);
    }
    
    void search(Key k, Value &v) {
        if (!kvCache->search(k, v)) {
            search_in_tree();
        }
    }

private: 
    void search_in_tree() {}

    static void recv_insertion_thread_run(void * args[]) {
        TestMachine *me = (TestMachine *)args[0];
        int my_id = *(int *)args[1];
        auto my_recv_queue = me->cm->getPageQueue(my_id);
        while (me->recv_queue_blocker.load()) {
            TransferObj * page;
            if(!my_recv_queue->try_dequeue(page)) {
                continue;
            }
            for (int i = 0; i < kMcCardinality; i++) {
                me->kvCache->insert(&page->elements[i]);
            }
        }    
    }

    int sender_num;
    int recevier_num;

    std::thread rw_threads[kMaxRwCoreNum];
    std::thread recv_insertion_threads[kMaxRpcCoreNum];
    rdmacm::multicast::multicastCM * cm;
    std::atomic<int> recv_queue_blocker{0};
    KVCache * kvCache;
};