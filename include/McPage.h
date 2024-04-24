#pragma once

#include "Common.h"
#include "KVCache.h"
#include "mc.h"

struct PageMeta {
    int psn;
    int elen;
};

//40 ud padding
constexpr int kMcCardinality =
    (kMcPageSize - sizeof(int) - 40) /
    sizeof(KVTS);

struct TransferObj {
    int psn{-1};
    KVTS elements[kMcCardinality];
};


class McPage {
public:
    McPage() {
        
    }

    void insert(Key k, TS ts, Value v) {
        
        if (cur_psn == -1) {
            cur_psn = global_psn->fetch_add(1);
            TransferObj * next = new TransferObj();
            next->psn = cur_psn;
            buffer[cur_psn] = next;
        }

        buffer[cur_psn]->elements[cur_size++] = {k, ts, v};
        if (cur_size == kMcCardinality) {
            emit(buffer[cur_psn]);
            cur_psn = global_psn->fetch_add(1);
            cur_size = 0;
            TransferObj * next = new TransferObj();
            next->psn = cur_psn;
            buffer[cur_psn] = next;
        }
    }

    bool locate(int psn) {
        if(buffer.find(psn)!= buffer.end()) {
            // acquire a new psn
            emit(buffer[psn]);
            int new_psn = global_psn->fetch_add(1);
            buffer[psn]->psn = new_psn;
            buffer[new_psn] = buffer[psn];
            buffer.erase(psn);
            return true;
        }
        return false;
    }

    void clear_old (int psn_bar) {
        for (auto it = buffer.begin(); it != buffer.end(); ) {
            if (it->first < psn_bar) {
                delete it->second;
                buffer.erase(it++);
            } else {
                ++it;
            }
        }
    }

private:
    void emit(TransferObj * obj) {

    }

    int cur_psn{-1};
    int cur_size = 0;

    std::map<int, TransferObj*> buffer; //psn, page
    std::atomic<int> * global_psn;

};