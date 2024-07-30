#pragma once

#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <errno.h>
#include <sys/types.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <netdb.h>
#include <unistd.h>
#include <getopt.h>
#include <thread>
#include <rdma/rdma_cma.h>
#include <infiniband/ib.h>
#include <unordered_map>
#include <vector>

#include "memManager.h"
#include "flags.h"
#include "Common.h"
#include "KVCache.h"
#include "third_party/readerwritercircularbuffer.h"

namespace rdmacm {
namespace multicast {

constexpr int kMcCardinality =
    (kMcPageSize - sizeof(int) - sizeof(int)) /
    sizeof(KVTS);

struct TransferObj {
    int psn{-1};
    int node_id;
    KVTS elements[kMcCardinality];
};

struct multicast_node {
    int id;
    struct rdma_event_channel *channel;
    struct rdma_cm_id *cma_id;
    int connected;

    struct ibv_cq *send_cq;
    struct ibv_cq *recv_cq;
    struct ibv_ah *ah;

    struct sockaddr_storage dst_in;
    struct sockaddr *dst_addr;

    uint32_t remote_qpn;
    uint32_t remote_qkey;
    uint8_t * send_messages;
    uint8_t * recv_messages;
    struct ibv_recv_wr recv_wr[kMcMaxPostList];
    struct ibv_sge recv_sgl[kMcMaxPostList];
    struct ibv_send_wr send_wr[kMcMaxPostList];
    struct ibv_sge send_sgl[kMcMaxPostList];
    int send_pos{0};
};

struct rdma_event_channel *create_first_event_channel();
int get_addr(std::string dst, struct sockaddr *addr);
int verify_port(struct multicast_node *node);

enum SR {SEND, RECV};

// package loss
typedef std::pair<uint64_t, uint64_t> Gpsn; // <nodeid, psn>
uint64_t check_package_loss(uint64_t psn_num);

class multicastCM {
public:
	multicastCM();
    ~multicastCM();

    int test_node();
    int getGroupSize(){return mcGroups;}
    struct multicast_node *getNode(int i) {return &nodes[i];}
    moodycamel::BlockingReaderWriterCircularBuffer<TransferObj *> *getPageQueue(int i) {return pageQueues[i];}
    void send_message(int tid, int pos);
    int get_pos(int tid, TransferObj *&message_address);
    void print_self() { 
    }

private:
    int init_node(struct multicast_node *node);
    int create_message(struct multicast_node *node, int message_count = kMcMaxPostList);
    void destroy_node(struct multicast_node *node); 
    int alloc_nodes (int connections);

    int poll_scqs(int connections, int message_count);
    int poll_rcqs(int connections, int message_count);
	int poll_cqs(int connections, int message_count, enum SR sr);
    int post_recvs(struct multicast_node *node);
    int post_sends(struct multicast_node *node, int signal_flag);

    // void send_message(multicast_node *node, uint8_t *message);

    void handle_recv(struct multicast_node *node, int id);

    int cma_handler(struct rdma_cm_id *cma_id, struct rdma_cm_event *event);
	int addr_handler(struct multicast_node *node);
    int join_handler(struct multicast_node *node, struct rdma_ud_param *param);

    int init_recvs(struct multicast_node *node);

    int connect_events(struct multicast_node *node);
    int resolve_nodes();

    static void *cma_thread_worker(void *arg);
    static void *cma_thread_manager(void *arg);
    static void *psn_checker(void *arg);

    static void *mc_maintainer(uint16_t id, multicastCM *me);

private:
    pthread_t cmathread;
    std::thread maintainers[kMaxRpcCoreNum];
    moodycamel::BlockingReaderWriterCircularBuffer<TransferObj *> *pageQueues[kMaxRpcCoreNum];

    struct multicast_node *nodes;
    int conn_index;
    int connects_left;

	std::string mcIp;
	int mcGroups;
    struct ibv_mr *mr;
    struct ibv_pd *pd;
    utils::SynchronizedMonotonicBufferRessource mbr; 
};


} // namespace multicast
} // namespace rdmacm