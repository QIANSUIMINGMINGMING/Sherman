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

#include "memManager.h"
#include "flags.h"
#include "Common.h"

namespace rdmacm {
namespace multicast {

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
    uint8_t * messages;
};

struct rdma_event_channel *create_first_event_channel();
int get_addr(std::string dst, struct sockaddr *addr);
int verify_port(struct multicast *node);

enum SR {SEND, RECV};

class multicastCM {
public:
	multicastCM();
    ~multicastCM();

    int test_node();
    int getGroupSize(){return mcGroups;}
    struct multicast_node *getNode(int i) {return &nodes[i];}

private:
    int init_node(struct multicast_node *node);
    int create_message(struct multicast_node *node, int message_size, int message_count);
    void destroy_node(struct multicast_node *node); 
    int alloc_nodes (int connections);

    int poll_scqs(int connections, int message_count);
    int poll_rcqs(int connections, int message_count);
	int poll_cqs(int connections, int message_count, enum SR sr);
    int post_recvs(struct multicast_node *node);
    int post_sends(struct multicast_node *node, int signal_flag);

    int handle_recv(struct multicast_node *node);

    int cma_handler(struct rdma_cm_id *cma_id, struct rdma_cm_event *event);
	int addr_handler(struct multicast_node *node);
    int join_handler(struct multicast_node *node, struct rdma_ud_param *param);

    int connect_events(struct multicast_node *node);
    int resolve_nodes();

    static void *cma_thread_worker(void *arg);
    static void *cma_thread_manager(void *arg);

    static void *mc_maintainer(void *args[]);

private:
    pthread_t cmathread;
    std::thread maintainers[kMaxRpcCoreNum];

    struct multicast_node *nodes;
    int conn_index;
    int connects_left;

	std::string mcIp;
	int mcGroups;
    struct ibv_mr *mr;
    struct ibv_pd *pd;

    bool isSender;
    utils::SynchronizedMonotonicBufferRessource mbr; 
};


} // namespace multicast
} // namespace rdmacm