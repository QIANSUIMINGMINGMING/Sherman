#include "mc.h"

namespace rdmacm {
namespace multicast {

struct rdma_event_channel *create_first_event_channel() {
    struct rdma_event_channel *channel;

	channel = rdma_create_event_channel();
	if (!channel) {
		if (errno == ENODEV)
			fprintf(stderr, "No RDMA devices were detected\n");
		else
			perror("failed to create RDMA CM event channel");
	}
	return channel;
}

int get_addr(std::string dst, struct sockaddr *addr) {
    struct addrinfo *res;
	int ret;

	ret = getaddrinfo(dst.c_str(), NULL, NULL, &res);
	if (ret) {
		printf("getaddrinfo failed (%s) - invalid hostname or IP address\n", gai_strerror(ret));
		return ret;
	}

	memcpy(addr, res->ai_addr, res->ai_addrlen);
	freeaddrinfo(res);
	return ret;
}

int verify_port(struct multicast_node * node) {
    struct ibv_port_attr port_attr;
    int ret;

    ret = ibv_query_port(node->cma_id->verbs, node->cma_id->port_num, &port_attr);
    if (ret) {
        perror("failed to query port");
        return ret;
    }

    if (port_attr.state != IBV_PORT_ACTIVE) {
        fprintf(stderr, "port %d is not active\n", node->cma_id->port_num);
        return -1;
    }

    return 0;
} 

multicastCM::multicastCM(): mcIp(FLAGS_mcIp), mcGroups(FLAGS_mcGroups), isSender(FLAGS_mcIsSender), mbr(FLAGS_dramGB * FLAGS_rdmaMemoryFactor * 1024 * 1024 * 1024) {
    connects_left = mcGroups;

    if (alloc_nodes(mcGroups))
        exit(1);

    pthread_create(&cmathread, NULL, cma_thread_manager, this);

    /*
    * Pause to give SM chance to configure switches.  We don't want to
    * handle reliability issue in this simple test program.
    */
    sleep(5);    
};

multicastCM::~multicastCM() {
    for (int i = 0; i < mcGroups; i++) {
        destroy_node(&nodes[i]);
    }
}

int multicastCM::alloc_nodes (int connections) {
    int ret, i;
    nodes = (multicast_node*)mbr.allocate(sizeof(struct multicast_node) * connections);
    if (!nodes) {
        fprintf(stderr, "failed to allocate memory for test nodes\n");
        return -ENOMEM;
    }

    memset(nodes, 0, sizeof(struct multicast_node) * connections);
    for (i = 0; i< connections; i++) {
        nodes[i].id = i;
        nodes[i].dst_addr = (struct sockaddr *) &nodes[i].dst_in;
        
        std::string nodeMcIp = mcIp;
        std::string::size_type pos = nodeMcIp.find_last_of(".");
        nodeMcIp = nodeMcIp.substr(0, pos+1);
        nodeMcIp += std::to_string(i+1);
        ret = get_addr(nodeMcIp, (struct sockaddr *) &nodes[i].dst_in);
        if (ret) {
            fprintf(stderr, "failed to get destination address\n");
            return ret;
        }

        nodes[i].channel = create_first_event_channel();
        if (!nodes[i].channel) {
            fprintf(stderr, "failed to create RDMA CM event channel\n");
            return -1;
        }
        ret = rdma_create_id(nodes[i].channel, &nodes[i].cma_id, &nodes[i], RDMA_PS_UDP);
        if (ret) {
            fprintf(stderr, "failed to create RDMA CM ID\n");
            return ret; 
        }
        ret = rdma_resolve_addr(nodes[i].cma_id, NULL, nodes[i].dst_addr, 2000);
        if (ret) {
            perror("mckey: resolve addr failure");
            return ret;
        }

        struct rdma_cm_event *event;

        while (!nodes[i].connected && !ret) {
            ret = rdma_get_cm_event(nodes[i].channel, &event);
            if (!ret) {
                ret = cma_handler(event->id, event);
                rdma_ack_cm_event(event);
            }
        }
    }
    return 0;
}


int multicastCM::init_node(struct multicast_node *node) {
    struct ibv_qp_init_attr_ex init_qp_attr_ex;
    int cqe, ret = 0;

    if (pd == NULL) {
        pd = ibv_alloc_pd(node->cma_id->verbs);
        if (!pd) {
            fprintf(stderr, "failed to allocate PD\n");
            return -1;
        }

        mr = ibv_reg_mr(pd, mbr.getUnderlyingBuffer(), mbr.getBufferSize(), IBV_ACCESS_LOCAL_WRITE);
        if (!mr) {
            fprintf(stderr, "failed to register MR\n");
            return -1;
        } 
    }

    // cqe = message_count ? message_count * 2 : 2;
    cqe = 32;
    node->send_cq = ibv_create_cq(node->cma_id->verbs, cqe, NULL, NULL, 0);
    node->recv_cq = ibv_create_cq(node->cma_id->verbs, cqe, NULL, NULL, 0);

    if (!node->send_cq || !node->recv_cq) {
        ret = -ENOMEM;
        printf("mckey: unable to create CQ\n");
        return ret;
    }

    memset(&init_qp_attr_ex, 0, sizeof(init_qp_attr_ex));
    init_qp_attr_ex.cap.max_send_wr = 32;
    init_qp_attr_ex.cap.max_recv_wr = 32;
    init_qp_attr_ex.cap.max_send_sge = 1;
    init_qp_attr_ex.cap.max_recv_sge = 1;
    init_qp_attr_ex.qp_context = node;
    init_qp_attr_ex.sq_sig_all = 0;
    init_qp_attr_ex.qp_type = IBV_QPT_UD;
    init_qp_attr_ex.send_cq = node->send_cq;
    init_qp_attr_ex.recv_cq = node->recv_cq;

    init_qp_attr_ex.comp_mask = IBV_QP_INIT_ATTR_CREATE_FLAGS|IBV_QP_INIT_ATTR_PD;
    init_qp_attr_ex.pd = pd;
    init_qp_attr_ex.create_flags = IBV_QP_CREATE_BLOCK_SELF_MCAST_LB;

    ret = rdma_create_qp_ex(node->cma_id, &init_qp_attr_ex);

    if (ret) {
        perror("mckey: unable to create QP");
        return ret;
    }

    int message_size = 100;
    int message_count = 10;
    ret = create_message(node, message_size, message_count);
    if (ret) {
        printf("mckey: failed to create messages: %d\n", ret);
        return ret;
    }
    return 0;
}

void multicastCM::destroy_node(struct multicast_node *node) {
    if (!node->cma_id)
        return;

    if (node->ah)
        ibv_destroy_ah(node->ah);

    if (node->cma_id->qp)
        rdma_destroy_qp(node->cma_id);

    if (node->send_cq)
        ibv_destroy_cq(node->send_cq);
    
    if (node->recv_cq) {
        ibv_destroy_cq(node->recv_cq);
    }

    /* Destroy the RDMA ID after all device resources */
    rdma_destroy_id(node->cma_id);
    rdma_destroy_event_channel(node->channel);
} 

int multicastCM::create_message(struct multicast_node *node, int message_size, int message_count) {
    if (!message_size)
        message_count = 0;

    if (!message_count)
        return 0;

    node->messages = (uint8_t *)mbr.allocate(message_size * message_count);
    return 0;
}

int multicastCM::poll_scqs(int connections, int message_count) {
    return poll_cqs(connections, message_count, SEND);
}

int multicastCM::poll_rcqs(int connections, int message_count) {
    return poll_cqs(connections, message_count, RECV);
}

int multicastCM::poll_cqs(int connections, int message_count, enum SR sr) {
    struct ibv_wc wc[8];
    int done, i, ret;

    for (i = 0; i < connections; i++) {
        if (!nodes[i].connected)
            continue;

        struct ibv_cq *cq = sr == SEND ? nodes[i].send_cq : nodes[i].recv_cq;

        for (done = 0; done < message_count; done += ret) {
            ret = ibv_poll_cq(cq, 8, wc);
            if (ret < 0) {
                printf("mckey: failed polling CQ: %d\n", ret);
                return ret;
            }
        }
    }
    return 0;
}

int multicastCM::handle_recv(struct multicast_node *node) {
    return 0;
}

int multicastCM::cma_handler(struct rdma_cm_id *cma_id, struct rdma_cm_event *event) {
    int ret = 0;
    multicast_node * m_node = static_cast<multicast_node *> (cma_id->context);
    switch (event->event) {
        case RDMA_CM_EVENT_ADDR_RESOLVED:
            ret = addr_handler(m_node);
            break;
        case RDMA_CM_EVENT_MULTICAST_JOIN:
            ret = join_handler(m_node, &event->param.ud);
            break;
        case RDMA_CM_EVENT_ADDR_ERROR:
        case RDMA_CM_EVENT_ROUTE_ERROR:
        case RDMA_CM_EVENT_MULTICAST_ERROR:
            printf("mckey: event: %s, error: %d\n",
                    rdma_event_str(event->event), event->status);
            // connect_error();
            ret = event->status;
            break;
        case RDMA_CM_EVENT_DEVICE_REMOVAL:
            /* Cleanup will occur after test completes. */
            break;
        default:
            break;
    }
    return ret;
}

int multicastCM::addr_handler(struct multicast_node *node) {
    int ret;
    struct rdma_cm_join_mc_attr_ex mc_attr;

    ret = init_node(node);
    if (ret)
        return ret;

    if (!isSender) {
        ret = post_recvs(node);
        if (ret)
            return ret;
    }

    mc_attr.comp_mask =
        RDMA_CM_JOIN_MC_ATTR_ADDRESS | RDMA_CM_JOIN_MC_ATTR_JOIN_FLAGS;
    mc_attr.addr = node->dst_addr;
    mc_attr.join_flags = RDMA_MC_JOIN_FLAG_FULLMEMBER;

    ret = rdma_join_multicast_ex(node->cma_id, &mc_attr, node);

    if (ret) {
        perror("mckey: failure joining");
        return ret;
    }
    return 0;
}

int multicastCM::join_handler(struct multicast_node *node, struct rdma_ud_param *param) {
    char buf[40];
    inet_ntop(AF_INET6, param->ah_attr.grh.dgid.raw, buf, 40);
    printf("mckey: joined dgid: %s mlid 0x%x sl %d\n", buf,
        param->ah_attr.dlid, param->ah_attr.sl);

    node->remote_qpn = param->qp_num;
    node->remote_qkey = param->qkey;
    node->ah = ibv_create_ah(pd, &param->ah_attr);
    if (!node->ah) {
        printf("mckey: failure creating address handle\n");
        return -1;
    }

    node->connected = 1;
    connects_left--;
    return 0;
}

int multicastCM::post_recvs(struct multicast_node *node) {
    struct ibv_recv_wr recv_wr, *recv_failure;
    struct ibv_sge sge;
    int i, ret = 0;

    int message_size = 100;
    int message_count = 10;

    if (!message_count)
        return 0;

    recv_wr.next = NULL;
    recv_wr.sg_list = &sge;
    recv_wr.num_sge = 1;
    recv_wr.wr_id = (uintptr_t) node;

    sge.length = message_size + sizeof(struct ibv_grh);
    sge.lkey = mr->lkey; 
    sge.addr = (uintptr_t) node->messages;

    for (i = 0; i < message_count && !ret; i++ ) {
        ret = ibv_post_recv(node->cma_id->qp, &recv_wr, &recv_failure);
        if (ret) {
            printf("failed to post receives: %d\n", ret);
            break;
        }
    }
    return ret;
}

int multicastCM::post_sends(struct multicast_node *node, int signal_flag) {
    struct ibv_send_wr send_wr, *bad_send_wr;
    struct ibv_sge sge;
    int i, ret = 0;

    send_wr.next = NULL;
    send_wr.sg_list = &sge;
    send_wr.num_sge = 1;
    send_wr.opcode = IBV_WR_SEND_WITH_IMM;
    send_wr.send_flags = signal_flag;
    send_wr.wr_id = (unsigned long)node;
    send_wr.imm_data = htobe32(node->cma_id->qp->qp_num);

    send_wr.wr.ud.ah = node->ah;
    send_wr.wr.ud.remote_qpn = node->remote_qpn;
    send_wr.wr.ud.remote_qkey = node->remote_qkey;

    int message_size = 100;
    int message_count = 10;

    sge.length = message_size;
    sge.lkey = mr->lkey;
    sge.addr = (uintptr_t) node->messages;

    for (i = 0; i < message_count && !ret; i++) {
        ret = ibv_post_send(node->cma_id->qp, &send_wr, &bad_send_wr);
        if (ret)
            printf("failed to post sends: %d\n", ret);
    }
    return ret;
}

int multicastCM::resolve_nodes() {
    int i, ret;

    for (i = 0; i < mcGroups; i++) {
        ret = rdma_resolve_addr(nodes[i].cma_id, NULL, nodes[i].dst_addr, 2000);
        if (ret) {
            perror("mckey: resolve addr failure");
            return ret;
        }
    }
    return 0;
}

void *multicastCM::cma_thread_worker(void *arg) {
    bindCore(95);
    struct rdma_cm_event *event;
    int ret;

    struct multicast_node *node = static_cast<multicast_node *>(arg);
    while (1) {
        ret = rdma_get_cm_event(node->channel, &event);
        if (ret) {
            perror("rdma_get_cm_event");
            break;
        }

        switch (event->event) {
            case RDMA_CM_EVENT_MULTICAST_ERROR:
            case RDMA_CM_EVENT_ADDR_CHANGE:
                printf("mckey: event: %s, status: %d\n",
                    rdma_event_str(event->event), event->status);
                break;
            default:
                break;
        }

        rdma_ack_cm_event(event);
    }

    return NULL;
}

void *multicastCM::cma_thread_manager(void *arg) {
    bindCore(95);
    multicastCM *mckey = static_cast<multicastCM *>(arg);
    pthread_t workers[mckey->getGroupSize()];

    for (int i = 0; i < mckey->getGroupSize(); i++) {
        // create worker threads for each group
        int ret;
        ret = pthread_create(&workers[i], NULL, cma_thread_worker, mckey->getNode(i));
        if (ret) {
            perror("mckey: failed to create worker thread");
            return NULL;
        }
    }

    for (int i = 0; i < mckey->getGroupSize(); i++) {
        // join worker threads for each group
        int ret;
        ret = pthread_join(workers[i], NULL);
        if (ret) {
            perror("mckey: failed to join worker thread");
            return NULL;
        }
    }

    return NULL;
}

int multicastCM::test_node() {
    int i, ret;
    int message_count = 10;

    printf("mckey: starting %s\n", FLAGS_mcIsSender ? "client" : "server");

    printf("mckey: joining\n");
    // ret = resolve_nodes();
    // if (ret) {
    //     perror("mckey: resolve nodes failure");
    //     return ret;
    // }

    // ret = connect_events(&nodes[0]);
    // if (ret) {
    //     perror("mckey: connect events failure");
    //     return ret;
    // }

    // pthread_create(&cmathread, NULL, cma_thread_manager, this);

    // /*
    // * Pause to give SM chance to configure switches.  We don't want to
    // * handle reliability issue in this simple test program.
    // */
    // sleep(5);

    if (message_count) {
        if (isSender) {
            printf("initiating data transfers\n");
            for (i = 0; i < mcGroups; i++) {
                ret = post_sends(&nodes[i], 0);
                if (ret) {
                    perror("mckey: failure sending");
                    return ret;
                }
            }
        } else {
            printf("receiving data transfers\n");
            ret = poll_rcqs(mcGroups, message_count);
            if (ret) {
                perror("mckey: failure receiving");
                return ret;
            }
        }
        printf("data transfers complete\n");
    }

    return 0;
}

} // namespace multicast
} // namespace rdmacm
