/*
 * Copyright (c) 2005 Topspin Communications.  All rights reserved.
 * Copyright (c) 2006 Cisco Systems.  All rights reserved.
 *
 * This software is available to you under a choice of one of two
 * licenses.  You may choose to be licensed under the terms of the GNU
 * General Public License (GPL) Version 2, available from the file
 * COPYING in the main directory of this source tree, or the
 * OpenIB.org BSD license below:
 *
 *     Redistribution and use in source and binary forms, with or
 *     without modification, are permitted provided that the following
 *     conditions are met:
 *
 *      - Redistributions of source code must retain the above
 *        copyright notice, this list of conditions and the following
 *        disclaimer.
 *
 *      - Redistributions in binary form must reproduce the above
 *        copyright notice, this list of conditions and the following
 *        disclaimer in the documentation and/or other materials
 *        provided with the distribution.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS
 * BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN
 * ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
 * CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/param.h>
#include <sys/time.h>
#include <stdlib.h>
#include <getopt.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <time.h>
#include <math.h>


#include <infiniband/verbs.h>

#include "msgType.h"
#include "HashMap.h"

#define WC_BATCH (10)
#define _GNU_SOURCE
#define ITERS 5555
#define MAX_ITERS 10

enum {
    PINGPONG_RECV_WRID = 1,
    PINGPONG_SEND_WRID = 2,
};

#define BUFFER_MAP_SIZE 128
#define EAGER_MAX_SIZE 4096 // TODO: +20 ??
#define MAX_SIZE  129 * 4096 // 128 for send, 1 for recv
#define MAX_SIZE  129 * 4096 // 128 for send, 1 for recv
#define MICRO_SEC  1e6
#define BYTE_TO_BIT  8
#define WARMUP_NUM_OF_SENDS  5000
#define CONVERGE  0.01

int bufferMap[BUFFER_MAP_SIZE] = {0};
map_t serverMap;
map_t valuePointerToMr; // contains pairs of values and their memory region (for rendez-vous)

static int page_size;

static void usage(const char *argv0) {
    printf("Usage:\n");
    printf("  %s            start a server and wait for connection\n", argv0);
    printf("  %s <host>     connect to server at <host>\n", argv0);
    printf("\n");
    printf("Options:\n");
    printf("  -p, --port=<port>      listen on/connect to port <port> (default 18515)\n");
    printf("  -d, --ib-dev=<dev>     use IB device <dev> (default first device found)\n");
    printf("  -i, --ib-port=<port>   use port <port> of IB device (default 1)\n");
    printf("  -s, --size=<size>      size of message to exchange (default 4096)\n");
    printf("  -m, --mtu=<size>       path MTU (default 1024)\n");
    printf("  -r, --rx-depth=<dep>   number of receives to post at a time (default 500)\n");
    printf("  -n, --iters=<iters>    number of exchanges (default 1000)\n");
    printf("  -l, --sl=<sl>          service level value\n");
    printf("  -e, --events           sleep on CQ events (default poll)\n");
    printf("  -g, --gid-idx=<gid index> local port gid index\n");
}

struct pingpong_context {
    struct ibv_context *context;
    struct ibv_comp_channel *channel;
    struct ibv_pd *pd;
    struct ibv_mr *mr;
    struct ibv_cq *cq;
    struct ibv_qp *qp;
    void *buf;
//    int size;
    int rx_depth;
    int routs;
    struct ibv_port_attr portinfo;
};

struct pingpong_dest {
    int lid;
    int qpn;
    int psn;
    union ibv_gid gid;
};

enum ibv_mtu pp_mtu_to_enum(int mtu) {
    switch (mtu) {
        case 256:
            return IBV_MTU_256;
        case 512:
            return IBV_MTU_512;
        case 1024:
            return IBV_MTU_1024;
        case 2048:
            return IBV_MTU_2048;
        case 4096:
            return IBV_MTU_4096;
        default:
            return -1;
    }
}

uint16_t pp_get_local_lid(struct ibv_context *context, int port) {
    struct ibv_port_attr attr;

    if (ibv_query_port(context, port, &attr))
        return 0;

    return attr.lid;
}

int pp_get_port_info(struct ibv_context *context, int port,
                     struct ibv_port_attr *attr) {
    return ibv_query_port(context, port, attr);
}

void wire_gid_to_gid(const char *wgid, union ibv_gid *gid) {
    char tmp[9];
    uint32_t v32;
    int i;

    for (tmp[8] = 0, i = 0; i < 4; ++i) {
        memcpy(tmp, wgid + i * 8, 8);
        sscanf(tmp, "%x", &v32);
        *(uint32_t *) (&gid->raw[i * 4]) = ntohl(v32);
    }
}

void gid_to_wire_gid(const union ibv_gid *gid, char wgid[]) {
    int i;

    for (i = 0; i < 4; ++i)
        sprintf(&wgid[i * 8], "%08x", htonl(*(uint32_t *) (gid->raw + i * 4)));
}

static int pp_connect_ctx(struct pingpong_context *ctx, int port, int my_psn,
                          enum ibv_mtu mtu, int sl,
                          struct pingpong_dest *dest, int sgid_idx) {
    struct ibv_qp_attr attr = {
            .qp_state        = IBV_QPS_RTR,
            .path_mtu        = mtu,
            .dest_qp_num        = dest->qpn,
            .rq_psn            = dest->psn,
            .max_dest_rd_atomic    = 1,
            .min_rnr_timer        = 12,
            .ah_attr        = {
                    .is_global    = 0,
                    .dlid        = dest->lid,
                    .sl        = sl,
                    .src_path_bits    = 0,
                    .port_num    = port
            }
    };

    if (dest->gid.global.interface_id) {
        attr.ah_attr.is_global = 1;
        attr.ah_attr.grh.hop_limit = 1;
        attr.ah_attr.grh.dgid = dest->gid;
        attr.ah_attr.grh.sgid_index = sgid_idx;
    }
    if (ibv_modify_qp(ctx->qp, &attr,
                      IBV_QP_STATE |
                      IBV_QP_AV |
                      IBV_QP_PATH_MTU |
                      IBV_QP_DEST_QPN |
                      IBV_QP_RQ_PSN |
                      IBV_QP_MAX_DEST_RD_ATOMIC |
                      IBV_QP_MIN_RNR_TIMER)) {
        fprintf(stderr, "Failed to modify QP to RTR\n");
        return 1;
    }

    attr.qp_state = IBV_QPS_RTS;
    attr.timeout = 14;
    attr.retry_cnt = 7;
    attr.rnr_retry = 7;
    attr.sq_psn = my_psn;
    attr.max_rd_atomic = 1;
    if (ibv_modify_qp(ctx->qp, &attr,
                      IBV_QP_STATE |
                      IBV_QP_TIMEOUT |
                      IBV_QP_RETRY_CNT |
                      IBV_QP_RNR_RETRY |
                      IBV_QP_SQ_PSN |
                      IBV_QP_MAX_QP_RD_ATOMIC)) {
        fprintf(stderr, "Failed to modify QP to RTS\n");
        return 1;
    }

    return 0;
}

static struct pingpong_dest *pp_client_exch_dest(const char *servername, int port,
                                                 const struct pingpong_dest *my_dest) {
    struct addrinfo *res, *t;
    struct addrinfo hints = {
            .ai_family   = AF_INET,
            .ai_socktype = SOCK_STREAM
    };
    char *service;
    char msg[sizeof "0000:000000:000000:00000000000000000000000000000000"];
    int n;
    int sockfd = -1;
    struct pingpong_dest *rem_dest = NULL;
    char gid[33];

    if (asprintf(&service, "%d", port) < 0)
        return NULL;

    n = getaddrinfo(servername, service, &hints, &res);

    if (n < 0) {
        fprintf(stderr, "%s for %s:%d\n", gai_strerror(n), servername, port);
        free(service);
        return NULL;
    }

    for (t = res; t; t = t->ai_next) {
        sockfd = socket(t->ai_family, t->ai_socktype, t->ai_protocol);
        if (sockfd >= 0) {
            if (!connect(sockfd, t->ai_addr, t->ai_addrlen))
                break;
            close(sockfd);
            sockfd = -1;
        }
    }

    freeaddrinfo(res);
    free(service);

    if (sockfd < 0) {
        fprintf(stderr, "Couldn't connect to %s:%d\n", servername, port);
        return NULL;
    }

    gid_to_wire_gid(&my_dest->gid, gid);
    sprintf(msg, "%04x:%06x:%06x:%s", my_dest->lid, my_dest->qpn, my_dest->psn, gid);
    if (write(sockfd, msg, sizeof msg) != sizeof msg) {
        fprintf(stderr, "Couldn't send local address\n");
        goto out;
    }

    if (read(sockfd, msg, sizeof msg) != sizeof msg) {
        perror("client read");
        fprintf(stderr, "Couldn't read remote address\n");
        goto out;
    }

    write(sockfd, "done", sizeof "done");

    rem_dest = malloc(sizeof *rem_dest);
    if (!rem_dest)
        goto out;

    sscanf(msg, "%x:%x:%x:%s", &rem_dest->lid, &rem_dest->qpn, &rem_dest->psn, gid);
    wire_gid_to_gid(gid, &rem_dest->gid);

    out:
    close(sockfd);
    return rem_dest;
}

static struct pingpong_dest *pp_server_exch_dest(struct pingpong_context *ctx,
                                                 int ib_port, enum ibv_mtu mtu,
                                                 int port, int sl,
                                                 const struct pingpong_dest *my_dest,
                                                 int sgid_idx) {
    struct addrinfo *res, *t;
    struct addrinfo hints = {
            .ai_flags    = AI_PASSIVE,
            .ai_family   = AF_INET,
            .ai_socktype = SOCK_STREAM
    };
    char *service;
    char msg[sizeof "0000:000000:000000:00000000000000000000000000000000"];
    int n;
    int sockfd = -1, connfd;
    struct pingpong_dest *rem_dest = NULL;
    char gid[33];

    if (asprintf(&service, "%d", port) < 0)
        return NULL;

    n = getaddrinfo(NULL, service, &hints, &res);

    if (n < 0) {
        fprintf(stderr, "%s for port %d\n", gai_strerror(n), port);
        free(service);
        return NULL;
    }

    for (t = res; t; t = t->ai_next) {
        sockfd = socket(t->ai_family, t->ai_socktype, t->ai_protocol);
        if (sockfd >= 0) {
            n = 1;

            setsockopt(sockfd, SOL_SOCKET, SO_REUSEADDR, &n, sizeof n);

            if (!bind(sockfd, t->ai_addr, t->ai_addrlen))
                break;
            close(sockfd);
            sockfd = -1;
        }
    }

    freeaddrinfo(res);
    free(service);

    if (sockfd < 0) {
        fprintf(stderr, "Couldn't listen to port %d\n", port);
        return NULL;
    }

    listen(sockfd, 1);
    connfd = accept(sockfd, NULL, 0);
    close(sockfd);
    if (connfd < 0) {
        fprintf(stderr, "accept() failed\n");
        return NULL;
    }

    n = read(connfd, msg, sizeof msg);
    if (n != sizeof msg) {
        perror("server read");
        fprintf(stderr, "%d/%d: Couldn't read remote address\n", n, (int) sizeof msg);
        goto out;
    }

    rem_dest = malloc(sizeof *rem_dest);
    if (!rem_dest)
        goto out;

    sscanf(msg, "%x:%x:%x:%s", &rem_dest->lid, &rem_dest->qpn, &rem_dest->psn, gid);
    wire_gid_to_gid(gid, &rem_dest->gid);

    if (pp_connect_ctx(ctx, ib_port, my_dest->psn, mtu, sl, rem_dest, sgid_idx)) {
        fprintf(stderr, "Couldn't connect to remote QP\n");
        free(rem_dest);
        rem_dest = NULL;
        goto out;
    }


    gid_to_wire_gid(&my_dest->gid, gid);
    sprintf(msg, "%04x:%06x:%06x:%s", my_dest->lid, my_dest->qpn, my_dest->psn, gid);
    if (write(connfd, msg, sizeof msg) != sizeof msg) {
        fprintf(stderr, "Couldn't send local address\n");
        free(rem_dest);
        rem_dest = NULL;
        goto out;
    }

    read(connfd, msg, sizeof msg);

    out:
    close(connfd);
    return rem_dest;
}

#include <sys/param.h>
#include "HashMap.h"
/*
 * ibv_device - NIC we are using
 * size - the size of the message, we can define here 1MB buffer.
 * rx_depth -
 *
 * */
/**
 *
 * @param ib_dev
 * @param size
 * @param rx_depth
 * @param tx_depth
 * @param port
 * @param use_event
 * @param is_server
 * @return
 */
static struct pingpong_context *pp_init_ctx(struct ibv_device *ib_dev, int size,
                                            int rx_depth, int tx_depth, int port,
                                            int use_event, int is_server) {

    struct pingpong_context *ctx;

    ctx = calloc(1, sizeof *ctx);
    if (!ctx)
        return NULL;

//    ctx->size = size;
    ctx->rx_depth = rx_depth;
    ctx->routs = rx_depth;

    ctx->buf = malloc(roundup(size, page_size));
    if (!ctx->buf) {
        fprintf(stderr, "Couldn't allocate work buf.\n");
        return NULL;
    }

    memset(ctx->buf, 0x7b + is_server, size);
    // lunching the NIC to work with our program
    ctx->context = ibv_open_device(ib_dev);
    if (!ctx->context) {
        fprintf(stderr, "Couldn't get context for %s\n",
                ibv_get_device_name(ib_dev));
        return NULL;
    }

    // create protection domain - to make sure we are showing only relevant data.
    ctx->pd = ibv_alloc_pd(ctx->context);
    if (!ctx->pd) {
        fprintf(stderr, "Couldn't allocate PD\n");
        return NULL;
    }

    // buffer registration
    ctx->mr = ibv_reg_mr(ctx->pd, ctx->buf, size, IBV_ACCESS_LOCAL_WRITE);
    if (!ctx->mr) {
        fprintf(stderr, "Couldn't register MR\n");
        return NULL;
    }

    ctx->cq = ibv_create_cq(ctx->context, rx_depth + tx_depth, NULL,
                            ctx->channel, 0);
    if (!ctx->cq) {
        fprintf(stderr, "Couldn't create CQ\n");
        return NULL;
    }

    {
        struct ibv_qp_init_attr attr = {
                .send_cq = ctx->cq,
                .recv_cq = ctx->cq,
                .cap     = {
                        .max_send_wr  = tx_depth,
                        .max_recv_wr  = rx_depth,
                        .max_send_sge = 1,
                        .max_recv_sge = 1
                },
                .qp_type = IBV_QPT_RC
        };

        ctx->qp = ibv_create_qp(ctx->pd, &attr);
        if (!ctx->qp) {
            fprintf(stderr, "Couldn't create QP\n");
            return NULL;
        }
    }

    {
        struct ibv_qp_attr attr = {
                .qp_state        = IBV_QPS_INIT,
                .pkey_index      = 0,
                .port_num        = port,
                .qp_access_flags = IBV_ACCESS_REMOTE_READ |
                                   IBV_ACCESS_REMOTE_WRITE
        };

        if (ibv_modify_qp(ctx->qp, &attr,
                          IBV_QP_STATE |
                          IBV_QP_PKEY_INDEX |
                          IBV_QP_PORT |
                          IBV_QP_ACCESS_FLAGS)) {
            fprintf(stderr, "Failed to modify QP to INIT\n");
            return NULL;
        }
    }

    return ctx;
}

int pp_close_ctx(struct pingpong_context *ctx) {
    if (ibv_destroy_qp(ctx->qp)) {
        fprintf(stderr, "Couldn't destroy QP\n");
        return 1;
    }

    if (ibv_destroy_cq(ctx->cq)) {
        fprintf(stderr, "Couldn't destroy CQ\n");
        return 1;
    }

    if (ibv_dereg_mr(ctx->mr)) {
        fprintf(stderr, "Couldn't deregister MR\n");
        return 1;
    }

    if (ibv_dealloc_pd(ctx->pd)) {
        fprintf(stderr, "Couldn't deallocate PD\n");
        return 1;
    }

    if (ctx->channel) {
        if (ibv_destroy_comp_channel(ctx->channel)) {
            fprintf(stderr, "Couldn't destroy completion channel\n");
            return 1;
        }
    }

    if (ibv_close_device(ctx->context)) {
        fprintf(stderr, "Couldn't release context\n");
        return 1;
    }

    free(ctx->buf);
    free(ctx);

    return 0;
}


static int pp_post_recv(struct pingpong_context *ctx, void * buf, unsigned long ourSize, int id) { //added unsigned long ourSize

    struct ibv_sge list = {
//            .addr    = (uintptr_t) ctx->buf,
            .addr    = (uintptr_t) buf,
//            .length = ctx->size,
            .length = ourSize,
            .lkey    = ctx->mr->lkey
    };
    struct ibv_recv_wr wr = {
            .wr_id        = id,
            .sg_list    = &list,
            .num_sge    = 1,
            .next       = NULL
    };
    struct ibv_recv_wr *bad_wr;

    if (ibv_post_recv(ctx->qp, &wr, &bad_wr))
        return 1;

    return 0;
}

static int pp_post_send(struct pingpong_context *ctx, void * buf, unsigned long ourSize, int id,
                        enum ibv_wr_opcode opcode, struct ibv_mr *mr, void * remotePtr, uint32_t rkey) { //added unsigned long ourSize

    struct ibv_sge list = {

            .addr   = (uintptr_t) (mr ? mr->addr : buf),
            .length = ourSize,
            .lkey    =  mr ? mr->lkey : ctx->mr->lkey,
    };

    struct ibv_send_wr *bad_wr, wr = {
            .wr_id        = id,
            .sg_list    = &list,
            .num_sge    = 1,
            .opcode     = mr ? opcode : IBV_WR_SEND,
            .send_flags = IBV_SEND_SIGNALED,
            .next       = NULL
    };

    if (remotePtr) {
        wr.wr.rdma.remote_addr = (uintptr_t) remotePtr;
        wr.wr.rdma.rkey = rkey;
    }

    return ibv_post_send(ctx->qp, &wr, &bad_wr);
}

/**
 * opens a connection and saves the the ctx to kv_handle
 * @param servername
 * @param kv_handle
 * @return
 */
int kv_open(char * servername, void ** kv_handle){
    struct ibv_device **dev_list;
    struct ibv_device *ib_dev;
    struct pingpong_context *ctx;
    struct pingpong_dest my_dest;
    struct pingpong_dest *rem_dest;
    char *ib_devname = NULL;
    int port = 12346;
    int ib_port = 1;
    enum ibv_mtu mtu = IBV_MTU_2048;
    int rx_depth = 100;
    int tx_depth = 100;
    int iters = 5555;
    int use_event = 0;
    int size = 1;
    int sl = 0;
    int gidx = -1;
    char gid[33];


    srand48(getpid() * time(NULL));

    page_size = sysconf(_SC_PAGESIZE);

    // ibv_get_device_list - request to use the NIC
    dev_list = ibv_get_device_list(NULL);
    if (!dev_list) {
        perror("Failed to get IB devices list");
        return 1;
    }

    ib_dev = *dev_list;
    if (!ib_dev) {
        fprintf(stderr, "No IB devices found\n");
        return 1;
    }


    /// interesting part
    // init context - save all relevant params
    ctx = pp_init_ctx(ib_dev, MAX_SIZE, rx_depth, tx_depth, ib_port, use_event,
                      !servername); //changed size to MAX_SIZE
    if (!ctx)
        return 1;


    if (pp_get_port_info(ctx->context, ib_port, &ctx->portinfo)) {
        fprintf(stderr, "Couldn't get port info\n");
        return 1;
    }

    my_dest.lid = ctx->portinfo.lid;
    if (ctx->portinfo.link_layer == IBV_LINK_LAYER_INFINIBAND && !my_dest.lid) {
        fprintf(stderr, "Couldn't get local LID\n");
        return 1;
    }

    if (gidx >= 0) {
        if (ibv_query_gid(ctx->context, ib_port, gidx, &my_dest.gid)) {
            fprintf(stderr, "Could not get local gid for gid index %d\n", gidx);
            return 1;
        }
    } else
        memset(&my_dest.gid, 0, sizeof my_dest.gid);

    my_dest.qpn = ctx->qp->qp_num;
    my_dest.psn = lrand48() & 0xffffff;
    inet_ntop(AF_INET6, &my_dest.gid, gid, sizeof gid);
    printf("  local address:  LID 0x%04x, QPN 0x%06x, PSN 0x%06x, GID %s\n",
           my_dest.lid, my_dest.qpn, my_dest.psn, gid);


    if (servername) // client code
        rem_dest = pp_client_exch_dest(servername, port, &my_dest);
    else // server code
        rem_dest = pp_server_exch_dest(ctx, ib_port, mtu, port, sl, &my_dest, gidx);

    if (!rem_dest)
        return 1;

    inet_ntop(AF_INET6, &rem_dest->gid, gid, sizeof gid);
    printf("  remote address: LID 0x%04x, QPN 0x%06x, PSN 0x%06x, GID %s\n",
           rem_dest->lid, rem_dest->qpn, rem_dest->psn, gid); //gid is the dest addr ?

    if (servername) // client code
        if (pp_connect_ctx(ctx, ib_port, my_dest.psn, mtu, sl, rem_dest, gidx))
            return 1;

    *kv_handle = ctx;
    ibv_free_device_list(dev_list);
    free(rem_dest);
    return 0;
}

/**
 * Wait for normal completion (buffers pool)
 * @param ctx
 * @param wc
 * @return
 */
int waitPoolCompletion(struct pingpong_context* ctx, struct ibv_wc * wc){
    int ne;
    do {
        ne = ibv_poll_cq(ctx->cq, 1, wc);
        if (ne == 1) {
            if (wc->wr_id < 128){
                bufferMap[wc->wr_id] = 0;
                return wc->wr_id;
            }
            ne = 0;
        }
        else if (ne < 0){
            fprintf(stderr, "poll CQ failed %d\n", ne);
            return -1;
        }

    } while (ne < 1);
}

/**
 * Wait for the completion of the FIN msg
 * @param ctx
 * @return
 */
int waitFINCompletion(struct pingpong_context* ctx){
    struct ibv_wc wc;
    int ne;
    do {
        ne = ibv_poll_cq(ctx->cq, 1, &wc);
        if (ne == 1) {
            if (strcmp(ctx->buf + (wc.wr_id * EAGER_MAX_SIZE),"FIN") == 0){
                bufferMap[wc.wr_id] = 0;
                return 0;
            }
            else {
                ne = 0;}
        }
        else if (ne < 0){
            fprintf(stderr, "poll CQ failed %d\n", ne);
            return -1;
        }
    }
        while (ne < 1);
}

/**
 * Wait for specific wr.id 128 Completion
 * @param ctx
 * @return
 */
int waitRecvCompletion128(struct pingpong_context* ctx){
    struct ibv_wc wc;
    int ne;
    do {
        ne = ibv_poll_cq(ctx->cq, 1, &wc);
        if (ne == 1) {
            if (wc.wr_id == 128){
                return wc.wr_id;
            }
            else {
                bufferMap[wc.wr_id] = 0; //now free
                ne = 0;
            }
        }
        else if (ne < 0){
            fprintf(stderr, "poll CQ failed %d\n", ne);
            return -1;
        }

    } while (ne < 1);
}

/**
 * gets the next free buffer in the buffer map
 * @param ctx
 * @return
 */
int getNextfreeBufNum(struct pingpong_context* ctx){
    struct ibv_wc wc;
    for(int i = 0; i < BUFFER_MAP_SIZE; i++)
    {
        if (bufferMap[i] == 0){
            bufferMap[i] = 1; // make occupied
            return i;
        }
    }
    return waitPoolCompletion(ctx, &wc);
}

/**
 * Server function
 * @param ctx
 * @return
 */
int handleGetReq(struct pingpong_context *ctx, char * buffer) {

    char *key = (buffer + sizeof(enum msgType));
    char * valueToSend;
    char *currBuff = ctx->buf + (128 * EAGER_MAX_SIZE); // for INFO msg

    if (hashmap_get(serverMap, key, (any_t *) &valueToSend) == MAP_MISSING){
        valueToSend = "";
    }


    if (strlen(valueToSend) <= EAGER_MAX_SIZE) {
        int est = EAGER_GET_RESPONSE;
        memcpy(currBuff, &est, sizeof(enum msgType));
        strcpy(currBuff + sizeof(enum msgType), valueToSend);
        size_t msgSize = sizeof(enum msgType) + strlen(valueToSend) + 1;
        pp_post_send(ctx, currBuff, msgSize, 128, -1, NULL, NULL, 0);
        waitRecvCompletion128(ctx); // TODO: check if necessary
    }
        //RDV
    else {
        int type = RENDEZVOUS_GET_RESPONSE;
        size_t valueLen = strlen(valueToSend) + 1;

        // starting build INFO msg
        memcpy(currBuff, &type, sizeof(enum msgType));

        struct ibv_mr *mr;
        hashmap_get(valuePointerToMr, valueToSend, (any_t *) &mr);

        memcpy(currBuff + sizeof(enum msgType), &(mr->addr), sizeof(mr->addr));
        memcpy(currBuff + sizeof(enum msgType) + sizeof(mr->addr), &(mr->rkey), sizeof(mr->rkey));
        memcpy(currBuff + sizeof(enum msgType) + sizeof(mr->addr) + sizeof((mr->rkey)), &valueLen, sizeof(size_t));
        size_t msgSize = sizeof(enum msgType) + sizeof(mr->addr) + sizeof(mr->rkey) + sizeof(size_t);

        pp_post_send(ctx, currBuff, msgSize, 128, -1, NULL, NULL, 0); //server sent the mr to read from to client
        waitRecvCompletion128(ctx);

        // wait for FIN
        pp_post_recv(ctx, ctx->buf + (128 * EAGER_MAX_SIZE), EAGER_MAX_SIZE, 128);
        waitFINCompletion(ctx);
    }
    return 0;
}


/**
 * Server function
 * @param ctx
 * @param buffer
 * @return
 */
int handleSetEagerReq(char * buffer) {

    char *key = (buffer + sizeof(enum msgType));
    size_t lenKey = strlen(key) + 1;
    char *value = key + lenKey;
    size_t lenValue = strlen(value) + 1;
    size_t buffSize = lenKey + lenValue;

    void *buf = malloc(buffSize);

    strcpy((char *) buf, key);
    strcpy((char *) buf + lenKey, value);

    hashmap_put(serverMap, buf, buf + lenKey);
    return 0;
}

/**
 * Server function
 * @param ctx
 * @param buffer
 * @return
 */
int handleSetRdvReq(struct pingpong_context *ctx, char * buffer) {
    void * mr_addr;
    uint32_t mr_rkey;
    size_t  valueLen;
    char *key = (buffer + sizeof(enum msgType) + sizeof(void *) + sizeof(uint32_t) + sizeof(size_t));


    memcpy(&mr_addr, buffer + sizeof(enum msgType), sizeof(void *));
    memcpy(&mr_rkey, buffer + sizeof(enum msgType) + sizeof(void *), sizeof(uint32_t));
    memcpy(&valueLen, buffer + sizeof(enum msgType) + sizeof(void *) + sizeof(uint32_t), sizeof(size_t));

    char * value = malloc(valueLen);
    hashmap_put(serverMap, key, value); //1. Hashmap set to key with empty string,
    struct  ibv_mr * valueMr = ibv_reg_mr(ctx->pd, value, valueLen, IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ); //will write there the value after reading from client

    if (pp_post_send(ctx, value, valueLen, 128, IBV_WR_RDMA_READ, valueMr, mr_addr, mr_rkey)){
        fprintf(stderr, "RDV set Server: Error in sending mr address of server to client");
    }
    waitRecvCompletion128(ctx); //TODO: maybe send valueMR ??????

    strcpy(ctx->buf + (128 * EAGER_MAX_SIZE), "FIN");
    //send FIN to server
    if (pp_post_send(ctx, ctx->buf + (128 * EAGER_MAX_SIZE), EAGER_MAX_SIZE, 128, -1, NULL, NULL, 0)){
        fprintf(stderr, "RDV set Server: Error in sending FIN to client");
    }
    waitFINCompletion(ctx);
    fflush(stdout);
    hashmap_put(valuePointerToMr, value, valueMr);
    return 0;
}

/**
 * Handles all server operations according to the msg type
 * @param ctx
 * @param infoBuf
 * @return
 */
int HandleMsg(struct pingpong_context *ctx, char * infoBuf) {

    int type;
    memcpy(&type, infoBuf, sizeof(int));
    switch (type) {
        case EAGER_GET_REQUEST:
            handleGetReq(ctx, infoBuf);
            break;
        case EAGER_SET_REQUEST:
            handleSetEagerReq(infoBuf);
            break;
        case RENDEZVOUS_SET_REQUEST:
            handleSetRdvReq(ctx, infoBuf);
            break;
        default:
            fprintf(stderr, "packet type = %d\n", type);
            break;
    }
    return 0;
}


/**
 * Main server function
 * @param ctx
 * @return
 */
int serverLogic(struct pingpong_context *ctx) {
    struct ibv_wc wc;
    void * wantedBuff;
    for (int i = 0; i < 100; i++) { //do 100 post receive
        wantedBuff = (ctx->buf + (EAGER_MAX_SIZE * i));
        pp_post_recv(ctx, wantedBuff, EAGER_MAX_SIZE, i);
        bufferMap[i] = 1;
    }

    while (1) {
        int id = waitPoolCompletion(ctx, &wc);
        if (wc.status != IBV_WC_SUCCESS) {
            fprintf(stderr, "Failed status %s (%d) for wr_id %d\n",
                    ibv_wc_status_str(wc.status),
                    wc.status, (int) wc.wr_id);
            return 1;
        }
        if ((int) wc.wr_id < 128) {
            HandleMsg(ctx, ctx->buf + (EAGER_MAX_SIZE * wc.wr_id));
            int nextFreeBuff = getNextfreeBufNum(ctx);
            wantedBuff = (ctx->buf + (EAGER_MAX_SIZE * nextFreeBuff));
            pp_post_recv(ctx, wantedBuff, EAGER_MAX_SIZE, nextFreeBuff);
        }
    }
}

/**
 *
 * @param kv_handle
 * @param key
 * @param value
 * @return
 */
int kv_get(void *kv_handle, const char *key, char **value) {
    struct pingpong_context *ctx = (struct pingpong_context *) kv_handle;
    int buffToUse = getNextfreeBufNum(kv_handle);

    char *currBuf = ctx->buf + (EAGER_MAX_SIZE * buffToUse);
    size_t keyLen = strlen(key) + 1;

    unsigned msgSize = sizeof(enum msgType) + keyLen;

    if (msgSize <= EAGER_MAX_SIZE) { //eager
        int est = EAGER_GET_REQUEST;
        memcpy(currBuf, &est, sizeof(enum msgType));
        strcpy(currBuf + sizeof(enum msgType), key);
        pp_post_send(ctx, currBuf, msgSize, buffToUse, -1, NULL, NULL, 0);
        pp_post_recv(ctx, ctx->buf + (128 * EAGER_MAX_SIZE), EAGER_MAX_SIZE,128); //waiting for completion in big buffer[128]
        int id = waitRecvCompletion128(ctx); //id where the server response is

        char *serverResponse = ctx->buf + (EAGER_MAX_SIZE * id);
        int type;
        memcpy(&type, serverResponse, sizeof(int));
        char *valFromServerAddr = serverResponse + sizeof(enum msgType);
        void * mr_addr;
        uint32_t mr_rkey;
        size_t  valueLen;

        switch (type) {
            case EAGER_GET_RESPONSE:
                *value = malloc(strlen(valFromServerAddr));
                strcpy(*value, valFromServerAddr);
                break;

            case RENDEZVOUS_GET_RESPONSE:

                memcpy(&mr_addr, serverResponse + sizeof(enum msgType), sizeof(void *));
                memcpy(&mr_rkey, serverResponse + sizeof(enum msgType) + sizeof(void *), sizeof(uint32_t));
                memcpy(&valueLen, serverResponse + sizeof(enum msgType) + sizeof(void *) + sizeof(uint32_t), sizeof(size_t));
                *value = malloc(valueLen);
                struct  ibv_mr * valueMr = ibv_reg_mr(ctx->pd, *value, valueLen, IBV_ACCESS_LOCAL_WRITE); //will write there the value after reading from client
                if (pp_post_send(ctx, *value, valueLen, 128, IBV_WR_RDMA_READ, valueMr, mr_addr, mr_rkey)){
                    fprintf(stderr, "RDV get Client: Error in reading mr of server");
                }
                waitRecvCompletion128(ctx);
                fflush(stdout);

                strcpy(ctx->buf + (128 * EAGER_MAX_SIZE), "FIN");
                //send FIN to server
                if (pp_post_send(ctx, ctx->buf + (128 * EAGER_MAX_SIZE), EAGER_MAX_SIZE, 128, -1, NULL, NULL, 0)){
                    fprintf(stderr, "RDV set Server: Error in sending FIN to client");
                }
                waitRecvCompletion128(ctx);
                break;
            default:
                fprintf(stderr, "packet type = %d\n", type);
                break;
        }

        return 0;
    } else {
        fprintf(stderr, "Invalid key size\n");
    }
}

/**
 * client function
 * @param kv_handle
 * @param key
 * @param value
 * @return
 */
int kv_set(void *kv_handle, const char *key, const char *value){
    struct pingpong_context *ctx = (struct pingpong_context *) kv_handle;
    int buffToUse = getNextfreeBufNum(kv_handle);

    char * currBuf = ctx->buf + (EAGER_MAX_SIZE * buffToUse);

    size_t keyLen = strlen(key) + 1;
    size_t valueLen = strlen(value) + 1;
    unsigned msgSize = sizeof(enum msgType) + keyLen + valueLen ;

    if (msgSize <= EAGER_MAX_SIZE) { //eager
        int est = EAGER_SET_REQUEST;
        memcpy(currBuf, &est, sizeof(enum msgType));
        strcpy(currBuf + sizeof(enum msgType), key);
        strcpy(currBuf + sizeof(enum msgType) + keyLen, value);
        pp_post_send(ctx, currBuf, msgSize, buffToUse, -1, NULL, NULL, 0);
        bufferMap[buffToUse] = 1; //make occupied
        return 0;
    }

    else { //RENDEZ VOUZ - key + value size > EAGER MAX SIZE
        int type = RENDEZVOUS_SET_REQUEST;
        memcpy(currBuf, &type, sizeof(enum msgType));
        struct ibv_mr *mr = ibv_reg_mr(ctx->pd, value, valueLen, IBV_ACCESS_REMOTE_READ);
        if (!mr) {
            fprintf(stderr, "Couldn't register MR\n");
            return 1;
        }

        memcpy(currBuf + sizeof(enum msgType), &(mr->addr), sizeof(mr->addr));
        memcpy(currBuf + sizeof(enum msgType) + sizeof(mr->addr), &(mr->rkey), sizeof(mr->rkey));
        memcpy(currBuf + sizeof(enum msgType) + sizeof(mr->addr) + sizeof((mr->rkey)), &valueLen, sizeof(size_t));
        strcpy(currBuf + sizeof(enum msgType) + sizeof(mr->addr) + sizeof((mr->rkey)) + sizeof(size_t), key); // curbuff = "type, mr_addr, mr_rkey, valueLen, key"
        msgSize = sizeof(enum msgType) + sizeof(mr->addr) + sizeof(mr->rkey) + sizeof(size_t) + keyLen;
        pp_post_send(ctx, currBuf, msgSize, buffToUse, -1, NULL, NULL, 0); // client sent to server the mr to read from
        struct ibv_wc wc;
        waitPoolCompletion(ctx, &wc);
        // wait for Fin
        pp_post_recv(ctx, ctx->buf + (128 * EAGER_MAX_SIZE), EAGER_MAX_SIZE, 128);
        waitRecvCompletion128(ctx);
        return 0;
    }
}

/**
 * release the value that we did malloc to it
 * @param value
 */
void kv_release(char *value){
    free(value);
}

/**
 * Throughput set test
 * @param kv_handle
 * @return
 */
int throughputSetTest(void * kv_handle){
    char key[100];
    size_t size = 1;
    char * value = malloc((unsigned long) pow(2,20));
    printf("############################# SET - SHORT MSGS #############################\n");

    for (int i = 0; i < MAX_ITERS; i++){
        memset(value, '1', size);
        memset(value + size, '\0', 1);
        memset(key, i + 48, 1);
        memset(key + 1, '\0', 1);
        // open timer
        struct timeval start, end;
        if (gettimeofday(&start, NULL)) {
            perror("gettimeofday");
            return -1;
        }
        for (unsigned long j = 0; j < ITERS; j ++){
            kv_set(kv_handle, key, value);
        }

        // close timer
        if (gettimeofday(&end, NULL)) {
            perror("gettimeofday");
            return -1;
        }
        __suseconds_t usec = (((end.tv_sec - start.tv_sec) * (__suseconds_t) MICRO_SEC +
                               (end.tv_usec - start.tv_usec)));

        unsigned long total_bit = size * ITERS * BYTE_TO_BIT;
        unsigned long throughput = total_bit / usec;
        if (usec < 0) {
            printf("Error in client send message func");
        }
        printf("%lu\t%lu\tMbps\n", size, throughput);
        fflush(stdout);
        size = size * 2;
    }
    return 0;
}

/**
 * Throughput get test
 * @param kv_handle
 * @return
 */
int throughputGetTest(void * kv_handle){
    char key[100];
    size_t size = 1;
    char * value;
    printf("############################# GET #############################\n");

    for (int i = 0; i < MAX_ITERS; i++){
        memset(key, i + 48, 1);
        memset(key + 1, '\0', 1);
        // open timer
        struct timeval start, end;
        if (gettimeofday(&start, NULL)) {
            perror("gettimeofday");
            return -1;
        }
        for (unsigned long j = 0; j < ITERS; j ++){
            kv_get(kv_handle, key, &value);
        }

        // close timer
        if (gettimeofday(&end, NULL)) {
            perror("gettimeofday");
            return -1;
        }
        __suseconds_t usec = (((end.tv_sec - start.tv_sec) * (__suseconds_t) MICRO_SEC +
                               (end.tv_usec - start.tv_usec)));

        unsigned long total_bit = size * ITERS * BYTE_TO_BIT;
        unsigned long throughput = total_bit / usec;
        if (usec < 0) {
            printf("Error in client send message func");
        }
        printf("%lu\t%lu\tMbps\n", size, throughput);
        fflush(stdout);
        size = size * 2;
    }
    return 0;
}



int main(int argc, char *argv[]) {

    struct pingpong_context * kv_handle;
    //get servername
    char * servername = NULL;
    char bigVal [10001];
    memset(bigVal, '2', 10000);
    memset(bigVal + 10000, '\0', 1);

    char * valResponse;
    if (optind == argc - 1)
        servername = strdup(argv[optind]);
    else if (optind < argc) {
        usage(argv[0]);
        return 1;
    }

    if  (kv_open(servername, (void **) &kv_handle) != 0){
        printf("error in kv_open");
        return 1;
    }

    if (!servername) {
        serverMap = hashmap_new();
        valuePointerToMr = hashmap_new();
        serverLogic(kv_handle);
    }
    else {
        throughputSetTest(kv_handle);
        throughputGetTest(kv_handle);
        printf("############################# END #############################\n");
    }
    return 0;
}