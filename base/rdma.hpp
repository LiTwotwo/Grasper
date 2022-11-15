/*
 * Copyright (c) 2016 Shanghai Jiao Tong University.
 *     All rights reserved.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an "AS
 *  IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *  express or implied.  See the License for the specific language
 *  governing permissions and limitations under the License.
 *
 * For more about this software visit:
 *
 *      http://ipads.se.sjtu.edu.cn/projects/wukong
 *
 */

/* Copyright 2019 Husky Data Lab, CUHK

Authors: Hongzhi Chen (hzchen@cse.cuhk.edu.hk)
*/

#pragma once

// #pragma GCC diagnostic warning "-fpermissive"

#include <vector>
#include <string>
#include <iostream>     // std::cout
#include <fstream>      // std::ifstream
#include "base/node.hpp"
#include "base/qp_manager.hpp"

using namespace std;

#include "utils/timer.hpp"
#include "rlib/rdma_ctrl.hpp"

using namespace rdmaio;

#define REMOTE_MR_ID 97
#define LOCAL_MR_ID 100
#define REMOTE_NID 0
#define SINGLE_REMOTE_ID 0
#define RC_MAX_SEND_SIZE 1024

class RDMA_Device {
    // use new third party rdma lib
    // 1. update init function, in current impl, QPs are created and connected between different workers
    //    workers pass logic plan by msgs (not change now)
    //    but worker -> remote connection is not created
    // 2. update read/write API
    // 3. update API usage in **_expert
public:
    RdmaCtrlPtr global_rdma_ctrl = nullptr;

    RNicHandler * opened_rnic;

    std::vector<Node> remote_nodes;

    RDMA_Device(int num_nodes, int num_threads, int nid, char *mem, uint64_t mem_sz, vector<Node> & nodes, Node & remote);

    void GetMRMeta(const Node& node);

    void AllocMR(char *mem, uint64_t mem_sz);
    
    void BuildQPConnection(QPManager * qp_manager);

    // inline?
    const MemoryAttr& GetRemoteMr(int node_id = 0);

    // 0 on success, -1 otherwise
    int RdmaRead(int dst_tid, int dst_nid, char *local, uint64_t size, uint64_t off);

    int RdmaWrite(int dst_tid, int dst_nid, char *local, uint64_t size, uint64_t off);

        // int RdmaWriteSelective(int dst_tid, int dst_nid, char *local, uint64_t size, uint64_t off) {
        //     Qp* qp = ctrl->get_rc_qp(dst_tid, dst_nid);
        //     int flags = (qp->first_send() ? IBV_SEND_SIGNALED : 0);
        //     // int flags = IBV_SEND_SIGNALED;

        //     qp->rc_post_send(IBV_WR_RDMA_WRITE, local, size, off, flags);

        //     if (qp->need_poll())
        //         qp->poll_completion();
        //     return 0;
        //     // return rdmaOp(dst_tid, dst_nid, local, size, off, IBV_WR_RDMA_WRITE);
        // }

        // int RdmaWriteNonSignal(int dst_tid, int dst_nid, char *local, uint64_t size, uint64_t off) {
        //     Qp* qp = ctrl->get_rc_qp(dst_tid, dst_nid);
        //     int flags = 0;
        //     qp->rc_post_send(IBV_WR_RDMA_WRITE, local, size, off, flags);
        //     return 0;
        // }

     private:   
        int num_threads_;

        // LCY: if more remote nodes, change this to a map<node_id, MemoryAttr>
        MemoryAttr remote_mr_;

        QPManager *qp_man_;        
    };

class RDMA {
 public:
    RDMA_Device *dev = NULL;

    RDMA() { }

    ~RDMA() { if (dev != NULL) delete dev; }

    void init_dev(int num_nodes, int num_threads, int nid, char *mem, uint64_t mem_sz, vector<Node> & nodes, Node & remote) {
        dev = new RDMA_Device(num_nodes, num_threads, nid, mem, mem_sz, nodes, remote);
    }

    inline static bool has_rdma() { return true; }

    static RDMA &get_rdma() {
        static RDMA rdma;
        return rdma;
    }
};

void RDMA_init(int num_nodes, int num_threads, int nid, char *mem, uint64_t mem_sz, vector<Node> & nodes, Node & remote);
