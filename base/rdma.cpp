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
#include "base/rdma.hpp"

RDMA_Device::RDMA_Device(int num_nodes, int num_threads, int nid, char *mem, uint64_t mem_sz, vector<Node> & nodes, Node & remote) : num_threads_(num_threads) {
    // record IPs of ndoes
    vector<string> ipset;
    for (const auto & node : nodes)
        ipset.push_back(node.ibname);

    // initialization of new librdma
    // int node_id, int tcp_base_port
    // start listen QP & MR request on local tcp port
    global_rdma_ctrl = std::make_shared<RdmaCtrl>(nid, nodes[nid].tcp_port);
    std::cout << "RDMA Ctrl create success!" << std::endl;
            
    RdmaCtrl::DevIdx idx;
    idx.dev_id = 1;
    idx.port_id = 1;
            
    opened_rnic = global_rdma_ctrl->open_device(idx);
    std::cout << "RDMA open device!" << std::endl;

    // only one remote node now, which is at the end of nodes vector
    remote_nodes.push_back(remote);
    GetMRMeta(remote);

    // Alloc rdma memory region
    AllocMR(mem, mem_sz);

    qp_man_ = new QPManager(nid); //LCY: this param need to be global tid?
    BuildQPConnection(qp_man_); 
}

void RDMA_Device::GetMRMeta(const Node& node) {
    // LCY： what mr_id used for?
    while (QP::get_remote_mr(node.hostname, node.tcp_port, REMOTE_MR_ID, &remote_mr_) != SUCC) {
        usleep(2000);
    }
    std::cout << "Get remote data!" << std::endl;
}

void RDMA_Device::AllocMR(char *mem, uint64_t mem_sz) {
    RDMA_ASSERT(global_rdma_ctrl->register_memory(LOCAL_MR_ID, mem, mem_sz, opened_rnic));
    std::cout << "Alloc memory region!" << std::endl;
}

const MemoryAttr& RDMA_Device::GetRemoteMr(int node_id) {
    return remote_mr_;
}

void RDMA_Device::BuildQPConnection(QPManager * qp_manager) {
    MemoryAttr remote_mr = GetRemoteMr();
    MemoryAttr local_mr = global_rdma_ctrl->get_local_mr(LOCAL_MR_ID);
    
    RCQP * qp = global_rdma_ctrl->create_rc_qp(create_rc_idx(SINGLE_REMOTE_ID, qp_manager->global_tid),
                                                    opened_rnic, &local_mr);
                                                    
    ConnStatus rc;
    do {
        rc = qp->connect(remote_nodes[0].hostname, remote_nodes[0].tcp_port); // check paramenters
        if (rc == SUCC) {
            qp->bind_remote_mr(remote_mr); 
            qp_manager->data_qps[0] = qp; // so ugly, change it later
        }
        usleep(2000);
    } while (rc != SUCC);
}

/* This is a sync read completion
*  param:
*  dst_tid: remote node thread id
*  dst_nid: remote node node id
*  local: address of the buffer to read from / write to
*  size: length of the buffer in bytes  
*  off: start offset of remote memory block to access
*/
int RDMA_Device::RdmaRead(int dst_tid, int dst_nid, char *local, uint64_t size, uint64_t off) {
    RCQP * qp = qp_man_->GetRemoteDataQPWithNodeID(dst_nid); // TODO(big): this should be a combined id which contain both dstnid + dsttid
        
    auto rc = qp->post_send(IBV_WR_RDMA_READ, local, size, off, IBV_SEND_SIGNALED);
    if(rc != SUCC) {
        RDMA_LOG(ERROR) << "client: post read failed. rc=" << rc ;
        return -1;
    }
    ibv_wc wc;
    rc = qp->poll_till_completion(wc, no_timeout);
    if(rc != SUCC) {
        RDMA_LOG(ERROR) << "client: poll read failed. rc=" << rc;
        return -1;
    }
    return 0;
}


/* This is a sync batch read completion
*  param:
*  dst_tid: remote node thread id
*  dst_nid: remote node node id
*  local: address of the buffer to read from / write to
*  size: length of the buffer in bytes  
*  off: start offset of remote memory block to access
*/
int RDMA_Device::RdmaReadBatch(int dst_tid, int dst_nid, char*local, uint64_t size, vector<uint64_t>& off, uint64_t begin, uint64_t num) {
    RCQP* qp = qp_man_->GetRemoteDataQPWithNodeID(dst_nid);

    struct ibv_send_wr sr[num];
    
    struct ibv_sge sge[num];

    struct ibv_send_wr* bad_sr;

    for(int i = 0; i < num; ++i) {
        // setting the SGE
        sge[i].addr = (uint64_t)local + size*i; 
        sge[i].length = size; // all vertex size are same
        sge[i].lkey = qp->local_mr_.key;

        // setting sr, sr has to be initialized in this style
        sr[i].wr_id = 0;
        sr[i].opcode = IBV_WR_RDMA_READ;
        sr[i].num_sge = 1;
        sr[i].next = (i == num -1) ? NULL : &(sr[i+1]) ;
        
        sr[i].sg_list = &sge[i];
        sr[i].send_flags = 0;
        sr[i].imm_data = 0;

        sr[i].wr.rdma.remote_addr = qp->remote_mr_.buf + off[begin + i];
        sr[i].wr.rdma.rkey = qp->remote_mr_.key;
    }
    // sr[num-1].wr_id = 0;
    sr[num-1].send_flags = IBV_SEND_SIGNALED;
    auto rc = qp->post_batch(&(sr[0]), &bad_sr);
    if(rc != SUCC) {
        RDMA_LOG(ERROR) << "client: post batch failed. rc = " << rc;
        return -1;
    }
    
    ibv_wc wc;
    rc = qp->poll_till_completion(wc, no_timeout);
    if(rc != SUCC) {
        RDMA_LOG(ERROR) << "client: poll read failed. rc=" << rc;
        return -1;
    }
    return 0;
}

int RDMA_Device::RdmaWrite(int dst_tid, int dst_nid, char *local, uint64_t size, uint64_t off) {
    RCQP * qp = qp_man_->GetRemoteDataQPWithNodeID(dst_nid);

    int flags = IBV_SEND_SIGNALED;

    auto rc = qp->post_send(IBV_WR_RDMA_WRITE, local, size, off, flags);
    if (rc != SUCC) {
        RDMA_LOG(ERROR) << "client: post write fail.  rc = " << rc;
        return -1;
    }
    // TODO(big) batch polling ? see pendind_qps in ford
    ibv_wc wc{};
    int poll_rc = qp->poll_send_completion(wc);
    if(poll_rc != SUCC) {
        RDMA_LOG(ERROR) << "client: poll write fail. rc=" << rc;
        return -1;
    }
    return 0;
}

void RDMA_init(int num_nodes,  int num_threads, int nid, char *mem, uint64_t mem_sz, vector<Node> & nodes, Node & remote) {
    uint64_t t = timer::get_usec();

    // init RDMA device
    RDMA &rdma = RDMA::get_rdma();
    rdma.init_dev(num_nodes, num_threads, nid, mem, mem_sz, nodes, remote);

    t = timer::get_usec() - t;
    std::cout << "INFO: initializing RDMA done (" << t / 1000  << " ms)" << std::endl;
}
