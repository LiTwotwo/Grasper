/* Copyright 2019 Husky Data Lab, CUHK

Authors: Changji Li (cjli@cse.cuhk.edu.hk)
         Hongzhi Chen (hzchen@cse.cuhk.edu.hk)
*/

#include <assert.h>
#include "storage/vkvstore_local.hpp"
#include "storage/mpi_snapshot.hpp"
#include "storage/snapshot_func.hpp"


using namespace std;

// Get key by key remotely
void VKVStore_Local::get_key_remote(int tid, int dst_nid, uint64_t pid, ikey_t & key) {
    uint64_t bucket_id = pid % num_buckets;

    while (true) {
        char * buffer = buf_->GetSendBuf(tid);
        uint64_t off = offset + bucket_id * ASSOCIATIVITY * sizeof(ikey_t);
        uint64_t sz = ASSOCIATIVITY * sizeof(ikey_t);

        RDMA &rdma = RDMA::get_rdma();
        // timer::start_timer(tid);
        // RDMA_LOG(INFO) << "In vp get key remote";
        rdma.dev->RdmaRead(tid, dst_nid, buffer, sz, off);
        // timer::stop_timer(tid);

        ikey_t * keys = (ikey_t *)buffer;
        for (int i = 0; i < ASSOCIATIVITY; ++i) {
            if (i < ASSOCIATIVITY - 1) {
                if (keys[i].pid == pid) {
                    key = keys[i];
                    return;
                }
            } else {
                if (keys[i].is_empty())
                    return;  // not found

                bucket_id = keys[i].pid;  // move to next bucket
                break;  // break for-loop
            }
        }
    }
}

VKVStore_Local::VKVStore_Local(Node & my_node, vector<Node> & remotes, Buffer * buf) : my_node_(my_node), remotes_(remotes), buf_(buf) {}
 
void VKVStore_Local::init(uint64_t off, uint64_t slots_num, uint64_t buckets_num) {
    offset = off;
    num_slots = slots_num;
    num_buckets = buckets_num;
}

// Get properties by key remotely
void VKVStore_Local::get_property_remote(int tid, int dst_nid, uint64_t pid, value_t & val) {
    ikey_t key;
    // no need to hash pid
    get_key_remote(tid, dst_nid, pid, key); // first RDMA read
        if (key.is_empty()) {
            val.content.resize(0);
            return;
        }

        char * buffer = buf_->GetSendBuf(tid);
        uint64_t r_off = offset + num_slots * sizeof(ikey_t) + key.ptr.off;
        uint64_t r_sz = key.ptr.size;

        RDMA &rdma = RDMA::get_rdma();
        // timer::start_timer(tid);
        // RDMA_LOG(INFO) << "In vp get property remote";
        rdma.dev->RdmaRead(tid, dst_nid, buffer, r_sz, r_off);
        // timer::stop_timer(tid);

        // type : char to uint8_t
        val.type = buffer[0];
        val.content.resize(r_sz-1);

        char * ctt = &(buffer[1]);
        std::copy(ctt, ctt + r_sz-1, val.content.begin());
}

void VKVStore_Local::get_label_remote(int tid, int dst_nid, uint64_t pid, label_t & label) {
    value_t val;
    get_property_remote(tid, dst_nid, pid, val);
    label = (label_t)Tool::value_t2int(val);
}

void sm_handler(int, erpc::SmEventType, erpc::SmErrType, void *) {} // ????????????

void cont_func(void*, void*) {}

void VKVStore_Local::get_property_remote_erpc(int remote_id, uint64_t pid, value_t & val) {
    string client_uri = my_node_.hostname + ":" + to_string(my_node_.rdma_port); // rdma port will be used as udp port for eRPC, change the name later
    erpc::Nexus nexus(client_uri);

    erpc::Rpc<erpc::CTransport> *rpc = new erpc::Rpc<erpc::CTransport>(&nexus, nullptr, 0, sm_handler);
    
    string server_uri = remotes_[remote_id].hostname + ":" + to_string(remotes_[remote_id].rdma_port);
    int session_num = rpc->create_session(server_uri, 0); // second is remote rpc id, this create function shouldn't be called on data path

    while(!rpc->is_connected(session_num)) rpc->run_event_loop_once();

    erpc::MsgBuffer req = rpc->alloc_msg_buffer_or_die(16);
    erpc::MsgBuffer resp = rpc->alloc_msg_buffer_or_die(16);

    rpc->enqueue_request(session_num, 0 , &req, &resp, cont_func, nullptr); //REQ_TYPE::GET_PROPERTY
    rpc->run_event_loop(100);

    delete rpc;
}