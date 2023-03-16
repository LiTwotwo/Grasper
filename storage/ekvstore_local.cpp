/*
 * @Author: chunyuLi 
 * @Date: 2022-09-12 14:00:46 
 * @Last Modified by: chunyuLi
 * @Last Modified time: 2022-09-13 17:58:23
 */


#include <assert.h>
#include "storage/ekvstore_local.hpp"
#include "storage/mpi_snapshot.hpp"
#include "storage/snapshot_func.hpp"

using namespace std;

// Get key by key remotely
void EKVStore_Local::get_key_remote(int tid, int dst_nid, uint64_t pid, ikey_t & key) {
    // assert(config_->global_use_rdma);

    uint64_t bucket_id = pid % num_buckets;

    while (true) {
        char * buffer = buf_->GetSendBuf(tid);
        uint64_t off = offset + bucket_id * ASSOCIATIVITY * sizeof(ikey_t);
        uint64_t sz = ASSOCIATIVITY * sizeof(ikey_t);

        RDMA &rdma = RDMA::get_rdma();
        // timer::start_timer(tid);
        // RDMA_LOG(INFO) << "In ekv get key remote";
        rdma.dev->RdmaRead(tid, dst_nid, buffer, sz, off);
        // timer::stop_timer(tid);
        ikey_t *keys = (ikey_t *)buffer;
        for (int i = 0; i < ASSOCIATIVITY; i++) {
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

EKVStore_Local::EKVStore_Local(Node & my_node, vector<Node> & remotes, Buffer * buf) :my_node_(my_node), remotes_(remotes), buf_(buf) {}

void EKVStore_Local::init(uint64_t off, uint64_t slots_num, uint64_t buckets_num) {
    offset = off;
    num_slots = slots_num;
    num_buckets = buckets_num;
}

// Get properties by key remotely
void EKVStore_Local::get_property_remote(int tid, int dst_nid, uint64_t pid, value_t & val) {
    ikey_t key;
    get_key_remote(tid, dst_nid, pid, key);
    if (key.is_empty()) {
        val.content.resize(0);
        return;
    }

    char * buffer = buf_->GetSendBuf(tid);
    uint64_t r_off = offset + num_slots * sizeof(ikey_t) + key.ptr.off;
    uint64_t r_sz = key.ptr.size;

    RDMA &rdma = RDMA::get_rdma();
    // timer::start_timer(tid);
    // RDMA_LOG(INFO) << "In ekv get property remote";
    rdma.dev->RdmaRead(tid, dst_nid, buffer, r_sz, r_off);
    // timer::stop_timer(tid);

    // type : char to uint8_t
    val.type = buffer[0];
    val.content.resize(r_sz-1);

    char * ctt = &(buffer[1]);
    std::copy(ctt, ctt + r_sz-1, val.content.begin());
}

void EKVStore_Local::get_label_remote(int tid, int dst_nid, uint64_t pid, label_t & label) {
    value_t val;
    get_property_remote(tid, dst_nid, pid, val);
    label = (label_t)Tool::value_t2int(val);
}
