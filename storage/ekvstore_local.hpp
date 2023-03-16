/*
 * @Author: chunyuLi 
 * @Date: 2022-09-12 13:52:58 
 * @Last Modified by: chunyuLi
 * @Last Modified time: 2022-09-13 17:52:24
 */


#ifndef EKVSTORE_LOCAL_HPP_
#define EKVSTORE_LOCAL_HPP_

#include <stdint.h>
#include <string.h>
#include <vector>
#include <iostream>
#include <pthread.h>

#include "base/rdma.hpp"
#include "base/type.hpp"
#include "base/serialization.hpp"
#include "base/node_util.hpp"
#include "core/buffer.hpp"
#include "storage/layout.hpp"
#include "utils/mymath.hpp"
#include "third_party/zmq.hpp"
#include "utils/unit.hpp"
#include "utils/config.hpp"
#include "utils/global.hpp"
#include "utils/tool.hpp"

/* EKVStore:
 * For Edge-Properties
 * key (main-header and indirect-header region) | value (entry region)
 * The head region is a cluster chaining hash-table (with associativity),
 * while entry region is a varying-size array.
 * */

class EKVStore_Local {
 public:
    EKVStore_Local(Node & my_node, vector<Node> & remotes, Buffer * buf);

    void init(uint64_t off, uint64_t slots_num, uint64_t buckets_num);

    // Get properties by key remotely
    void get_property_remote(int tid, int dst_nid, uint64_t pid, value_t & val);

    // Get label by key remotely
    void get_label_remote(int tid, int dst_nid, uint64_t pid, label_t & label);

    // Get key remotely
    void get_key_remote(int tid, int dst_nid, uint64_t pid, ikey_t & key);

 private:
    Node & my_node_;
    vector<Node> & remotes_;
    Buffer * buf_;

   //  static const int NUM_LOCKS = 1024;

    static const int ASSOCIATIVITY = 8;  // the associativity of slots in each bucket
    uint64_t offset;

    uint64_t num_slots;        // 1 bucket = ASSOCIATIVITY slots
    uint64_t num_buckets;      // main-header region (static)
};

#endif /* EKVSTORE_LOCAL_HPP_ */
