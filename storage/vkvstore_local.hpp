/* Copyright 2019 Husky Data Lab, CUHK

Authors: Changji Li (cjli@cse.cuhk.edu.hk)
         Hongzhi Chen (hzchen@cse.cuhk.edu.hk)
*/

#ifndef VKVSTORE_LOCAL_HPP_
#define VKVSTORE_LOCAL_HPP_

#include <stdint.h>
#include <string.h>
#include <vector>
#include <iostream>
#include <pthread.h>

#include "base/type.hpp"
#include "base/rdma.hpp"
#include "base/serialization.hpp"
#include "base/node_util.hpp"
#include "core/buffer.hpp"
#include "storage/layout.hpp"
#include "third_party/zmq.hpp"
#include "utils/mymath.hpp"
#include "utils/unit.hpp"
#include "utils/config.hpp"
#include "utils/global.hpp"
#include "utils/tool.hpp"
#include "nexus.h"
#include "rpc.h"

/* VKVStore:
 * For Vertex-Properties
 * key (main-header and indirect-header region) | value (entry region)
 * The head region is a cluster chaining hash-table (with associativity),
 * while entry region is a varying-size array.
 * */

class VKVStore_Local {
 public:
   VKVStore_Local(Node & my_node, vector<Node> & remotes, Buffer * buf);

   void init(uint64_t off, uint64_t slots_num, uint64_t buckets_num);   

   // Get property by key remotely
   void get_property_remote(int tid, int dst_nid, uint64_t pid, value_t & val); 

   // Get label by key remotely
   void get_label_remote(int tid, int dst_nid, uint64_t pid, label_t & label);   

   // Get key remotely
   void get_key_remote(int tid, int dst_nid, uint64_t pid, ikey_t & key);

   void get_property_remote_erpc(int remote_id, uint64_t pid, value_t & val);

 private:
   Node & my_node_;
   vector<Node> & remotes_;
   Buffer * buf_;

   const int ASSOCIATIVITY = 8;  // the associativity of slots in each bucket

   // // size of VKVStore_Local and offset to rdma start point
   uint64_t offset;
   uint64_t num_slots;        // 1 bucket = ASSOCIATIVITY slots
   uint64_t num_buckets;      // main-header region (static)
};

#endif /* VKVSTORE_LOCAL_HPP_ */
