/*
 * @Author: chunyuLi 
 * @Date: 2022-09-08 16:02:44 
 * @Last Modified by: chunyuLi
 * @Last Modified time: 2022-09-08 16:23:50
 */

#pragma once

#include "rlib/rdma_ctrl.hpp"

#define REMOTE_SERVER_NUM 1

using namespace rdmaio;

// This QPManager builds qp connections (compute node <-> memory node) for each txn thread in each compute node
class QPManager {
 public:
  QPManager(int global_tid) : global_tid(global_tid) {}

  RCQP* GetRemoteDataQPWithNodeID(const int node_id) const {
    return data_qps[node_id];
  }

  void GetRemoteQPsWithNodeIDs(const std::vector<int>* node_ids, std::vector<RCQP*>& qps) {
    for (int node_id : *node_ids) {
      RCQP* qp = data_qps[node_id];
      if (qp) {
        qps.push_back(qp);
      }
    }
  }

  RCQP * data_qps[REMOTE_SERVER_NUM]{nullptr};
  
  int global_tid;
};

