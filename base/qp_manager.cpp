/*
 * @Author: chunyuLi 
 * @Date: 2022-09-08 16:24:29 
 * @Last Modified by: chunyuLi
 * @Last Modified time: 2022-09-08 17:24:17
 */

#include "base/qp_manager.hpp"

using namespace rdmaio;

// void BuildQPConnection(RDMA_Device * dev) {
//     MemoryAttr remote_mr = dev->GetRemoteMr();
//     MemoryAttr local_mr = dev->global_rdma_ctrl->get_local_mr(LOCAL_MR_ID);
    
//     RCQP * qp = dev->global_rdma_ctrl->create_rc_qp(create_rc_idx(SINGLE_REMOTE_ID, global_tid),
//                                                     dev->opened_rnic, &local_mr);
                                                    
//     ConnStatus rc;
//     do {
//         rc = qp->connect(dev->remote_nodes[0].hostname, dev->remote_nodes[0].tcp_port); // check paramenters
//         if (rc == SUCC) {
//             qp->bind_remote_mr(remote_mr);
//             data_qps[0] = qp; // so ugly, change it later
//         }
//         usleep(2000);
//     } while (rc != SUCC);
// }
