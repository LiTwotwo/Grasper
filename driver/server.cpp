/* Copyright 2019 Husky Data Lab, CUHK

Authors: Hongzhi Chen (hzchen@cse.cuhk.edu.hk)
*/

#include "base/node.hpp"
#include "base/node_util.hpp"
#include "utils/global.hpp"
#include "utils/config.hpp"
#include "driver/master.hpp"
#include "driver/worker.hpp"

#include "glog/logging.h"

int main(int argc, char* argv[]) {
    google::InitGoogleLogging(argv[0]);

    Node my_node;
    InitMPIComm(&argc, &argv, my_node);
    std::cout << "Init success" << std::endl;

    string cfg_fname = argv[1];
    CHECK(!cfg_fname.empty());

    vector<Node> nodes = ParseFile(cfg_fname);
    CHECK(CheckUniquePort(nodes));
    Node & node = GetNodeById(nodes, my_node.get_world_rank());

    my_node.ibname = node.ibname;
    my_node.tcp_port = node.tcp_port;
    my_node.rdma_port = node.rdma_port;
    my_node.hostname = node.hostname;
    std::cout << my_node.DebugString();
    nodes.erase(nodes.begin());  // delete the master info in nodes (array) for rdma init

    // LCY: currently only one remote node and config is at the end of ib.cfg
    vector<Node> memory_nodes = {nodes.back()}; //
    nodes.pop_back();

    // set my_node as the shared static Node instance
    my_node.InitialLocalWtime();

    // set this as
    Node::StaticInstance(&my_node);

    Config* config = Config::GetInstance();
    config->Init();

    std::cout  << "DONE -> Local Config->Init()" << endl;

    if (my_node.get_world_rank() == MASTER_RANK) {
        Master master(my_node);
        master.Init();

        master.Start();
    } else {
        Worker worker(my_node, nodes, memory_nodes);
        worker.Init();
        worker.Start();

        worker_barrier(my_node);
        worker_finalize(my_node);
    }

    node_barrier();
    node_finalize();

    return 0;
}
