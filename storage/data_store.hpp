/* Copyright 2019 Husky Data Lab, CUHK

Authors: Hongzhi Chen (hzchen@cse.cuhk.edu.hk)
*/

#pragma once
#define TEST_WITH_COUNT

#include <mutex>
#include <string>
#include <stdlib.h>
#include <ext/hash_map>
#include <ext/hash_set>
#include <hdfs.h>
#include "glog/logging.h"

#include "base/type.hpp"
#include "base/node_util.hpp"
#include "base/communication.hpp"
#include "core/id_mapper.hpp"
#include "core/remote_buffer.hpp"
#include "storage/vkvstore.hpp"
#include "storage/ekvstore.hpp"
#include "storage/vertex.hpp"
#include "storage/edge.hpp"
#include "utils/hdfs_core.hpp"
#include "utils/config.hpp"
#include "utils/unit.hpp"
#include "utils/tool.hpp"
#include "utils/global.hpp"

using __gnu_cxx::hash_map;
using __gnu_cxx::hash_set;

class DataStore {
 public:
    DataStore(Node & node, AbstractIdMapper * id_mapper, RemoteBuffer * buf);

    ~DataStore();

    void Init(vector<Node> & locals);

    GraphMeta GetGraphMeta();

    // index format
    // string \t index [int]
    /*
     *    unordered_map<string, label_t> str2el; //map to edge_label
     *    unordered_map<label_t, string> el2str;
     *    unordered_map<string, label_t> str2epk; //map to edge's property key
     *    unordered_map<label_t, string> epk2str;
     *    unordered_map<string, label_t> str2vl; //map to vtx_label
     *    unordered_map<label_t, string> vl2str;
     *    unordered_map<string, label_t> str2vpk; //map to vtx's property key
     *    unordered_map<label_t, string> vpk2str;
     */

    void LoadDataFromHDFS();
    void Shuffle();
    void DataConverter();

    // void ReadSnapshot();
    // void WriteSnapshot();

    // void InsertAggData(agg_t key, vector<value_t> & data);
    // void GetAggData(agg_t key, vector<value_t> & data);
    // void DeleteAggData(agg_t key);

    // single ptr instance
    // diff from Node
    static DataStore* StaticInstanceP(DataStore* p = NULL) {
        static DataStore* static_instance_p_ = NULL;
        if (p) {
            // if(static_instance_p_)
            //     delete static_instance_p_;
            // static_instance_p_ = new DataStore;
            static_instance_p_ = p;
        }

        assert(static_instance_p_ != NULL);
        return static_instance_p_;
    }
 
// #ifdef TEST_WITH_COUNT
// test with counter functions
    void InitCounter();
    void RecordVtx(int size);
    void RecordEdg(int size);
    void RecordVp(int size);
    void RecordEp(int size);
    void RecordVin(int size); 
    void RecordVout(int size); 
    void PrintCounter();
// #endif

    // load the index and data from HDFS
    string_index indexes;  // index is global, no need to shuffle

 private:
    RemoteBuffer * remote_buffer_;
    AbstractIdMapper* id_mapper_;
    Config* config_;
    Node & node_;

    //==================== Data Storage =======================
    VertexTable * v_table_;
    EdgeTable* e_table_;

    unordered_map<agg_t, vector<value_t>> agg_data_table;
    mutex agg_mutex;

    VKVStore * vpstore_;
    EKVStore * epstore_;

    GraphMeta graph_meta_;    

    // test counter
#ifdef TEST_WITH_COUNT
    int vtx_counter_;
    vector<int> vtx_sizes_;
    int edg_counter_;
    vector<int> edg_sizes_;
    int vp_counter_;
    vector<int> vp_sizes_;
    int ep_counter_;
    vector<int> ep_sizes_;
    int vin_nbs_counter_;
    vector<int> vin_nbs_sizes_;
    int vout_nbs_counter_;
    vector<int> vout_nbs_sizes_;
#endif

    // =========tmp usage=========
    // will not be used after data loading
    vector<Vertex*> vertices;
    vector<Edge*> edges;
    vector<VProperty*> vplist;
    vector<EProperty*> eplist;
    vector<vp_list*> vp_buf;

    // ==========tmp usage=========
    void get_string_indexes();
    void get_vertices();
    void load_vertices(const char* inpath);
    Vertex* to_vertex(char* line);

    void get_vplist();
    void load_vplist(const char* inpath);
    void to_vp(char* line, vector<VProperty*> & vplist, vector<vp_list*> & vp_buf);

    void get_eplist();
    void load_eplist(const char* inpath);
    void to_ep(char* line, vector<EProperty*> & eplist);
};
