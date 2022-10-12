/*
 * @Author: chunyuLi 
 * @Date: 2022-09-13 10:10:10 
 * @Last Modified by: chunyuLi
 * @Last Modified time: 2022-09-13 17:45:38
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
#include "core/buffer.hpp"
#include "storage/vkvstore_local.hpp"
#include "storage/ekvstore_local.hpp"
#include "storage/vertex.hpp"
#include "storage/edge.hpp"
#include "utils/hdfs_core.hpp"
#include "utils/config.hpp"
#include "utils/unit.hpp"
#include "utils/tool.hpp"
#include "utils/global.hpp"

using __gnu_cxx::hash_map;
using __gnu_cxx::hash_set;

class MetaData {
 public:
    MetaData(Node & node, AbstractIdMapper * id_mapper, Buffer * buf);

    ~MetaData();

    void GetRemoteMeta(vector<Node> & remotes);

    void Init(vector<Node> & nodes, GraphMeta& graphmeta);

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

    void GetVertex(int tid, vid_t v_id, Vertex& v);
    int GetVPList(int tid, Vertex& v, vector<label_t>& vpl);
    int GetInNbs(int tid, Vertex& v, vector<vid_t>& in_nbs);
    int GetOutNbs(int tid, Vertex& v, vector<vid_t>& out_nbs);

    void GetEdge(int tid, eid_t e_id, Edge & e);
    int GetEPList(int tid, Edge& e, vector<label_t>& epl);

    void GetAllVertices(int tid, vector<vid_t> & vid_list);
    void GetAllEdges(int tid, vector<eid_t> & eid_list);

    bool VPKeyIsLocal(vpid_t vp_id);
    bool EPKeyIsLocal(epid_t ep_id);

    bool GetPropertyForVertex(int tid, vpid_t vp_id, value_t & val);
    bool GetPropertyForEdge(int tid, epid_t ep_id, value_t & val);

    bool GetLabelForVertex(int tid, vid_t vid, label_t & label);
    bool GetLabelForEdge(int tid, eid_t eid, label_t & label);

    // LCY: currently not used, but will be used again if more than one remote servers are used
    int GetMachineIdForVertex(vid_t v_id);
    int GetMachineIdForEdge(eid_t e_id);

    void GetNameFromIndex(Index_T type, label_t label, string & str);

    void InsertAggData(agg_t key, vector<value_t> & data);
    void GetAggData(agg_t key, vector<value_t> & data);
    void DeleteAggData(agg_t key);

    void get_string_indexes();

    // // local access val for TCP request
    // void AccessVProperty(uint64_t vp_id_v, value_t & val);
    // void AccessEProperty(uint64_t ep_id_v, value_t & val);

    // single ptr instance
    // diff from Node
    static MetaData* StaticInstanceP(MetaData* p = NULL) {
        static MetaData* static_instance_p_ = NULL;
        if (p) {
            // if(static_instance_p_)
            //     delete static_instance_p_;
            // static_instance_p_ = new MetaData;
            static_instance_p_ = p;
        }

        assert(static_instance_p_ != NULL);
        return static_instance_p_;
    }
 
#ifdef TEST_WITH_COUNT
// test with counter functions
    void InitCounter();
    void RecordVtx(int size);
    void RecordEdg(int size);
    void RecordVp(int size);
    void RecordEp(int size);
    void RecordVin(int size); 
    void RecordVout(int size); 
    void RecordVtxExt(int size);
    void PrintCounter();
#endif

    // load the index and data from HDFS
    string_index indexes;  // index is global, no need to shuffle

 private:
    AbstractIdMapper* id_mapper_;
    Buffer* buffer_;
    Config* config_;
    Node & node_;

    //==================== Meta Storage =======================
    // Vertex
    uint64_t v_array_off_;
    uint64_t v_ext_off_;
    uint64_t v_num_;

    //edge
    uint64_t e_array_off_;
    uint64_t e_ext_off_;
    uint64_t e_num_;

    unordered_map<agg_t, vector<value_t>> agg_data_table;
    mutex agg_mutex;

    VKVStore_Local * vpstore_;
    EKVStore_Local * epstore_;
    

    // test counter
#ifdef TEST_WITH_COUNT
    int vtx_counter_;
    vector<int> vtx_sizes_;
    int vtx_ext_counter_;
    vector<int> vtx_ext_sizes_;
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

};
