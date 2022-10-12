/*
 * @Author: chunyuLi 
 * @Date: 2022-09-12 14:26:18 
 * @Last Modified by: chunyuLi
 * @Last Modified time: 2022-09-13 17:54:49
 */


#include "storage/metadata.hpp"
#include "storage/mpi_snapshot.hpp"
#include "storage/snapshot_func.hpp"

MetaData::MetaData(Node & node, AbstractIdMapper * id_mapper, Buffer * buf): node_(node), id_mapper_(id_mapper), buffer_(buf) {
    // TODO(big) new MetaData both in remote and local server
    config_ = Config::GetInstance();
    vpstore_ = NULL;
    epstore_ = NULL;    
}

MetaData::~MetaData() {
    std::cout << "Delete metadata" << std::endl;    
    delete vpstore_;
    delete epstore_;
}

void MetaData::GetRemoteMeta(vector<Node> & remotes) {
    
}

void MetaData::Init(vector<Node> & nodes, GraphMeta& graphmeta) {
    // TODO(big) init vkv ekv vtable etable in remote and local 
    // TODO(big) get v/e meta from remote
    v_array_off_ = graphmeta.v_array_off;
    v_ext_off_ = graphmeta.v_ext_off;
    v_num_ = graphmeta.v_num;

    e_array_off_ = graphmeta.e_array_off;
    e_ext_off_ = graphmeta.e_ext_off;
    e_num_ = graphmeta.e_num;
    
    vpstore_ = new VKVStore_Local(buffer_);
    epstore_ = new EKVStore_Local(buffer_);
    vpstore_->init(nodes, graphmeta.vp_off, graphmeta.vp_num_slots, graphmeta.vp_num_buckets);
    epstore_->init(nodes, graphmeta.ep_off, graphmeta.ep_num_slots, graphmeta.ep_num_buckets);

#ifdef TEST_WITH_COUNT
    InitCounter(); 
#endif
}

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
void MetaData::GetVertex(int tid, vid_t v_id, Vertex& v) {
    char * send_buf = buffer_->GetSendBuf(tid);
    uint64_t v_off = v_array_off_ + v_id.value() * sizeof(Vertex); // LCY: check vid start from

    RDMA &rdma = RDMA::get_rdma();
    rdma.dev->RdmaRead(tid, REMOTE_NID, send_buf, sizeof(Vertex), v_off);

    memcpy(&v, send_buf, sizeof(Vertex));

    #ifdef TEST_WITH_COUNT
        RecordVtx(sizeof(Vertex));
    #endif // DEBUG
    return;
}

int MetaData::GetVPList(int tid, Vertex& v, vector<label_t>& vpl) {
    int size;
    for(size = 0; size < VP_NBS; ++size) {
        if(v.vp_list[size] == 0)
            break;
        vpl.push_back(v.vp_list[size]);
    }
    if(v.ext_vp_ptr.size != 0) {
        // has ext property
        char * send_buf = buffer_->GetSendBuf(tid);
        uint64_t off = v_ext_off_ + v.ext_vp_ptr.off;
        uint64_t sz = v.ext_vp_ptr.size;
        
        RDMA &rdma = RDMA::get_rdma();
        rdma.dev->RdmaRead(tid, REMOTE_NID, send_buf, sz, off);
        
        int num = sz/sizeof(label_t);
        label_t * recv = (label_t*)send_buf;
        for(int i = 0; i<num; ++i) {
            vpl.push_back(recv[i]);
            size++;
        }
        #ifdef TEST_WITH_COUNT
        #endif
    }
    return size;
};

int MetaData::GetInNbs(int tid, Vertex& v, vector<vid_t>& in_nbs) {
    int size;
    for(size = 0; size < IN_NBS; ++size) {
        if(v.in_nbs[size] == 0) 
            break; // TODO(big): check that no vid is 0
        in_nbs.push_back(v.in_nbs[size]);
    }
    if(v.ext_in_nbs_ptr.size != 0) {
        // has ext in nbs
        char * send_buf = buffer_->GetSendBuf(tid);
        uint64_t off = v_ext_off_ + v.ext_in_nbs_ptr.off;
        uint64_t sz = v.ext_in_nbs_ptr.size;
        
        RDMA &rdma = RDMA::get_rdma();
        rdma.dev->RdmaRead(tid, REMOTE_NID, send_buf, sz, off);
        
        int num = sz/sizeof(vid_t);
        vid_t * recv = (vid_t*)send_buf;
        for(int i = 0; i<num; ++i) {
            in_nbs.push_back(recv[i]);
            size++;
        }
    }
    return size;
};

int MetaData::GetOutNbs(int tid, Vertex& v, vector<vid_t>& out_nbs) {
    int size;
    for(size = 0; size < OUT_NBS; ++size) {
        if(v.out_nbs[size] == 0) 
            break; // TODO(big): check that no vid is 0
        out_nbs.push_back(v.out_nbs[size]);
    }
    if(v.ext_out_nbs_ptr.size != 0) {
        // has ext out nbs
        char * send_buf = buffer_->GetSendBuf(tid);
        uint64_t off = v_ext_off_ + v.ext_out_nbs_ptr.off;
        uint64_t sz = v.ext_out_nbs_ptr.size;
        
        RDMA &rdma = RDMA::get_rdma();
        rdma.dev->RdmaRead(tid, REMOTE_NID, send_buf, sz, off);
        
        int num = sz/sizeof(vid_t);
        vid_t * recv = (vid_t*)send_buf;
        for(int i = 0; i<num; ++i) {
            out_nbs.push_back(recv[i]);
            size++;
        }
    }
    return size;
};

void MetaData::GetEdge(int tid, eid_t e_id, Edge& e) {
    char * send_buf = buffer_->GetSendBuf(tid);
    uint64_t e_off = e_array_off_ + e_id.tmp_hash(e_num_) * sizeof(Edge); // LCY: tmp usage, not correct

    RDMA &rdma = RDMA::get_rdma();
    rdma.dev->RdmaRead(tid, REMOTE_NID, send_buf, sizeof(Edge), e_off);
    memcpy(&e, send_buf, sizeof(Edge));

    #ifdef TEST_WITH_COUNT
        RecordEdg(sizeof(Edge));
    #endif
    return;
}

int MetaData::GetEPList(int tid, Edge& e, vector<label_t>& epl) {
    int size;
    for(size = 0; size < EP_NBS; ++size) {
        if(e.ep_list[size] == 0)    
            break;
        epl.push_back(e.ep_list[size]);
    }
    assert(e.ext_ep_ptr.size == 0);
    return size;    
}

void MetaData::GetAllVertices(int tid, vector<vid_t> & vid_list) {
    // TODO(big) get vertex from remote

    uint64_t buf_size = buffer_->GetSendBufSize();
    int per_read_num = buf_size/sizeof(Vertex);
    int count = 0;
    int remain = v_num_;
    RDMA_LOG(INFO) << "In get all vtx remain = " << remain << " per read num = " << per_read_num << " v_array_off = " << v_array_off_;
    while(remain > 0) {
        int read_sz = remain > per_read_num ? per_read_num : remain;
        
        char* send_buf = buffer_->GetSendBuf(tid);
        uint64_t off = v_array_off_ + count * sizeof(Vertex);
        uint64_t size = read_sz * sizeof(Vertex);

        RDMA &rdma = RDMA::get_rdma();
        rdma.dev->RdmaRead(tid, REMOTE_NID, send_buf, size, off);
        Vertex* v = (Vertex *)send_buf;

        for(int i = 0; i < read_sz; ++i) {
            vid_list.push_back(v[i].id);
        }

        count += read_sz;
        remain -= read_sz;
    }

    #ifdef TEST_WITH_COUNT
        RecordVtx(sizeof(Vertex)*v_num_);
    #endif
}

void MetaData::GetAllEdges(int tid, vector<eid_t> & eid_list) {
    // TODO(big) get all edge from remote
    uint64_t buf_size = buffer_->GetSendBufSize();
    int per_read_num = buf_size/sizeof(Edge);
    int count = 0;
    int remain = e_num_;
    while(remain > 0) {
        int read_sz = remain > per_read_num ? per_read_num : remain;
        
        char* send_buf = buffer_->GetSendBuf(tid);
        uint64_t off = e_array_off_ + count * sizeof(Edge);
        uint64_t size = read_sz * sizeof(Edge);

        RDMA &rdma = RDMA::get_rdma();
        rdma.dev->RdmaRead(tid, REMOTE_NID, send_buf, size, off);
        Edge* e = (Edge *)send_buf;

        for(int i = 0; i < read_sz; ++i) {
            eid_list.push_back(e[i].id);
        }

        count += read_sz;
        remain -= read_sz;
    }

    #ifdef TEST_WITH_COUNT
        RecordVtx(sizeof(Edge)*e_num_);
    #endif
}

bool MetaData::VPKeyIsLocal(vpid_t vp_id) {
    // TODO(big) delete now all vp key is remote
    // if (id_mapper_->IsVPropertyLocal(vp_id)) {
    //     return true;
    // } else {
    //     return false;
    // }
    return false;
}

bool MetaData::EPKeyIsLocal(epid_t ep_id) {
    // TODO(big) delete now all epkey is remote
    // if (id_mapper_->IsEPropertyLocal(ep_id)) {
    //     return true;
    // } else {
    //     return false;
    // }
    return false;
}

bool MetaData::GetPropertyForVertex(int tid, vpid_t vp_id, value_t & val) {
    vpstore_->get_property_remote(tid, REMOTE_NID, vp_id.value(), val);

    #ifdef TEST_WITH_COUNT
        RecordVp(val.content.size());
    #endif
    if (val.content.size())
        return true;
    return false;
}

bool MetaData::GetPropertyForEdge(int tid, epid_t ep_id, value_t & val) {
    // TODO(big) get all property from remote
    // if (id_mapper_->IsEPropertyLocal(ep_id)) {      // locally
    //     epstore_->get_property_local(ep_id.value(), val);
    // } else {                                        // remotely
    //     epstore_->get_property_remote(tid, id_mapper_->GetMachineIdForEProperty(ep_id), ep_id.value(), val);
    // }
    epstore_->get_property_remote(tid, id_mapper_->GetMachineIdForEProperty(ep_id), ep_id.value(), val);


    #ifdef TEST_WITH_COUNT
        RecordEp(val.content.size());
    #endif
    if (val.content.size())
        return true;
    return false;
}

bool MetaData::GetLabelForVertex(int tid, vid_t vid, label_t & label) {
    // TODO(big) get all label from 
    label = 0;
    vpid_t vp_id(vid, 0);
    // if (id_mapper_->IsVPropertyLocal(vp_id)) {      // locally
        // vpstore_->get_label_local(vp_id.value(), label);
    // } else {                                        // remotely
        vpstore_->get_label_remote(tid, id_mapper_->GetMachineIdForVProperty(vp_id), vp_id.value(), label);
    // }
    #ifdef TEST_WITH_COUNT 
        RecordVp(sizeof(label_t));
    #endif
    return label;
}

bool MetaData::GetLabelForEdge(int tid, eid_t eid, label_t & label) {
    // TODO(big) get all edge label from remote
    label = 0;
    epid_t ep_id(eid, 0);
    // if (id_mapper_->IsEPropertyLocal(ep_id)) {      // locally
    //     epstore_->get_label_local(ep_id.value(), label);
    // } else {                                        // remotely
    //     epstore_->get_label_remote(tid, id_mapper_->GetMachineIdForEProperty(ep_id), ep_id.value(), label);
    // }
    epstore_->get_label_remote(tid, id_mapper_->GetMachineIdForEProperty(ep_id), ep_id.value(), label);

     #ifdef TEST_WITH_COUNT
        RecordEp(sizeof(label_t));
    #endif
    return label;
}

int MetaData::GetMachineIdForVertex(vid_t v_id) {
    return id_mapper_->GetMachineIdForVertex(v_id);
}

int MetaData::GetMachineIdForEdge(eid_t e_id) {
    return id_mapper_->GetMachineIdForEdge(e_id);
}

void MetaData::GetNameFromIndex(Index_T type, label_t id, string & str) {
    unordered_map<label_t, string>::const_iterator itr;

    switch (type) {
        case Index_T::E_LABEL:
            itr = indexes.el2str.find(id);
            if (itr == indexes.el2str.end()) {
                return;
            } else {
                str = itr->second;
            }
            break;
        case Index_T::E_PROPERTY:
            itr = indexes.epk2str.find(id);
            if (itr == indexes.epk2str.end()) {
                return;
            } else {
                str = itr->second;
            }
            break;
        case Index_T::V_LABEL:
            itr = indexes.vl2str.find(id);
            if (itr == indexes.vl2str.end()) {
                return;
            } else {
                str = itr->second;
            }
            break;
        case Index_T::V_PROPERTY:
            itr = indexes.vpk2str.find(id);
            if (itr == indexes.vpk2str.end()) {
                return;
            } else {
                str = itr->second;
            }
            break;
        default:
            return;
    }
}

void MetaData::InsertAggData(agg_t key, vector<value_t> & data) {
    lock_guard<mutex> lock(agg_mutex);

    unordered_map<agg_t, vector<value_t>>::iterator itr = agg_data_table.find(key);
    if (itr == agg_data_table.end()) {
        // Not Found, insert
        agg_data_table.insert(pair<agg_t, vector<value_t>>(key, data));
    } else {
        agg_data_table.at(key).insert(agg_data_table.at(key).end(), data.begin(), data.end());
    }
}

void MetaData::GetAggData(agg_t key, vector<value_t> & data) {
    lock_guard<mutex> lock(agg_mutex);

    unordered_map<agg_t, vector<value_t>>::iterator itr = agg_data_table.find(key);
    if (itr != agg_data_table.end()) {
        data = itr->second;
    }
}

void MetaData::DeleteAggData(agg_t key) {
    lock_guard<mutex> lock(agg_mutex);

    unordered_map<agg_t, vector<value_t>>::iterator itr = agg_data_table.find(key);
    if (itr != agg_data_table.end()) {
        agg_data_table.erase(itr);
    }
}

// void MetaData::AccessVProperty(uint64_t vp_id_v, value_t & val) {
//     // TODO(big) delete use when RDMA is disabled
//     vpstore_->get_property_local(vp_id_v, val);
//     #ifdef TEST_WITH_COUNT
//         RecordVp(val.content.size());
//     #endif
// }

// void MetaData::AccessEProperty(uint64_t ep_id_v, value_t & val) {
//     // TODO(big) delete use when RDMA is disabled
//     epstore_->get_property_local(ep_id_v, val);
//     #ifdef TEST_WITH_COUNT
//         RecordEp(val.content.size());
//     #endif
// }

void MetaData::get_string_indexes() {
    // TODO(big) string index cached in local
    const string INDEX_PATH = "./data/sf0.1/output/index/";
    ifstream file;
    string string_line;

    string el_path = INDEX_PATH + "./edge_label";
    file.open(el_path);
    if(!file.is_open()) {
        std::cout << "Error when open edge label index file" << std::endl;
        return;
    }
    while(getline(file, string_line)) {
        char *line = const_cast<char*>(string_line.c_str());
        char * pch;
        pch = strtok(line, "\t");
        string key(pch);
        pch = strtok(NULL, "\t");
        label_t id = atoi(pch);

        // both string and ID are unique
        assert(indexes.str2el.find(key) == indexes.str2el.end());
        assert(indexes.el2str.find(id) == indexes.el2str.end());

        indexes.str2el[key] = id;
        indexes.el2str[id] = key;
    }
    file.close();

    string epk_path = INDEX_PATH + "./edge_property_index";
    file.open(epk_path);
    if(!file.is_open()) {
        std::cout << "Error when open edge property index file" << std::endl;
        return;
    }
    while (getline(file, string_line)) {
        char * line = const_cast<char*>(string_line.c_str());
        char * pch;
        pch = strtok(line, "\t");
        string key(pch);
        pch = strtok(NULL, "\t");
        label_t id = atoi(pch);
        pch = strtok(NULL, "\t");
        indexes.str2eptype[to_string(id)] = atoi(pch);

        // both string and ID are unique
        assert(indexes.str2epk.find(key) == indexes.str2epk.end());
        assert(indexes.epk2str.find(id) == indexes.epk2str.end());

        indexes.str2epk[key] = id;
        indexes.epk2str[id] = key;
    }
    file.close();

    string vl_path = INDEX_PATH + "./vtx_label";
    file.open(vl_path);
    if(!file.is_open()) {
        std::cout << "Error when open vertex label index file" << std::endl;
        return;
    }
    while (getline(file, string_line)) {
        char * line = const_cast<char*>(string_line.c_str());
        char * pch;
        pch = strtok(line, "\t");
        string key(pch);
        pch = strtok(NULL, "\t");
        label_t id = atoi(pch);

        // both string and ID are unique
        assert(indexes.str2vl.find(key) == indexes.str2vl.end());
        assert(indexes.vl2str.find(id) == indexes.vl2str.end());

        indexes.str2vl[key] = id;
        indexes.vl2str[id] = key;
    }
    file.close();

    string vpk_path = INDEX_PATH + "./vtx_property_index";
    file.open(vpk_path);
    if(!file.is_open()) {
        std::cout << "Error when open vertex property index file" << std::endl;
        return;
    }
    while (getline(file, string_line)) {
        char * line = const_cast<char*>(string_line.c_str());
        char * pch;
        pch = strtok(line, "\t");
        string key(pch);
        pch = strtok(NULL, "\t");
        label_t id = atoi(pch);
        pch = strtok(NULL, "\t");
        indexes.str2vptype[to_string(id)] = atoi(pch);

        // both string and ID are unique
        assert(indexes.str2vpk.find(key) == indexes.str2vpk.end());
        assert(indexes.vpk2str.find(id) == indexes.vpk2str.end());

        indexes.str2vpk[key] = id;
        indexes.vpk2str[id] = key;
    }
    file.close();
}

#ifdef TEST_WITH_COUNT
// test with counter functions
void MetaData::InitCounter() {
    vtx_counter_ = 0;
    vtx_sizes_.clear();
    edg_counter_ = 0;
    edg_sizes_.clear();
    vp_counter_ = 0;
    vp_sizes_.clear();
    ep_counter_ = 0;
    ep_sizes_.clear();
    vin_nbs_counter_ = 0;
    vin_nbs_sizes_.clear();
    vout_nbs_counter_ = 0;
    vout_nbs_sizes_.clear();
}
void MetaData::RecordVtx(int size) {
    vtx_counter_ += 1;
    vtx_sizes_.push_back(size);
}
void MetaData::RecordEdg(int size) {
    edg_counter_ += 1;
    edg_sizes_.push_back(size);
}
void MetaData::RecordVp(int size) {
    vp_counter_ += 1;
    vp_sizes_.push_back(size);
}
void MetaData::RecordEp(int size) {
    ep_counter_ += 1;
    ep_sizes_.push_back(size);
}
void MetaData::RecordVin(int size) {
    vin_nbs_counter_ += 1;
    vin_nbs_sizes_.push_back(size);
}
void MetaData::RecordVout(int size) {
    vout_nbs_counter_ += 1;
    vout_nbs_sizes_.push_back(size);
}
void MetaData::RecordVtxExt(int size) {
    vtx_ext_counter_ += 1;
    vtx_sizes_.push_back(size);
}
void MetaData::PrintCounter() {
    if(vtx_counter_ || vtx_ext_counter_ || edg_counter_ || vin_nbs_counter_ || vout_nbs_counter_ || vp_counter_ || ep_counter_ )
        std::cout << "============================================" << std::endl;

    if(vtx_counter_) {
        std::cout << "Visit vertex table " << vtx_counter_ << std::endl;
        long total = 0;
        for(auto it = vtx_sizes_.begin(); it != vtx_sizes_.end(); ++it) {
            total += *it;
            // std::cout << *it << " ";
        }
        std::cout << "Visit vertex size " << total << std::endl;
    }

    if(vtx_ext_counter_) {
        std::cout << "Visit vtx extension " << vtx_ext_counter_ << std::endl;
        long total = 0;
        for(auto it = vtx_ext_sizes_.begin(); it != vtx_ext_sizes_.end(); ++it) {
            total += *it;
        }
        std::cout << "Visit vtx extension size " << total << std::endl;
    }
    
    if(edg_counter_) {
        std::cout << "Visit edge table " << edg_counter_ << std::endl;
        long total = 0;
        for(auto it = edg_sizes_.begin(); it != edg_sizes_.end(); ++it) {
            total += *it;
            // std::cout << *it << " ";
        }
        std::cout << "Visit edge size " << total << std::endl;
    }

    if(vin_nbs_counter_) {
        std::cout << "Visit vin nbs " << vin_nbs_counter_ << std::endl;
        long total = 0;
        for(auto it = vin_nbs_sizes_.begin(); it != vin_nbs_sizes_.end(); ++it) {
            total += *it;
            // std::cout << *it << " ";
        }
        std::cout << "Visit vin nbs size " << total << std::endl;
    }

    if(vout_nbs_counter_) {
        std::cout << "Visit vout nbs " << vout_nbs_counter_ << std::endl;
        long total = 0;
        for(auto it = vout_nbs_sizes_.begin(); it != vout_nbs_sizes_.end(); ++it) {
            total += *it;
            // std::cout << *it << " ";
        }
        std::cout << "Visit vout nbs size " << total << std::endl;       
    }

    if(vp_counter_) {
        std::cout << "Visit vp table " << vp_counter_ << std::endl;
        long total = 0;
        for(auto it = vp_sizes_.begin(); it != vp_sizes_.end(); ++it) {
            total += *it;
            // std::cout << *it << " ";
        }
        std::cout << "Visit vp size " << total << std::endl;
        // std::cout << std::endl;
    }

    if(ep_counter_) {
        std::cout << "Visit ep table " << ep_counter_ << std::endl;
        long total = 0;
        for(auto it = ep_sizes_.begin(); it != ep_sizes_.end(); ++it) {
            total += *it;
            // std::cout << *it << " ";
        }
        std::cout << "Visit ep size " << total << std::endl;
        // std::cout << std::endl;
    }
    if(vtx_counter_ || vtx_ext_counter_ || edg_counter_ || vin_nbs_counter_ || vout_nbs_counter_ || vp_counter_ || ep_counter_ )
        std::cout << "============================================" << std::endl << std::endl;
}
#endif
