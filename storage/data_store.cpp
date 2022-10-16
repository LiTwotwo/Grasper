/* Copyright 2019 Husky Data Lab, CUHK

Authors: Hongzhi Chen (hzchen@cse.cuhk.edu.hk)
*/

#include "storage/data_store.hpp"
#include "storage/mpi_snapshot.hpp"
#include "storage/snapshot_func.hpp"

DataStore::DataStore(Node & node, AbstractIdMapper * id_mapper, RemoteBuffer * buf): node_(node), id_mapper_(id_mapper), remote_buffer_(buf) {
    // TODO(big) new datastore both in remote and local server
    config_ = Config::GetInstance();
    v_table_ = NULL;
    e_table_ = NULL;
    vpstore_ = NULL;
    epstore_ = NULL;
}

DataStore::~DataStore() {
    delete v_table_;
    delete e_table_;
    delete vpstore_;
    delete epstore_;
}

void DataStore::Init(vector<Node> & nodes) {
    // TODO(big) init vkv ekv vtable etable in remote and local 
    v_table_ = new VertexTable(remote_buffer_);
    e_table_ = new EdgeTable(remote_buffer_);
    vpstore_ = new VKVStore(remote_buffer_);
    epstore_ = new EKVStore(remote_buffer_);
    v_table_->init(&graph_meta_);
    e_table_->init(&graph_meta_);
    vpstore_->init(&graph_meta_, nodes);
    epstore_->init(&graph_meta_, nodes);

#ifdef TEST_WITH_COUNT
    InitCounter(); 
#endif
}

GraphMeta DataStore::GetGraphMeta() {
    return graph_meta_;
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

void DataStore::LoadDataFromHDFS() {
    // MPISnapshot* snapshot = MPISnapshot::GetInstance();

    get_string_indexes();
    cout << "Remote Node " << node_.get_local_rank() << " get_string_indexes() DONE !" << endl;

    get_vertices();
    cout << "Remote Node " << node_.get_local_rank() << " Get_vertices() DONE !" << endl; // tmp vertices have been prepared

    // if (!snapshot->TestRead("vkvstore")) {
    //     get_vplist();
    // }
    get_vplist();
    cout << "Remote Node " << node_.get_local_rank() << " Get_vplist() DONE !" << endl;

    // if (!snapshot->TestRead("ekvstore")) {
    //     get_eplist();
    // }
    get_eplist();
    cout << "Remote Node " << node_.get_local_rank() << " Get_eplist() DONE !" << endl;
}

// void DataStore::ReadSnapshot() {
//     MPISnapshot* snapshot = MPISnapshot::GetInstance();

//     snapshot->ReadData("datastore_v_table", v_table_, ReadHashMapSerImpl);
//     snapshot->ReadData("datastore_e_table", e_table_, ReadHashMapSerImpl);

//     vpstore_->ReadSnapshot();
//     epstore_->ReadSnapshot();
// }

// void DataStore::WriteSnapshot() {
//     MPISnapshot* snapshot = MPISnapshot::GetInstance();

//     snapshot->WriteData("datastore_v_table", v_table_, WriteHashMapSerImpl);
//     snapshot->WriteData("datastore_e_table", e_table_, WriteHashMapSerImpl);

//     vpstore_->WriteSnapshot();
//     epstore_->WriteSnapshot();
// }

void DataStore::DataConverter() {
    // MPISnapshot* snapshot = MPISnapshot::GetInstance();
    // if (!snapshot->TestRead("datastore_v_table")) {
        for (int i = 0 ; i < vertices.size(); ++i) {
            Vertex* m_v = v_table_->insert(vertices[i]->id);
            memcpy((void *)m_v, (void *)vertices[i], sizeof(Vertex));
        }

        //==================insert vp list to struct vertex
        for (int i = 0 ; i < vp_buf.size(); ++i) {
            Vertex* m_v = v_table_->find(vp_buf[i]->vid);
            if (!m_v) {
                cout << "ERROR: FAILED TO MATCH ONE ELEMENT in vp_buf" << endl;
                exit(-1);
            }
            int num_vp = vp_buf[i]->pkeys.size();
            int num_ext_vp = num_vp - VP_NBS;
            uint64_t vp_ext_offset = 0;
            label_t* m_vp_ext = nullptr;
            if(num_ext_vp > 0) {
                vp_ext_offset = v_table_->sync_alloc_ext(sizeof(label_t) * num_ext_vp);
                m_v->ext_vp_ptr = ptr_t(sizeof(label_t) * num_ext_vp, vp_ext_offset);
                m_vp_ext = (label_t *) (v_table_->get_ext() + vp_ext_offset);
            }
            for(int j = 0; j < num_vp; ++j) {
                if(j < VP_NBS)
                    m_v->vp_list[j] = vp_buf[i]->pkeys[j];
                else 
                    m_vp_ext[j-VP_NBS] = vp_buf[i]->pkeys[j];
            }
        }
        // ==================================================

        // clean the vp_buf
        for (int i = 0 ; i < vp_buf.size(); i++) delete vp_buf[i];
        vector<vp_list*>().swap(vp_buf);
        vector<Vertex*>().swap(vertices);   
    // } else {
    //     if (node_.get_local_rank() == MASTER_RANK)
    //         printf("DataConverter snapshot->TestRead('datastore_v_table')\n");
    // }

    // if (!snapshot->TestRead("datastore_e_table")) {
        for (int i = 0 ; i < edges.size(); i++) {
            Edge * m_e = e_table_->insert(edges[i]->id);
            memcpy((void *)m_e, (void *)edges[i], sizeof(Edge));
        }
    // }
    vector<Edge*>().swap(edges);

    // LCY: tmply keep property insert funcitons
    //      but may need to buffer some hash meta(can reference FORD)  
    // if (!snapshot->TestRead("vkvstore")) {
        vpstore_->insert_vertex_properties(vplist);
        // clean the vp_list
        for (int i = 0 ; i < vplist.size(); i++) {
            // cout << vplist[i]->DebugString();  // TEST
            delete vplist[i];
        }
        vector<VProperty*>().swap(vplist);
    // }

    // if (!snapshot->TestRead("ekvstore")) {
        epstore_->insert_edge_properties(eplist);
        // clean the ep_list
        for (int i = 0 ; i < eplist.size(); i++) {
            // cout << eplist[i]->DebugString();  // TEST
            delete eplist[i];
        }
        vector<EProperty*>().swap(eplist);
    // }
}

// void DataStore::InsertAggData(agg_t key, vector<value_t> & data) {
//     lock_guard<mutex> lock(agg_mutex);

//     unordered_map<agg_t, vector<value_t>>::iterator itr = agg_data_table.find(key);
//     if (itr == agg_data_table.end()) {
//         // Not Found, insert
//         agg_data_table.insert(pair<agg_t, vector<value_t>>(key, data));
//     } else {
//         agg_data_table.at(key).insert(agg_data_table.at(key).end(), data.begin(), data.end());
//     }
// }

// void DataStore::GetAggData(agg_t key, vector<value_t> & data) {
//     lock_guard<mutex> lock(agg_mutex);

//     unordered_map<agg_t, vector<value_t>>::iterator itr = agg_data_table.find(key);
//     if (itr != agg_data_table.end()) {
//         data = itr->second;
//     }
// }

// void DataStore::DeleteAggData(agg_t key) {
//     lock_guard<mutex> lock(agg_mutex);

//     unordered_map<agg_t, vector<value_t>>::iterator itr = agg_data_table.find(key);
//     if (itr != agg_data_table.end()) {
//         agg_data_table.erase(itr);
//     }
// }

// void DataStore::AccessVProperty(uint64_t vp_id_v, value_t & val) {
//     // TODO(big) delete use when RDMA is disabled
//     vpstore_->get_property_local(vp_id_v, val);
//     #ifdef TEST_WITH_COUNT
//         RecordVp(val.content.size());
//     #endif
// }

// void DataStore::AccessEProperty(uint64_t ep_id_v, value_t & val) {
//     // TODO(big) delete use when RDMA is disabled
//     epstore_->get_property_local(ep_id_v, val);
//     #ifdef TEST_WITH_COUNT
//         RecordEp(val.content.size());
//     #endif
// }

void DataStore::get_string_indexes() {
    // TODO(big) string index cached in local
    hdfsFS fs = get_hdfs_fs();

    string el_path = config_->HDFS_INDEX_PATH + "./edge_label";
    hdfsFile el_file = get_r_handle(el_path.c_str(), fs);
    LineReader el_reader(fs, el_file);
    while (true) {
        el_reader.read_line();
        if (!el_reader.eof()) {
            char * line = el_reader.get_line();
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
        } else {
            break;
        }
    }
    hdfsCloseFile(fs, el_file);

    string epk_path = config_->HDFS_INDEX_PATH + "./edge_property_index";
    hdfsFile epk_file = get_r_handle(epk_path.c_str(), fs);
    LineReader epk_reader(fs, epk_file);
    while (true) {
        epk_reader.read_line();
        if (!epk_reader.eof()) {
            char * line = epk_reader.get_line();
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
        } else {
            break;
        }
    }
    hdfsCloseFile(fs, epk_file);

    string vl_path = config_->HDFS_INDEX_PATH + "./vtx_label";
    hdfsFile vl_file = get_r_handle(vl_path.c_str(), fs);
    LineReader vl_reader(fs, vl_file);
    while (true) {
        vl_reader.read_line();
        if (!vl_reader.eof()) {
            char * line = vl_reader.get_line();
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
        } else {
            break;
        }
    }
    hdfsCloseFile(fs, vl_file);

    string vpk_path = config_->HDFS_INDEX_PATH + "./vtx_property_index";
    hdfsFile vpk_file = get_r_handle(vpk_path.c_str(), fs);
    LineReader vpk_reader(fs, vpk_file);
    while (true) {
        vpk_reader.read_line();
        if (!vpk_reader.eof()) {
            char * line = vpk_reader.get_line();
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
        } else {
            break;
        }
    }
    hdfsCloseFile(fs, vpk_file);
    hdfsDisconnect(fs);
}

void DataStore::get_vertices() {
    // MPISnapshot* snapshot = MPISnapshot::GetInstance();
    // // break if the vtxs has already been finished.
    // if (snapshot->TestRead("datastore_v_table")) {
    //     if (node_.get_local_rank() == MASTER_RANK)
    //         printf("get_vertices snapshot->TestRead('datastore_v_table')\n");
    //     return;
    // }

    // // check path + arrangement
    const char * indir = config_->HDFS_VTX_SUBFOLDER.c_str();

    if (node_.get_local_rank() == MASTER_RANK) {
        if (dir_check(indir) == -1)
            exit(-1);
    }

    if (node_.get_local_rank() == MASTER_RANK) {
        vector<vector<string>> arrangement = dispatch_locality(indir, node_.get_local_size());
        // master_scatter(node_, false, arrangement);
        vector<string>& assigned_splits = arrangement[0];
        // reading assigned splits (map)
        for (vector<string>::iterator it = assigned_splits.begin(); it != assigned_splits.end(); it++)
            load_vertices(it->c_str());
    } 
    else {
        vector<string> assigned_splits;
        slave_scatter(node_, false, assigned_splits);
        // reading assigned splits (map)
        for (vector<string>::iterator it = assigned_splits.begin(); it != assigned_splits.end(); it++)
            load_vertices(it->c_str());
    }
}

void DataStore::load_vertices(const char* inpath) {
    // TODO(big) load vertex from remote server
    hdfsFS fs = get_hdfs_fs();
    hdfsFile in = get_r_handle(inpath, fs);
    LineReader reader(fs, in);
    while (true) {
        reader.read_line();
        if (!reader.eof()) {
            Vertex * v = to_vertex(reader.get_line());
            vertices.push_back(v);
            graph_meta_.v_num++;
        } else {
            break;
        }
    }
    hdfsCloseFile(fs, in);
    hdfsDisconnect(fs);
}

// TODO(big) all these functions are used in load data, local or remote?
// Format
// vid [\t] #in_nbs [\t] nb1 [space] nb2 [space] ... #out_nbs [\t] nb1 [space] nb2 [space] ...
Vertex* DataStore::to_vertex(char* line) {
    Vertex * v = new Vertex;

    char * pch;
    pch = strtok(line, "\t");

    vid_t vid(atoi(pch));
    v->id = vid;

    pch = strtok(NULL, "\t");
    int num_in_nbs = atoi(pch);
    int in_ext_num = num_in_nbs - IN_NBS;
    uint64_t in_ext_offset;
    vid_t* m_in_ext;
    if (in_ext_num > 0) {
        in_ext_offset = v_table_->sync_alloc_ext(sizeof(vid_t)*in_ext_num);
        v->ext_in_nbs_ptr = ptr_t(sizeof(vid_t)*in_ext_num, in_ext_offset);
        m_in_ext = (vid_t *)(v_table_->get_ext() + in_ext_offset);
    }
    for (int i = 0 ; i < num_in_nbs; ++i) {
        pch = strtok(NULL, " ");
        if (i < IN_NBS)
            v->in_nbs[i] = atoi(pch); // LCY: need to change to vid type?
        else {
            m_in_ext[i-IN_NBS] = atoi(pch);
        }
    }

    pch = strtok(NULL, "\t");
    int num_out_nbs = atoi(pch);
    int out_ext_num = num_out_nbs - OUT_NBS;
    uint64_t out_ext_offset;
    vid_t* m_out_ext;
    if (out_ext_num > 0) {
        out_ext_offset = v_table_->sync_alloc_ext(sizeof(vid_t)*out_ext_num);
        v->ext_out_nbs_ptr = ptr_t(sizeof(vid_t)*out_ext_num, out_ext_offset);
        m_out_ext = (vid_t *)(v_table_->get_ext() + out_ext_offset);
    }
    for (int i = 0 ; i < num_out_nbs; ++i) {
        pch = strtok(NULL, " ");
        if (i < OUT_NBS)
            v->out_nbs[i] = atoi(pch);
        else {
            m_out_ext[i-OUT_NBS] = atoi(pch);
        }
    }
    return v;
}

void DataStore::get_vplist() {
    // check path + arrangement
    const char * indir = config_->HDFS_VP_SUBFOLDER.c_str();
    if (node_.get_local_rank() == MASTER_RANK) {
        if (dir_check(indir) == -1)
            exit(-1);
    }

    if (node_.get_local_rank() == MASTER_RANK) {
        vector<vector<string>> arrangement = dispatch_locality(indir, node_.get_local_size());
        // master_scatter(node_, false, arrangement);
        vector<string>& assigned_splits = arrangement[0];
        // reading assigned splits (map)
        for (vector<string>::iterator it = assigned_splits.begin(); it != assigned_splits.end(); it++)
            load_vplist(it->c_str());
    } 
    // else {
    //     vector<string> assigned_splits;
    //     slave_scatter(node_, false, assigned_splits);
    //     // reading assigned splits (map)
    //     for (vector<string>::iterator it = assigned_splits.begin(); it != assigned_splits.end(); it++)
    //         load_vplist(it->c_str());
    // }
}

void DataStore::load_vplist(const char* inpath) {
    hdfsFS fs = get_hdfs_fs();
    hdfsFile in = get_r_handle(inpath, fs);
    LineReader reader(fs, in);
    while (true) {
        reader.read_line();
        if (!reader.eof()) {
            to_vp(reader.get_line(), vplist, vp_buf);
        } else {
            break;
        }
    }
    hdfsCloseFile(fs, in);
    hdfsDisconnect(fs);
}

// Format
// vid [\t] label[\t] [kid:value,kid:value,...]
void DataStore::to_vp(char* line, vector<VProperty*> & vplist, vector<vp_list*> & vp_buf) {
    VProperty * vp = new VProperty;
    vp_list * vpl = new vp_list;

    char * pch;
    pch = strtok(line, "\t");
    vid_t vid(atoi(pch));
    vp->id = vid;
    vpl->vid = vid;

    pch = strtok(NULL, "\t");
    label_t label = (label_t)atoi(pch);

    // insert label to VProperty
    V_KVpair v_pair;
    v_pair.key = vpid_t(vid, 0);
    Tool::str2int(to_string(label), v_pair.value);
    // push to property_list of v
    vp->plist.push_back(v_pair);

    pch = strtok(NULL, "");
    string s(pch);

    vector<string> kvpairs;
    Tool::splitWithEscape(s, "[],:", kvpairs);
    assert(kvpairs.size() % 2 == 0);
    for (int i = 0 ; i < kvpairs.size(); i += 2) {
        kv_pair p;
        Tool::get_kvpair(kvpairs[i], kvpairs[i+1], indexes.str2vptype[kvpairs[i]], p);
        V_KVpair v_pair;
        v_pair.key = vpid_t(vid, p.key);
        v_pair.value = p.value;

        // push to property_list of v
        vp->plist.push_back(v_pair);

        // for property index on v
        vpl->pkeys.push_back((label_t)p.key);
    }

    // sort p_list in vertex
    sort(vpl->pkeys.begin(), vpl->pkeys.end());
    vplist.push_back(vp);
    vp_buf.push_back(vpl);

    // cout << "####### " << vp->DebugString(); //DEBUG
}

void DataStore::get_eplist() {
    // check path + arrangement
    const char * indir = config_->HDFS_EP_SUBFOLDER.c_str();
    if (node_.get_local_rank() == MASTER_RANK) {
        if (dir_check(indir) == -1)
            exit(-1);
    }

    if (node_.get_local_rank() == MASTER_RANK) {
        vector<vector<string>> arrangement = dispatch_locality(indir, node_.get_local_size());
        // master_scatter(node_, false, arrangement);
        vector<string>& assigned_splits = arrangement[0];
        // reading assigned splits (map)
        for (vector<string>::iterator it = assigned_splits.begin(); it != assigned_splits.end(); it++)
            load_eplist(it->c_str());
    } 
    // else {
    //     vector<string> assigned_splits;
    //     slave_scatter(node_, false, assigned_splits);
    //     // reading assigned splits (map)
    //     for (vector<string>::iterator it = assigned_splits.begin(); it != assigned_splits.end(); it++)
    //         load_eplist(it->c_str());
    // }
}

void DataStore::load_eplist(const char* inpath) {
    hdfsFS fs = get_hdfs_fs();
    hdfsFile in = get_r_handle(inpath, fs);
    LineReader reader(fs, in);
    while (true) {
        reader.read_line();
        if (!reader.eof()) {
            to_ep(reader.get_line(), eplist);
            graph_meta_.e_num++;
         } else {
            break;
         }
    }
    hdfsCloseFile(fs, in);
    hdfsDisconnect(fs);
}

// Format
// in-v[\t] out-v[\t] label[\t] [kid:value,kid:value,...]
void DataStore::to_ep(char* line, vector<EProperty*> & eplist) {
    Edge * e = new Edge;
    EProperty * ep = new EProperty;

    uint64_t atoi_time = timer::get_usec();
    char * pch;
    pch = strtok(line, "\t");
    int out_v = atoi(pch);
    pch = strtok(NULL, "\t");
    int in_v = atoi(pch);

    eid_t eid(in_v, out_v);
    e->id = eid;
    ep->id = eid;

    pch = strtok(NULL, "\t");
    label_t label = (label_t)atoi(pch);
    // insert label to EProperty
    E_KVpair e_pair;
    e_pair.key = epid_t(in_v, out_v, 0);
    Tool::str2int(to_string(label), e_pair.value);
    // push to property_list of v
    ep->plist.push_back(e_pair);
    pch = strtok(NULL, "");
    string s(pch);
    vector<label_t> pkeys;

    vector<string> kvpairs;
    Tool::splitWithEscape(s, "[],:", kvpairs);
    assert(kvpairs.size() % 2 == 0);

    for (int i = 0 ; i < kvpairs.size(); i += 2) {
        kv_pair p;
        Tool::get_kvpair(kvpairs[i], kvpairs[i+1], indexes.str2eptype[kvpairs[i]], p);

        E_KVpair e_pair;
        e_pair.key = epid_t(in_v, out_v, p.key);
        e_pair.value = p.value;

        ep->plist.push_back(e_pair);
        pkeys.push_back((label_t)p.key);
    }

    sort(pkeys.begin(), pkeys.end());

    //=================Insert ep list
    int num_ep = pkeys.size();
    int ep_ext_num = num_ep - EP_NBS;
    uint64_t ep_ext_offset = 0;
    label_t * m_ep_ext = nullptr;

    if(ep_ext_num > 0) {
        ep_ext_offset = e_table_->sync_alloc_ext(sizeof(label_t)*ep_ext_num);
        e->ext_ep_ptr = ptr_t(sizeof(label_t)*ep_ext_num, ep_ext_offset);
        m_ep_ext = (label_t *)(e_table_->get_ext() + ep_ext_offset);
    }

    for(int i = 0; i < num_ep; ++i) {
        if(i < EP_NBS)
            e->ep_list[i] = pkeys[i];
        else    
            m_ep_ext[i - EP_NBS] = pkeys[i];
    }

    edges.push_back(e);
    eplist.push_back(ep);
}

#ifdef TEST_WITH_COUNT
// test with counter functions
void DataStore::InitCounter() {
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
void DataStore::RecordVtx(int size) {
    vtx_counter_ += 1;
    vtx_sizes_.push_back(size);
}
void DataStore::RecordEdg(int size) {
    edg_counter_ += 1;
    edg_sizes_.push_back(size);
}
void DataStore::RecordVp(int size) {
    vp_counter_ += 1;
    vp_sizes_.push_back(size);
}
void DataStore::RecordEp(int size) {
    ep_counter_ += 1;
    ep_sizes_.push_back(size);
}
void DataStore::RecordVin(int size) {
    vin_nbs_counter_ += 1;
    vin_nbs_sizes_.push_back(size);
}
void DataStore::RecordVout(int size) {
    vout_nbs_counter_ += 1;
    vout_nbs_sizes_.push_back(size);
}
void DataStore::PrintCounter() {
    if(vtx_counter_ || edg_counter_ || vin_nbs_counter_ || vout_nbs_counter_ || vp_counter_ || ep_counter_ )
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
    if(vtx_counter_ || edg_counter_ || vin_nbs_counter_ || vout_nbs_counter_ || vp_counter_ || ep_counter_ )
        std::cout << "============================================" << std::endl << std::endl;
}
#endif
