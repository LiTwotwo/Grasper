/* Copyright 2019 Husky Data Lab, CUHK

Authors: Hongzhi Chen (hzchen@cse.cuhk.edu.hk)
*/

#include "storage/layout.hpp"

// string Vertex::DebugString() const {
//     stringstream ss;
//     ss << "Vertex: { id = " << id.vid << " in_nbs = [";
//     for (auto & vid : in_nbs)
//         ss << vid.vid << ", ";
//     ss << "] out_nbs = [";
//     for (auto & vid : out_nbs)
//         ss << vid.vid << ", ";
//     ss << "] vp_list = [";
//     for (auto & p : vp_list)
//         ss << p << ", ";
//     ss << "]}" << endl;
//     return ss.str();
// }

ibinstream& operator<<(ibinstream& m, const Vertex& v) {
    m << v.id;
    // m << v.label;
    for(int i = 0; i<IN_NBS; ++i) {
        m << v.in_nbs[i];
    }
    m << v.ext_in_nbs_ptr;
    for(int i = 0; i<OUT_NBS; ++i) {
        m << v.out_nbs[i];
    }
    m << v.ext_out_nbs_ptr;
    for(int i = 0; i<VP_NBS; ++i) {
        m << v.vp_list[i];
    }
    m << v.ext_vp_ptr;
    return m;
}

obinstream& operator>>(obinstream& m, Vertex& v) {
    m >> v.id;
    // m >> v.label;
    for(int i = 0; i<IN_NBS; ++i) {
        m >> v.in_nbs[i];
    }
    m >> v.ext_in_nbs_ptr;
    for(int i = 0; i<OUT_NBS; ++i) {
        m >> v.out_nbs[i];
    }
    m >> v.ext_out_nbs_ptr;
    for(int i = 0; i<VP_NBS; ++i) {
        m >> v.vp_list[i];
    }
    m >> v.ext_vp_ptr;
    return m;
}

string Edge::DebugString() const {
    stringstream ss;
    ss << "Edge: { id = " << id.in_v << "," << id.out_v <<  " ep_list = [";
    for (auto & ep : ep_list)
        ss << ep << ", ";
    ss << "]}" << endl;
    return ss.str();
}

ibinstream& operator<<(ibinstream& m, const Edge& e) {
    m << e.id;
    // m << e.label;
    for(int i = 0; i<EP_NBS; ++i) {
        m << e.ep_list[i];
    }
    m << e.ext_ep_ptr;
    return m;
}

obinstream& operator>>(obinstream& m, Edge& e) {
    m >> e.id;
    // m >> e.label;
    for(int i = 0; i<EP_NBS; ++i) {
        m >> e.ep_list[i];
    }
    m >> e.ext_ep_ptr;
    return m;
}

string V_KVpair::DebugString() const {
    stringstream ss;
    ss << "V_KVpair: { key = " << key.vid << "|" << key.pid << ", value.type = " << (int)value.type << " }" << endl;
    return ss.str();
}

ibinstream& operator<<(ibinstream& m, const V_KVpair& pair) {
    m << pair.key;
    m << pair.value;
    return m;
}

obinstream& operator>>(obinstream& m, V_KVpair& pair) {
    m >> pair.key;
    m >> pair.value;
    return m;
}

string VProperty::DebugString() const {
    stringstream ss;
    ss << "VProperty: { id = " << id.vid <<  " plist = [" << endl;
    for (auto & vp : plist)
        ss << vp.DebugString();
    ss << "]}" << endl;
    return ss.str();
}

ibinstream& operator<<(ibinstream& m, const VProperty& vp) {
    m << vp.id;
    m << vp.plist;
    return m;
}

obinstream& operator>>(obinstream& m, VProperty& vp) {
    m >> vp.id;
    m >> vp.plist;
    return m;
}

string E_KVpair::DebugString() const {
    stringstream ss;
    ss << "E_KVpair: { key = " << key.in_vid << "|" << key.out_vid << "|" << key.pid << ", value.type = " << (int)value.type << " }" << endl;
    return ss.str();
}

ibinstream& operator<<(ibinstream& m, const E_KVpair& pair) {
    m << pair.key;
    m << pair.value;
    return m;
}

obinstream& operator>>(obinstream& m, E_KVpair& pair) {
    m >> pair.key;
    m >> pair.value;
    return m;
}

string EProperty::DebugString() const {
    stringstream ss;
    ss << "EProperty: { id = " << id.in_v << "," << id.out_v <<  " plist = [" << endl;
    for (auto & ep : plist)
        ss << ep.DebugString();
    ss << "]}" << endl;
    return ss.str();
}

ibinstream& operator<<(ibinstream& m, const EProperty& ep) {
    m << ep.id;
    m << ep.plist;
    return m;
}

obinstream& operator>>(obinstream& m, EProperty& ep) {
    m >> ep.id;
    m >> ep.plist;
    return m;
}

    // uint64_t v_array_off;
    // uint64_t v_ext_off;
    // uint64_t v_num;

    // // edge
    // uint64_t e_array_off;
    // uint64_t e_ext_off;
    // uint64_t e_num;

    // // vp
    // uint64_t vp_off;
    // uint64_t vp_num_slots;
    // uint64_t vp_num_buckets;

    // //ep
    // uint64_t ep_off;
    // uint64_t ep_num_slots;
    // uint64_t ep_num_buckets;

string GraphMeta::DebugString() const {
    stringstream ss;
    ss << "v_array_off = " << v_array_off << " v_ext_off = " << v_ext_off << " v_num = " << v_num << endl;
    ss << "e_array_off = " << e_array_off << " e_ext_off = " << e_ext_off << " e_num = " << e_num << endl;
    ss << "vp_off = " << vp_off << " vp_num_slots = " << vp_num_slots << " vp_num_buckets = " << vp_num_buckets << endl;
    ss << "ep_off = " << ep_off << " ep_num_slots = " << ep_num_slots << " ep_num_buckets = " << ep_num_buckets << endl;
    return ss.str();
}

ibinstream& operator<<(ibinstream& m, const GraphMeta& graphmeta) {
    m << graphmeta.v_array_off;
    m << graphmeta.v_ext_off;
    m << graphmeta.v_num;
    m << graphmeta.e_array_off;
    m << graphmeta.e_ext_off;
    m << graphmeta.e_num;
    m << graphmeta.vp_off;
    m << graphmeta.vp_num_slots;
    m << graphmeta.vp_num_buckets;
    m << graphmeta.ep_off;
    m << graphmeta.ep_num_slots;
    m << graphmeta.ep_num_buckets;
    return m;
}

obinstream& operator>>(obinstream& m, GraphMeta& graphmeta) {
    m >> graphmeta.v_array_off;
    m >> graphmeta.v_ext_off;
    m >> graphmeta.v_num;
    m >> graphmeta.e_array_off;
    m >> graphmeta.e_ext_off;
    m >> graphmeta.e_num;
    m >> graphmeta.vp_off;
    m >> graphmeta.vp_num_slots;
    m >> graphmeta.vp_num_buckets;
    m >> graphmeta.ep_off;
    m >> graphmeta.ep_num_slots;
    m >> graphmeta.ep_num_buckets;
    return m;
}

