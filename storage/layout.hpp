/* Copyright 2019 Husky Data Lab, CUHK

Authors: Hongzhi Chen (hzchen@cse.cuhk.edu.hk)
*/

#pragma once

#include <cstdint>
#include <vector>
#include <string>
#include <sstream>

#include "base/type.hpp"
#include "base/serialization.hpp"

#define IN_NBS 3
#define OUT_NBS 3
#define VP_NBS 6
#define EP_NBS 1
#define META_SIZE 96

using namespace std;

struct Vertex {
    vid_t id;
    // label_t label;
    vid_t in_nbs[IN_NBS];
    ptr_t ext_in_nbs_ptr;
    vid_t out_nbs[OUT_NBS] ;
    ptr_t ext_out_nbs_ptr;
    label_t vp_list[VP_NBS] ;
    ptr_t ext_vp_ptr;
    // string DebugString() const;
};

ibinstream& operator<<(ibinstream& m, const Vertex& v);

obinstream& operator>>(obinstream& m, Vertex& v);

struct Edge {
    // vid_t v_1;
    // vid_t v_2;
    eid_t id;
    label_t ep_list[EP_NBS];
    ptr_t ext_ep_ptr;
    string DebugString() const;
};

ibinstream& operator<<(ibinstream& m, const Edge& e);

obinstream& operator>>(obinstream& m, Edge& e);

struct V_KVpair {
    vpid_t key;
    value_t value;
    string DebugString() const;
};

ibinstream& operator<<(ibinstream& m, const V_KVpair& pair);

obinstream& operator>>(obinstream& m, V_KVpair& pair);

struct VProperty {
    vid_t id;
    vector<V_KVpair> plist;
    string DebugString() const;
};

ibinstream& operator<<(ibinstream& m, const VProperty& vp);

obinstream& operator>>(obinstream& m, VProperty& vp);

struct E_KVpair {
    epid_t key;
    value_t value;
    string DebugString() const;
};

ibinstream& operator<<(ibinstream& m, const E_KVpair& pair);

obinstream& operator>>(obinstream& m, E_KVpair& pair);

struct EProperty {
    // vid_t v_1;
    // vid_t v_2;
    eid_t id;
    vector<E_KVpair> plist;
    string DebugString() const;
};

ibinstream& operator<<(ibinstream& m, const EProperty& ep);

obinstream& operator>>(obinstream& m, EProperty& ep);

struct GraphMeta {
    // vertex
    uint64_t v_array_off;
    uint64_t v_ext_off;
    uint64_t v_num;

    // edge
    uint64_t e_array_off;
    uint64_t e_ext_off;
    uint64_t e_num;

    // vp
    uint64_t vp_off;
    uint64_t vp_num_slots;
    uint64_t vp_num_buckets;

    //ep
    uint64_t ep_off;
    uint64_t ep_num_slots;
    uint64_t ep_num_buckets;
    
    GraphMeta(uint64_t v_array_off,
            uint64_t v_ext_off,
            uint64_t v_num,
            uint64_t e_array_off,
            uint64_t e_ext_off,
            uint64_t e_num,
            uint64_t vp_off,
            uint64_t vp_num_slots,
            uint64_t vp_num_buckets,
            uint64_t ep_off,
            uint64_t ep_num_slots,
            uint64_t ep_num_buckets) : 
            v_array_off(v_array_off),
            v_ext_off(v_ext_off),
            v_num(v_num),
            e_array_off(e_array_off),
            e_ext_off(e_ext_off),
            e_num(e_num),
            vp_off(vp_off),
            vp_num_slots(vp_num_slots),
            vp_num_buckets(vp_num_buckets),
            ep_off(ep_off),
            ep_num_slots(ep_num_slots),
            ep_num_buckets(ep_num_buckets){}
    GraphMeta() {}
    string DebugString() const;
};

ibinstream& operator<<(ibinstream& m, const GraphMeta& graphmeta);

obinstream& operator>>(obinstream& m, GraphMeta& graphmeta);
