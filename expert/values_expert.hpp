/* Copyright 2019 Husky Data Lab, CUHK

Authors: Aaron Li (cjli@cse.cuhk.edu.hk)
*/

#ifndef VALUES_EXPERT_HPP_
#define VALUES_EXPERT_HPP_

#include <string>
#include <vector>

#include "core/message.hpp"
#include "core/abstract_mailbox.hpp"
#include "base/type.hpp"
#include "expert/abstract_expert.hpp"
#include "expert/expert_cache.hpp"
#include "storage/layout.hpp"
#include "storage/metadata.hpp"
#include "utils/tool.hpp"

class ValuesExpert : public AbstractExpert {
 public:
    ValuesExpert(int id, MetaData* metadata, int machine_id, int num_thread, AbstractMailbox * mailbox, CoreAffinity * core_affinity) : AbstractExpert(id, metadata, core_affinity), machine_id_(machine_id), num_thread_(num_thread), mailbox_(mailbox), type_(EXPERT_T::VALUES) {
        config_ = Config::GetInstance();
    }

    // inType, [key]+
    void process(const vector<Expert_Object> & expert_objs, Message & msg) {
        int tid = TidMapper::GetInstance()->GetTid();

        Meta & m = msg.meta;
        Expert_Object expert_obj = expert_objs[m.step];

        Element_T inType = (Element_T)Tool::value_t2int(expert_obj.params.at(0));
        vector<int> key_list;
        for (int cnt = 1; cnt < expert_obj.params.size(); cnt++) {
            key_list.push_back(Tool::value_t2int(expert_obj.params.at(cnt)));
        }

        switch (inType) {
            case Element_T::VERTEX:
                #ifdef OP_BATCH
                    get_properties_for_vertex_batch(tid, key_list, msg.data);
                #else   
                    get_properties_for_vertex(tid, key_list, msg.data);
                #endif
                break;
            case Element_T::EDGE:
                get_properties_for_edge(tid, key_list, msg.data);
                break;
            default:
                cout << "Wrong in type" << endl;
        }

        vector<Message> msg_vec;
        msg.CreateNextMsg(expert_objs, msg.data, num_thread_, metadata_, core_affinity_, msg_vec);

        // Send Message
        for (auto& msg : msg_vec) {
            mailbox_->Send(tid, msg);
        }
    }

 private:
    // Number of threads
    int num_thread_;
    int machine_id_;

    // Expert type
    EXPERT_T type_;

    // Pointer of mailbox
    AbstractMailbox * mailbox_;

    // Cache
    ExpertCache cache;
    Config * config_;

    void get_properties_for_vertex(int tid, vector<int> & key_list, vector<pair<history_t, vector<value_t>>>& data) {
        for (auto & pair : data) {
            vector<value_t> newData;

            for (auto & value : pair.second) {
                vid_t v_id(Tool::value_t2int(value)); 
                Vertex vtx;
                metadata_->GetVertex(tid, v_id, vtx);
                vector<label_t> vp_list;
                metadata_->GetVPList(vtx.label, vp_list);

                if (key_list.empty()) {
                    for (auto & pkey : vp_list) {
                        vpid_t vp_id(v_id, pkey);

                        value_t val;
                        // Try cache
                        if (metadata_->VPKeyIsLocal(vp_id) || !config_->global_enable_caching) {
                            metadata_->GetPropertyForVertex(tid, vp_id, val);
                        } else {
                            if (!cache.get_property_from_cache(vp_id.value(), val)) {
                                // not found in cache
                                metadata_->GetPropertyForVertex(tid, vp_id, val);
                                cache.insert_properties(vp_id.value(), val);
                            }
                        }

                        newData.push_back(val);
                    }
                } else {
                    for (auto key : key_list) {
                        if (find(vp_list.begin(), vp_list.end(), key) == vp_list.end()) {
                            continue;
                        }

                        vpid_t vp_id(v_id, key);
                        value_t val;
                        if (metadata_->VPKeyIsLocal(vp_id) || !config_->global_enable_caching) {
                            metadata_->GetPropertyForVertex(tid, vp_id, val);
                        } else {
                            if (!cache.get_property_from_cache(vp_id.value(), val)) {
                                metadata_->GetPropertyForVertex(tid, vp_id, val);
                                cache.insert_properties(vp_id.value(), val);
                            }
                        }

                        newData.push_back(val);
                    }
                }
            }

            pair.second.swap(newData);
        }
    }
    
    void get_properties_for_vertex_batch(int tid, vector<int> & key_list, vector<pair<history_t, vector<value_t>>>& data) {
        for (auto & pair : data) {
            vector<value_t> newData;
            vector<vid_t> vids;

            for (auto & value : pair.second) {
                vids.emplace_back(Tool::value_t2int(value)); 
            }
            vector<Vertex> vertice;
            metadata_->GetVertexBatch(tid, vids, vertice);
            for(auto vtx: vertice) {
                vector<label_t> vp_list;
                metadata_->GetVPList(vtx.label, vp_list);

                if (key_list.empty()) {
                    for (auto & pkey : vp_list) {
                        vpid_t vp_id(vtx.id, pkey);

                        value_t val;
                        // Try cache
                        if (metadata_->VPKeyIsLocal(vp_id) || !config_->global_enable_caching) {
                            metadata_->GetPropertyForVertex(tid, vp_id, val);
                        } else {
                            if (!cache.get_property_from_cache(vp_id.value(), val)) {
                                // not found in cache
                                metadata_->GetPropertyForVertex(tid, vp_id, val);
                                cache.insert_properties(vp_id.value(), val);
                            }
                        }

                        newData.push_back(val);
                    }
                } else {
                    for (auto key : key_list) {
                        if (find(vp_list.begin(), vp_list.end(), key) == vp_list.end()) {
                            continue;
                        }

                        vpid_t vp_id(vtx.id, key);
                        value_t val;
                        if (metadata_->VPKeyIsLocal(vp_id) || !config_->global_enable_caching) {
                            metadata_->GetPropertyForVertex(tid, vp_id, val);
                        } else {
                            if (!cache.get_property_from_cache(vp_id.value(), val)) {
                                metadata_->GetPropertyForVertex(tid, vp_id, val);
                                cache.insert_properties(vp_id.value(), val);
                            }
                        }

                        newData.push_back(val);
                    }
                }
            }

            pair.second.swap(newData);
        }
    }

    void get_properties_for_edge(int tid, vector<int> & key_list, vector<pair<history_t, vector<value_t>>>& data) {
        for (auto & pair : data) {
            vector<value_t> newData;

            for (auto & value : pair.second) {
                eid_t e_id;
                uint2eid_t(Tool::value_t2uint64_t(value), e_id);
                label_t label;
                metadata_->GetLabelForEdge(tid, e_id, label);
                vector<label_t> ep_list;
                metadata_->GetEPList(label, ep_list);

                if (key_list.empty()) {
                    for (auto & pkey : ep_list) {
                        epid_t ep_id(e_id, pkey);

                        value_t val;
                        if (metadata_->EPKeyIsLocal(ep_id) || !config_->global_enable_caching) {
                            metadata_->GetPropertyForEdge(tid, ep_id, val);
                        } else {
                            if (!cache.get_property_from_cache(ep_id.value(), val)) {
                                // not found in cache
                                metadata_->GetPropertyForEdge(tid, ep_id, val);
                                cache.insert_properties(ep_id.value(), val);
                            }
                        }

                        newData.push_back(val);
                    }
                } else {
                    for (auto key : key_list) {
                        if (find(ep_list.begin(), ep_list.end(), key) == ep_list.end()) {
                            continue;
                        }

                        epid_t ep_id(e_id, key);
                        value_t val;
                        if (metadata_->EPKeyIsLocal(ep_id) || !config_->global_enable_caching) {
                            metadata_->GetPropertyForEdge(tid, ep_id, val);
                        } else {
                            if (!cache.get_property_from_cache(ep_id.value(), val)) {
                                // not found in cache
                                metadata_->GetPropertyForEdge(tid, ep_id, val);
                                cache.insert_properties(ep_id.value(), val);
                            }
                        }

                        newData.push_back(val);
                    }
                }
            }
            pair.second.swap(newData);
        }
    }
};

#endif /* VALUES_EXPERT_HPP_ */
