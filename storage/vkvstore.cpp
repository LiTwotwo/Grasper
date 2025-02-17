/* Copyright 2019 Husky Data Lab, CUHK

Authors: Changji Li (cjli@cse.cuhk.edu.hk)
         Hongzhi Chen (hzchen@cse.cuhk.edu.hk)
*/

#include <assert.h>
#include "storage/vkvstore.hpp"
#include "storage/mpi_snapshot.hpp"
#include "storage/snapshot_func.hpp"


using namespace std;

void VKVStore::ReadSnapshot() {
    MPISnapshot* snapshot = MPISnapshot::GetInstance();

    auto snapshot_tmp_tuple = make_tuple(last_entry, mem_sz, mem);

    if (snapshot->ReadData("vkvstore", snapshot_tmp_tuple, ReadKVStoreImpl)) {
        last_entry = get<0>(snapshot_tmp_tuple);
    }
}

void VKVStore::WriteSnapshot() {
    MPISnapshot* snapshot = MPISnapshot::GetInstance();

    auto snapshot_tmp_tuple = make_tuple(last_entry, mem_sz, mem);
    snapshot->WriteData("vkvstore", snapshot_tmp_tuple, WriteKVStoreImpl);
}

// ==================VKVStore=======================
uint64_t VKVStore::insert_id(uint64_t _pid) {
    // pid is not hashed
    uint64_t bucket_id = _pid % num_buckets;
    uint64_t slot_id = bucket_id * ASSOCIATIVITY;
    uint64_t lock_id = bucket_id % NUM_LOCKS;

    bool found = false;
    pthread_spin_lock(&bucket_locks[lock_id]);
    while (slot_id < num_slots) {
        // the last slot of each bucket is reserved for pointer to indirect header
        // key.pid is used to store the bucket_id of indirect header
        for (int i = 0; i < ASSOCIATIVITY - 1; i++, slot_id++) {
            // assert(vertices[slot_id].key != key);  // no duplicate key
            if (keys[slot_id].pid == _pid) {
                // Cannot get the original pid
                cout << "VKVStore ERROR: conflict at slot["
                     << slot_id << "] of bucket["
                     << bucket_id << "]" << endl;
                assert(false);
            }

            // insert to an empty slot
            if (keys[slot_id].pid == 0) {
                keys[slot_id].pid = _pid;
                goto done;
            }
        }

        // whether the bucket_ext (indirect-header region) is used
        if (!keys[slot_id].is_empty()) {
            slot_id = keys[slot_id].pid * ASSOCIATIVITY;
            continue;  // continue and jump to next bucket
        }

        // allocate and link a new indirect header
        pthread_spin_lock(&bucket_ext_lock);
        if (last_ext >= num_buckets_ext) {
            cout << "VKVStore ERROR: out of indirect-header region." << endl;
            assert(last_ext < num_buckets_ext);
        }
        keys[slot_id].pid = num_buckets + (last_ext++);
        pthread_spin_unlock(&bucket_ext_lock);

        slot_id = keys[slot_id].pid * ASSOCIATIVITY;  // move to a new bucket_ext
        keys[slot_id].pid = _pid;  // insert to the first slot
        goto done;
    }

done:
    pthread_spin_unlock(&bucket_locks[lock_id]);
    assert(slot_id < num_slots);
    assert(keys[slot_id].pid == _pid);
    return slot_id;
}

// Insert all properties for one vertex
void VKVStore::insert_single_vertex_property(VProperty* vp) {
    // Every <vpid_t, value_t>
    for (int i = 0; i < vp->plist.size(); i++) {
        vp_num_++;
        V_KVpair v_kv = vp->plist[i];
        // insert key and get slot_id
        // key do not need to be hash
        int slot_id = insert_id(v_kv.key.value());

        // get length of centent
        uint64_t length = v_kv.value.content.size();

        // allocate for values in entry_region
        uint64_t off = sync_fetch_and_alloc_values(length + 1);

        // insert ptr
        ptr_t ptr = ptr_t(length + 1, off);
        keys[slot_id].ptr = ptr;

        // insert type of value first
        values[off++] = (char)v_kv.value.type;

        // insert value
        memcpy(&values[off], &v_kv.value.content[0], length);
    }
}

// Get current available memory location and reserve enough space for current kv
uint64_t VKVStore::sync_fetch_and_alloc_values(uint64_t n) {
    uint64_t orig;
    pthread_spin_lock(&entry_lock);
    orig = last_entry;
    last_entry += n;
    if (last_entry >= num_entries) {
        cout << "VKVStore ERROR: out of entry region." << endl;
        assert(last_entry < num_entries);
    }
    pthread_spin_unlock(&entry_lock);
    return orig;
}

VKVStore::VKVStore(RemoteBuffer * buf) : buf_(buf) {
    config_ = Config::GetInstance();
    mem = config_->kvstore;
    mem_sz = GiB2B(config_->global_vertex_property_kv_sz_gb);
    offset = config_->kvstore_offset;
    HD_RATIO = config_->key_value_ratio_in_rdma;

    vp_num_ = 0;

    // size for header and entry
    uint64_t header_sz = mem_sz * HD_RATIO / 100;
    uint64_t entry_sz = mem_sz - header_sz;

    // header region
    num_slots = header_sz / sizeof(ikey_t);
    num_buckets = mymath::hash_prime_u64((num_slots / ASSOCIATIVITY) * MHD_RATIO / 100);
    num_buckets_ext = (num_slots / ASSOCIATIVITY) - num_buckets;
    last_ext = 0;

    // entry region
    num_entries = entry_sz;
    last_entry = 0;

    cout << "INFO: vkvstore = " << header_sz + entry_sz << " bytes " << std::endl
         << "      header region: " << num_slots << " slots"
         << " (main = " << num_buckets << ", indirect = " << num_buckets_ext << ")" << std::endl
         << "      entry region: " << num_entries << " entries" << std::endl;

    // Header
    keys = (ikey_t *)(mem);
    // Entry
    values = (char *)(mem + num_slots * sizeof(ikey_t));

    pthread_spin_init(&entry_lock, 0);
    pthread_spin_init(&bucket_ext_lock, 0);
    for (int i = 0; i < NUM_LOCKS; i++)
        pthread_spin_init(&bucket_locks[i], 0);
}

void VKVStore::init(GraphMeta * graph_meta,  vector<Node> & nodes) {
    // initiate keys to 0 which means empty key
    for (uint64_t i = 0; i < num_slots; i++) {
        keys[i] = ikey_t();
    }

    // init graph meta
    graph_meta->vp_off = offset;
    graph_meta->vp_num_slots = num_slots;
    graph_meta->vp_num_buckets = num_buckets;
}

// Insert a list of Vertex properties
void VKVStore::insert_vertex_properties(vector<VProperty*> & vplist) {
    // MPISnapshot* snapshot = MPISnapshot::GetInstance();
    // if (!snapshot->TestRead("vkvstore")) {
        for (int i = 0; i < vplist.size(); i++) {
            insert_single_vertex_property(vplist.at(i));
        }
    // }
}

// analysis
void VKVStore::print_mem_usage() {
    uint64_t used_slots = 0;
    for (uint64_t x = 0; x < num_buckets; x++) {
        uint64_t slot_id = x * ASSOCIATIVITY;
        for (int y = 0; y < ASSOCIATIVITY - 1; y++, slot_id++) {
            if (keys[slot_id].is_empty())
                continue;
            used_slots++;
        }
    }

    cout << "VKVStore main header: " << B2MiB(num_buckets * ASSOCIATIVITY * sizeof(ikey_t))
         << " MB (" << num_buckets * ASSOCIATIVITY << " slots)" << endl;
    cout << "\tused: " << 100.0 * used_slots / (num_buckets * ASSOCIATIVITY)
         << " % (" << used_slots << " slots)" << endl;
    cout << "\tchain: " << 100.0 * num_buckets / (num_buckets * ASSOCIATIVITY)
         << " % (" << num_buckets << " slots)" << endl;

    used_slots = 0;
    for (uint64_t x = num_buckets; x < num_buckets + last_ext; x++) {
        uint64_t slot_id = x * ASSOCIATIVITY;
        for (int y = 0; y < ASSOCIATIVITY - 1; y++, slot_id++) {
            if (keys[slot_id].is_empty())
                continue;
            used_slots++;
        }
    }

    cout << "indirect header: " << B2MiB(num_buckets_ext * ASSOCIATIVITY * sizeof(ikey_t))
         << " MB (" << num_buckets_ext * ASSOCIATIVITY << " slots)" << endl;
    cout << "\talloced: " << 100.0 * last_ext / num_buckets_ext
         << " % (" << last_ext << " buckets)" << endl;
    cout << "\tused: " << 100.0 * used_slots / (num_buckets_ext * ASSOCIATIVITY)
         << " % (" << used_slots << " slots)" << endl;

    cout << "entry: " << B2MiB(num_entries * sizeof(char))
         << " MB (" << num_entries << " entries)" << endl;
    cout << "\tused: " << 100.0 * last_entry / num_entries
         << " % (" << last_entry << " entries)" << endl;
}
