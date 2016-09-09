#pragma once

#include <string>
#include <unordered_map>

#include "communication/bolt/v1/transport/bolt_encoder.hpp"
#include "mvcc/id.hpp"
#include "serialization/graph_encoder.hpp"
#include "serialization/serialization.hpp"
#include "storage/indexes/index_definition.hpp"
#include "utils/stream_wrapper.hpp"

// Represents creation of a snapshot. Contains all necessary informations
// for
// write. Caller is responisble to structure his calls as following:
// * property_name_init
// * label_name_init
// * edge_type_name_init
// 1 start_vertices
//      * <vertex>
// 1 start_edges
//      * <edges>
// 1 start_indexes
//      * index
// 1 end
class SnapshotEncoder : public GraphEncoder
{
public:
    SnapshotEncoder(std::ofstream &stream) : stream(stream) {}

    SnapshotEncoder(SnapshotEncoder const &) = delete;
    SnapshotEncoder(SnapshotEncoder &&) = delete;

    SnapshotEncoder &operator=(SnapshotEncoder const &) = delete;
    SnapshotEncoder &operator=(SnapshotEncoder &&) = delete;

    // Tells in advance which names will be used.
    void property_name_init(std::string const &name);

    // Tells in advance which labels will be used.
    void label_name_init(std::string const &name);

    // Tells in advance which edge_type will be used.
    void edge_type_name_init(std::string const &name);

    // Prepares for vertices
    void start_vertices();

    // Prepares for edges
    void start_edges();

    // Prepares for indexes
    void start_indexes();

    // Writes index definition
    void index(IndexDefinition const &);

    // Finishes snapshot
    void end();

    // *********************From graph encoder
    // Starts writing vertex with given id.
    void start_vertex(Id id);

    // Number of following label calls.
    void label_count(size_t n);

    // Label of currently started vertex.
    void label(std::string const &l);

    // Starts writing edge from vertex to vertex
    void start_edge(Id from, Id to);

    // Type of currently started edge
    void edge_type(std::string const &et);

    // Number of following paired property_name,handle calls.
    void property_count(size_t n);

    // Property family name of next property for currently started element.
    void property_name(std::string const &name);

    void handle(const Void &v);

    void handle(const bool &prop);

    void handle(const float &prop);

    void handle(const double &prop);

    void handle(const int32_t &prop);

    void handle(const int64_t &prop);

    void handle(const std::string &value);

    void handle(const ArrayStore<bool> &);

    void handle(const ArrayStore<int32_t> &);

    void handle(const ArrayStore<int64_t> &);

    void handle(const ArrayStore<float> &);

    void handle(const ArrayStore<double> &);

    void handle(const ArrayStore<std::string> &);

private:
    std::ofstream &stream;
    StreamWrapper<std::ofstream> wrapped = {stream};
    bolt::BoltEncoder<StreamWrapper<std::ofstream>> encoder = {wrapped};

    // Contains for every property_name here snapshot local id.
    std::unordered_map<std::string, size_t> property_name_map;

    // Contains for every label_name here snapshot local id.
    std::unordered_map<std::string, size_t> label_name_map;

    // Contains for every edge_type here snapshot local id.
    std::unordered_map<std::string, size_t> edge_type_name_map;
};
