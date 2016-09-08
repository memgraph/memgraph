#pragma once

#include "utils/array_store.hpp"
#include "utils/void.hpp"

// Common menthods for translating Vertex/Edge representations in database to
// other format for extern usage.
// Implementor should override those methods which he needs, and ignore the
// rest.
// Caller is responisble to structure his calls as following:
//      * start_vertex
//          1 label_count
//          * label
//          1 property_count
//          * property_name
//              1 handle
//          1 end_vertex
// or
//      * start_edge
//          0-1 edge_type
//          1 property_count
//          * property_name
//              1 handle
//          1 end_edge
//
// End goal would be to enforce these rules during compile time.
class GraphEncoder
{
public:
    // Starts writing vertex with given id.
    void start_vertex(Id id) {}

    // Number of following label calls.
    void label_count(size_t n);

    // Label of currently started vertex.
    void label(std::string const &l) {}

    // Ends writing vertex
    void end_vertex() {}

    // Starts writing edge from vertex to vertex
    void start_edge(Id id, Id from, Id to) {}

    // Type of currently started edge
    void edge_type(std::string const &et) {}

    // Ends writing edge
    void end_edge() {}

    // Number of following paired property_name,handle calls.
    void property_count(size_t n);

    // Property family name of next property for currently started element.
    void property_name(std::string const &name) {}

    void handle(const Void &v) {}

    void handle(const bool &prop) {}

    void handle(const float &prop) {}

    void handle(const double &prop) {}

    void handle(const int32_t &prop) {}

    void handle(const int64_t &prop) {}

    void handle(const std::string &value) {}

    void handle(const ArrayStore<bool> &) {}

    void handle(const ArrayStore<int32_t> &) {}

    void handle(const ArrayStore<int64_t> &) {}

    void handle(const ArrayStore<float> &) {}

    void handle(const ArrayStore<double> &) {}

    void handle(const ArrayStore<std::string> &) {}
};
