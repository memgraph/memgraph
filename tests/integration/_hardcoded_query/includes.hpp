#pragma once

#include <cassert>
#include <iostream>
#include <map>
#include <map>
#include <type_traits>
#include <utility>
#include <vector>

#include "communication/bolt/v1/serialization/bolt_serializer.hpp"
#include "communication/bolt/v1/serialization/record_stream.hpp"
#include "database/graph_db.hpp"
#include "database/graph_db.hpp"
#include "database/db_accessor.hpp"
#include "database/db_accessor.hpp"
#include "io/network/socket.hpp"
#include "mvcc/id.hpp"
#include "query/util.hpp"
#include "storage/edge_type/edge_type.hpp"
#include "storage/edge_x_vertex.hpp"
#include "storage/indexes/index_definition.hpp"
#include "storage/label/label.hpp"
#include "storage/model/properties/all.hpp"
#include "storage/model/properties/property.hpp"
#include "utils/border.hpp"
#include "utils/iterator/iterator.hpp"
#include "utils/iterator/iterator.hpp"
#include "utils/option_ptr.hpp"
#include "utils/reference_wrapper.hpp"
#include "utils/variadic/variadic.hpp"

using namespace std;

namespace hardcode
{

using namespace utils;

using query_functions_t =
    std::map<uint64_t, std::function<bool(properties_t &&)>>;

size_t edge_count(DbAccessor& t)
{
    size_t count = 0;
    t.edge_access().fill().for_all([&](auto ea) { ++count; });
    return count;
}

size_t vertex_count(DbAccessor& t)
{
    size_t count = 0;
    t.vertex_access().fill().for_all([&](auto va) { ++count; });
    return count;
}

// DEBUG HELPER METHODS

void print(const VertexAccessor& va)
{
    va.stream_repr(std::cout);
}

void print(const EdgeAccessor& ea)
{
    ea.stream_repr(std::cout);
}

void print_vertices(DbAccessor& t)
{
    utils::println("Vertices:");
    t.vertex_access().fill().for_all(
        [&](auto va) {
            va.stream_repr(std::cout);     
            utils::println("");
        }
    );
    utils::println("");
}

void print_edges(DbAccessor& t)
{
    utils::println("Edges:");
    t.edge_access().fill().for_all(
        [&](auto ea) {
            ea.stream_repr(std::cout);
            println("");
        }
    );
}

void print_entities(DbAccessor& t)
{
    print_vertices(t);
    print_edges(t);
}     

void print_edge_count_(DbAccessor& t)
{
    utils::println("");
    utils::println("__EDGE_COUNT:", edge_count(t));
}

void print_vertex_count_(DbAccessor& t)
{
    utils::println("");
    utils::println("__VERTEX_COUNT:", vertex_count(t));
}

}
