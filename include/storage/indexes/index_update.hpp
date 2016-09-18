#pragma once

#include "storage/indexes/index_record.hpp"
#include "storage/type_group_edge.hpp"
#include "storage/type_group_vertex.hpp"

// Record for updating indexes of edge
struct IndexUpdateEdge
{
    EdgeRecord *vlist;
    Edge *record;
};

// Record for updatin indexes of vertex
struct IndexUpdateVertex
{
    VertexRecord *vlist;
    Vertex *record;
};

// based of IndexUpdate objects IndexRecords are created
// at the end of transaction (inside commit called on DbAccessor)
struct IndexUpdate
{
    enum
    {
        EDGE,
        VERTEX
    } tag;

    union
    {
        IndexUpdateEdge e;
        IndexUpdateVertex v;
    };
};

template <class T, class V>
IndexUpdate make_index_update(V *vlist, T *record);

template <>
IndexUpdate make_index_update(EdgeRecord *vlist, Edge *record);

template <>
IndexUpdate make_index_update(VertexRecord *vlist, Vertex *record);
