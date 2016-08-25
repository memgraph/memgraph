#pragma once

#include "storage/indexes/index_record.hpp"
#include "storage/type_group_edge.hpp"
#include "storage/type_group_vertex.hpp"

struct IndexUpdateEdge
{
    EdgeRecord *vlist;
    Edge *record;
};

struct IndexUpdateVertex
{
    VertexRecord *vlist;
    Vertex *record;
};

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
