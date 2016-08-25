#include "storage/indexes/index_update.hpp"

template <>
IndexUpdate make_index_update(EdgeRecord *vlist, Edge *record)
{
    return IndexUpdate{IndexUpdate::EDGE, .e = IndexUpdateEdge{vlist, record}};
}

template <>
IndexUpdate make_index_update(VertexRecord *vlist, Vertex *record)
{
    return IndexUpdate{IndexUpdate::VERTEX,
                       .v = IndexUpdateVertex{vlist, record}};
}
