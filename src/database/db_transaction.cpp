#include "database/db_transaction.hpp"

#include "database/graph_db.hpp"
#include "serialization/serialization.hpp"
#include "storage/edge.hpp"
#include "storage/edge_type/edge_type.hpp"
#include "storage/indexes/indexes.hpp"
#include "storage/label/label.hpp"
#include "storage/vertex.hpp"

DbTransaction::DbTransaction(Db &db)
    : Loggable("DbTransaction"), db(db), trans(db.tx_engine.begin())
{
}

// Cleaning for version lists
template <class A>
void clean_version_lists(A &&acc, Id oldest_active)
{
    for (auto &vlist : acc)
    {
        if (vlist.second.gc_deleted(oldest_active))
        {
            // TODO: Optimization, iterator with remove method.
            bool succ = acc.remove(vlist.first);
            // There is other cleaner here
            runtime_assert(succ, "Remove has failed");
        }
    }
}

// Cleans edge part of database. Should be called by one cleaner thread at
// one time.
void DbTransaction::clean_edge_section()
{
    Id oldest_active = trans.oldest_active();

    // Clean indexes
    db.indexes().edge_indexes([&](auto &in) { in.clean(oldest_active); });

    // Clean Edge list
    clean_version_lists(db.graph.edges.access(), oldest_active);
}

// Cleans vertex part of database. Should be called by one cleaner thread at
// one time.
void DbTransaction::clean_vertex_section()
{
    Id oldest_active = trans.oldest_active();

    // Clean indexes
    db.indexes().vertex_indexes([&](auto &in) { in.clean(oldest_active); });

    // Clean vertex list
    clean_version_lists(db.graph.vertices.access(), oldest_active);
}

bool DbTransaction::update_indexes()
{
    logger.trace("index_updates: {}, instance: {}, transaction: {}",
                 index_updates.size(), static_cast<void *>(this), trans.id);

    while (!index_updates.empty())
    {
        auto index_update = index_updates.back();

        if (index_update.tag == IndexUpdate::EDGE)
        {
            auto edge = index_update.e;

            // TODO: This could be done in batch
            // NOTE: This assumes that type index is created with the database.
            if (!edge.record->data.edge_type->index().insert(
                    EdgeTypeIndexRecord(std::nullptr_t(), edge.record,
                                        edge.vlist)))
                return false;

            if (!db.indexes().update_property_indexes<TypeGroupEdge>(edge,
                                                                     trans))
                return false;
        }
        else
        {
            auto vertex = index_update.v;

            for (auto label : vertex.record->data.labels())
            {
                // TODO: This could be done in batch
                // NOTE: This assumes that label index is created with the
                // database.
                if (!label.get().index().insert(LabelIndexRecord(
                        std::nullptr_t(), vertex.record, vertex.vlist)))
                    return false;
            }

            if (!db.indexes().update_property_indexes<TypeGroupVertex>(vertex,
                                                                       trans))
                return false;
        }

        index_updates.pop_back();
    }
    return true;
}

template <class TG>
void DbTransaction::to_update_index(typename TG::vlist_t *vlist,
                                    typename TG::record_t *record)
{
    index_updates.emplace_back(make_index_update(vlist, record));
    logger.trace("update_index, updates_no: {}, instance: {}, transaction: {}",
                 index_updates.size(), static_cast<void *>(this), trans.id);
}

template void DbTransaction::to_update_index<TypeGroupVertex>(
    TypeGroupVertex::vlist_t *vlist, TypeGroupVertex::record_t *record);
template void
DbTransaction::to_update_index<TypeGroupEdge>(TypeGroupEdge::vlist_t *vlist,
                                              TypeGroupEdge::record_t *record);
