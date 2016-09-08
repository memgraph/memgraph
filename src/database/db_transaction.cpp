#include "database/db_transaction.hpp"

#include "database/db.hpp"
#include "serialization/serialization.hpp"
#include "storage/edge.hpp"
#include "storage/edge_type/edge_type.hpp"
#include "storage/indexes/indexes.hpp"
#include "storage/label/label.hpp"
#include "storage/vertex.hpp"

#define TRY(x)                                                                 \
    if (!x) {                                                                  \
        return false;                                                          \
    }

DbTransaction::DbTransaction(Db &db) : db(db), trans(db.tx_engine.begin()) {}

// Cleaning for version lists
template <class A>
void clean_version_lists(A &&acc, Id oldest_active)
{
    for (auto &vlist : acc) {
        if (vlist.second.gc_deleted(oldest_active)) {
            // TODO: Optimization, iterator with remove method.
            bool succ = acc.remove(vlist.first);
            assert(succ); // There is other cleaner here
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
    while (!index_updates.empty()) {
        auto iu = index_updates.back();

        if (iu.tag == IndexUpdate::EDGE) {
            auto e = iu.e;

            // TODO: This could be done in batch
            // NOTE: This assumes that type index is created with the database.
            TRY(e.record->data.edge_type->index().insert(
                EdgeTypeIndexRecord(std::nullptr_t(), e.record, e.vlist)));

            TRY(db.indexes().update_property_indexes<TypeGroupEdge>(e, trans));

        } else {
            auto v = iu.v;

            for (auto l : v.record->data.labels()) {
                // TODO: This could be done in batch
                // NOTE: This assumes that label index is created with the
                // database.
                TRY(l.get().index().insert(
                    LabelIndexRecord(std::nullptr_t(), v.record, v.vlist)));
            }

            TRY(db.indexes().update_property_indexes<TypeGroupVertex>(v,
                                                                      trans));
        }

        index_updates.pop_back();
    }
    return true;
}
