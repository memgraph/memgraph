#include "database/db_transaction.hpp"

#include "database/db.hpp"
#include "storage/edge.hpp"
#include "storage/edge_type/edge_type.hpp"
#include "storage/label/label.hpp"
#include "storage/vertex.hpp"

#define TRY(x)                                                                 \
    if (!x) {                                                                  \
        return false;                                                          \
    }

DbTransaction::DbTransaction(Db &db) : db(db), trans(db.tx_engine.begin()) {}

// Cleaning for indexes in labels and edge_type
template <class A>
void clean_indexes(A &&acc, Id oldest_active)
{
    for (auto &l : acc) {
        l.second.get()->index().clean(oldest_active);
    }
}

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

// Cleaning for indexes in properties.
template <class A>
void clean_property_indexes(A &&acc, Id oldest_active)
{
    for (auto &family : acc) {
        auto oi = family.second->index.get_read();
        if (oi.is_present()) {
            oi.get()->clean(oldest_active);
        }
    }

    // TODO: Code for cleaning other indexes which are not yet coded into
    // the database.
}

// Cleans edge part of database. Should be called by one cleaner thread at
// one time.
void DbTransaction::clean_edge_section()
{
    Id oldest_active = trans.oldest_active();

    // Clean edge_type index
    clean_indexes(db.graph.edge_type_store.access(), oldest_active);

    // Clean family_type_s edge index
    clean_property_indexes(db.graph.edges.property_family_access(),
                           oldest_active);

    // Clean Edge list
    clean_version_lists(db.graph.edges.access(), oldest_active);
}

// Cleans vertex part of database. Should be called by one cleaner thread at
// one time.
void DbTransaction::clean_vertex_section()
{
    Id oldest_active = trans.oldest_active();

    // Clean label index
    clean_indexes(db.graph.label_store.access(), oldest_active);

    // Clean family_type_s vertex index
    clean_property_indexes(db.graph.vertices.property_family_access(),
                           oldest_active);

    // Clean vertex list
    clean_version_lists(db.graph.vertices.access(), oldest_active);
}

template <class TG, class IU>
bool update_property_indexes(IU &iu, const tx::Transaction &t)
{
    for (auto kp : iu.record->data.props) {

        // FamilyProperty index
        auto opi = kp.first.get_family().index.get_write(t);
        if (opi.is_present()) {
            TRY(opi.get()->insert(IndexRecord<TG, std::nullptr_t>(
                std::nullptr_t(), iu.record, iu.vlist)));
        }

        // TODO: other properti indexes
    }

    return true;
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

            TRY(update_property_indexes<TypeGroupEdge>(e, trans));

        } else {
            auto v = iu.v;

            for (auto l : v.record->data.labels()) {
                // TODO: This could be done in batch
                // NOTE: This assumes that label index is created with the
                // database.
                TRY(l.get().index().insert(
                    LabelIndexRecord(std::nullptr_t(), v.record, v.vlist)));
            }

            TRY(update_property_indexes<TypeGroupVertex>(v, trans));
        }

        index_updates.pop_back();
    }
    return true;
}
