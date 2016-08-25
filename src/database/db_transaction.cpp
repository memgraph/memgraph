#include "database/db_transaction.hpp"

#include "storage/edge.hpp"
#include "storage/edge_type/edge_type.hpp"
#include "storage/label/label.hpp"
#include "storage/vertex.hpp"

#define TRY(x)                                                                 \
    if (!x) {                                                                  \
        return false;                                                          \
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
            TRY(e.record->data.edge_type->index->insert(
                EdgeTypeIndexRecord(std::nullptr_t(), e.record, e.vlist)));

            TRY(update_property_indexes<TypeGroupEdge>(e, trans));

        } else {
            auto v = iu.v;

            for (auto l : v.record->data.labels()) {
                // TODO: This could be done in batch
                // NOTE: This assumes that label index is created with the
                // database.
                TRY(l.get().index->insert(
                    LabelIndexRecord(std::nullptr_t(), v.record, v.vlist)));
            }

            TRY(update_property_indexes<TypeGroupVertex>(v, trans));
        }

        index_updates.pop_back();
    }
    return true;
}
