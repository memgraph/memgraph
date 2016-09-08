#pragma once

#include "database/db.hpp"

// Operation on indexes in the Db
class Indexes
{
public:
    Indexes(Db &d) : db(d) {}

    // Calls F over all vertex indexes in the database which are readable.
    template <class F>
    void vertex_indexes(F &&f)
    {
        for (auto &l : db.graph.label_store.access()) {
            f(l.second.get()->index());
        }

        for_all_property_indexes_read(
            db.graph.vertices.property_family_access(), f);
    }

    // Calls F over all edge indexes in the database which are readable.
    template <class F>
    void edge_indexes(F &&f)
    {
        for (auto &l : db.graph.edge_type_store.access()) {
            f(l.second.get()->index());
        }

        for_all_property_indexes_read(db.graph.edges.property_family_access(),
                                      f);
    }

    // Updates property indexes for given TypeGroup TG and IU index update
    template <class TG, class IU>
    bool update_property_indexes(IU &iu, const tx::Transaction &t)
    {
        for (auto kp : iu.record->data.props) {

            // FamilyProperty index
            auto opi = kp.key.get_family().index.get_write(t);
            if (opi.is_present()) {
                if (!opi.get()->insert(IndexRecord<TG, std::nullptr_t>(
                        std::nullptr_t(), iu.record, iu.vlist))) {
                    return false;
                }
            }

            // TODO: other properti indexes
        }

        return true;
    }

private:
    // Calls F for all * property indexes which are readable.
    template <class A, class F>
    void for_all_property_indexes_read(A &&acc, F &f)
    {
        for (auto &family : acc) {
            auto oi = family.second->index.get_read();
            if (oi.is_present()) {
                f(*oi.get());
            }
        }

        // TODO: Code for reaching other property indexes which are not yet
        // coded into the database.
    }

    Db &db;
};
