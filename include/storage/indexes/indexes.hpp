#pragma once

#include "database/db.hpp"
#include "query_engine/exceptions/exceptions.hpp"
#include "storage/garbage/garbage.hpp"
#include "storage/graph.hpp"
#include "storage/indexes/impl/nonunique_unordered_index.hpp"
#include "storage/indexes/impl/unique_ordered_index.hpp"
#include "storage/indexes/index_definition.hpp"
#include "transactions/engine.hpp"

// Operation on indexes in the  This should be the only place which knows how
// to get all indexes in database.
// TODO: This class should be updated accordingly when adding new place for
// index.
class Indexes
{
public:
    Indexes(Db &d) : db(d) {}

    // Adds index defined in given definition. Returns true if successfull.
    bool add_index(IndexDefinition id);
    //
    // // Returns index from location.
    // template <class TG, class K>
    // Option<IndexHolder<TG, K>> get_index(IndexLocation loc)
    // {
    //     size_t code = loc.location_code();
    //
    //     switch (code) {
    //     case 0: // Illegal location
    //         return Option<IndexHolder<TG, K>>();
    //
    //     case 1:
    //         switch (loc.side) {
    //         case EdgeSide: {
    //             return make_option(
    //                 db.graph.edges
    //                     .property_family_find_or_create(loc.property_name.get())
    //                     .index);
    //         }
    //         case VertexSide: {
    //             return make_option(
    //                 db.graph.vertices
    //                     .property_family_find_or_create(loc.property_name.get())
    //                     .index);
    //         }
    //         default:
    //             throw new NonExhaustiveSwitch("Unkown side: " +
    //                                           std::to_string(loc.side));
    //         };
    //
    //     case 2: // Can't be removed
    //         return Option<IndexHolder<TG, K>>();
    //
    //     case 3: // Not yet implemented
    //         throw new NotYetImplemented("Getting index over label and "
    //                                     "property isn't yet implemented");
    //     case 4: // Can't be removed
    //         return Option<IndexHolder<TG, K>>();
    //
    //     case 5: // Not yet implemented
    //         throw new NotYetImplemented("Getting index over edge_type and "
    //                                     "property isn't yet implemented");
    //     case 6: // Not yet implemented
    //         throw new NotYetImplemented("Getting index over edge_type and "
    //                                     "label isn't yet implemented");
    //     case 7: // Not yet implemented
    //         throw new NotYetImplemented("Getting index over label, edge_type
    //         "
    //                                     "and property isn't yet
    //                                     implemented");
    //     default:
    //         throw new NonExhaustiveSwitch("Unkown index location code: " +
    //                                       std::to_string(code));
    //     }
    // }

    // Removes index from given location. Returns true if successfull or if no
    // index was present. False if index location is illegal.
    bool remove_index(IndexLocation loc)
    {
        size_t code = loc.location_code();

        switch (code) {
        case 0: // Illegal location
            return false;

        case 1:
            switch (loc.side) {
            case EdgeSide: {
                return remove_index(
                    db.graph.edges
                        .property_family_find_or_create(loc.property_name.get())
                        .index);
            }
            case VertexSide: {
                return remove_index(
                    db.graph.vertices
                        .property_family_find_or_create(loc.property_name.get())
                        .index);
            }
            default:
                throw new NonExhaustiveSwitch("Unkown side: " +
                                              std::to_string(loc.side));
            };

        case 2: // Can't be removed
            return false;

        case 3: // Not yet implemented
            throw new NotYetImplemented("Remove of index over label and "
                                        "property isn't yet implemented");
        case 4: // Can't be removed
            return false;

        case 5: // Not yet implemented
            throw new NotYetImplemented("Remove of index over edge_type and "
                                        "property isn't yet implemented");
        case 6: // Not yet implemented
            throw new NotYetImplemented("Remove of index over edge_type and "
                                        "label isn't yet implemented");
        case 7: // Not yet implemented
            throw new NotYetImplemented("Remove of index over label, edge_type "
                                        "and property isn't yet implemented");
        default:
            throw new NonExhaustiveSwitch("Unkown index location code: " +
                                          std::to_string(code));
        }
    }

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

    // Updates property indexes for given TypeGroup TG and IU index_update
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

            // TODO: other property indexes
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

    // Creates type of index specified ind index definition.
    // TG - type group vertex/edge
    // K - key of index
    template <class TG, class K>
    std::unique_ptr<IndexBase<TG, K>> create_index(IndexDefinition id,
                                                   tx::Transaction const &t)
    {
        // Determine which index is needed
        if (id.type.unique) {
            switch (id.type.order) {
            case None: {
                // TODO: Implement this version of index.
                throw NotYetImplemented(
                    "Missing implementation for Unique Unordered Index");
            }
            case Ascending:
            case Descending: {
                return std::make_unique<UniqueOrderedIndex<TG, K>>(
                    id.loc.clone(), id.type.order, t);
            }
            default:
                throw new NonExhaustiveSwitch("Unknown order: " +
                                              std::to_string(id.type.order));
            };

        } else {
            switch (id.type.order) {
            case None: {
                return std::make_unique<NonUniqueUnorderedIndex<TG, K>>(
                    id.loc.clone(), t);
            }
            case Ascending:
            case Descending: {
                // TODO: Implement this version of index.
                throw NotYetImplemented(
                    "Missing implementation for Nonunique Ordered Index");
            }
            default:
                throw new NonExhaustiveSwitch("Unknown order: " +
                                              std::to_string(id.type.order));
            };
        }
    }

    // Fills index with returned elements. Return true if successfully filled.
    // Iterator must return std::pair<TG::accessor_t, K>
    template <class TG, class K, class I>
    bool fill_index(DbTransaction &t, IndexHolder<TG, K> &holder, I &&iter)
    {
        // Wait for all other active transactions to finish so that this
        // transaction can see there changes so that they can be added into
        // index.
        t.trans.wait_for_active();

        auto oindex = holder.get_write(t.trans);
        if (oindex.is_present()) {
            auto index = oindex.get();

            // Iterate over all elements and add them into index. Fail if
            // some insert failed.
            bool res = iter.all([&](auto elem) {
                if (!index->insert(elem.first.create_index_record(
                        std::move(elem.second)))) {

                    // Index is probably unique.
                    auto owned_maybe = holder.remove_index(index);
                    if (owned_maybe.is_present()) {
                        db.garbage.dispose(db.tx_engine.snapshot(),
                                           owned_maybe.get().release());
                    }

                    return false;
                }
                return true;
            });
            if (res) {
                index->activate();
                return true;
            }
        }

        return false;
    }

    // Removes index from index holder
    template <class TG, class K>
    bool remove_index(IndexHolder<TG, K> &ih)
    {
        auto owned_maybe = ih.remove_index();
        if (owned_maybe.is_present()) {
            db.garbage.dispose(db.tx_engine.snapshot(),
                               owned_maybe.get().release());
            return true;
        }

        return false;
    }

    Db &db;
};
