#include "storage/indexes/indexes.hpp"

#include "database/db_accessor.hpp"

bool Indexes::add_index(IndexDefinition id)
{
    auto logger = logging::log->logger("Add index");
    logger.info("Starting");

    // Closure which needs to be runned to finish creating index.
    std::function<bool(DbTransaction &)> finish = [](auto &t) { return false; };

    // Creates transaction and during it's creation adds index into it's
    // place. Also creates finish closure which will add necessary elements
    // into index.
    DbTransaction t(db, db.tx_engine.begin([&](auto &t) mutable {
        size_t code = id.loc.location_code();

        switch (code) {

        // Index over nothing
        case 0: // Illegal location
            break;

        // Index over property
        case 1: {
            switch (id.loc.side) {
            case EdgeSide: {
                auto &holder = db.graph.edges
                                   .property_family_find_or_create(
                                       id.loc.property_name.get())
                                   .index;
                if (holder.set_index(
                        create_index<TypeGroupEdge, std::nullptr_t>(id, t))) {
                    // Set closure which will fill index.
                    finish = [&](auto &t) mutable {
                        DbAccessor acc(t.db, t.trans);
                        auto &key = acc.edge_property_family_get(
                            id.loc.property_name.get());

                        return fill_index(
                            t, holder,
                            acc.edge_access().fill().has_property(key).map(
                                [&](auto ra) {
                                    return std::make_pair(std::move(ra),
                                                          std::nullptr_t());
                                }));
                    };
                }
                break;
            }
            case VertexSide: { // TODO: Duplicate code as above, only difference
                               // is vertices/vertex
                auto &holder = db.graph.vertices
                                   .property_family_find_or_create(
                                       id.loc.property_name.get())
                                   .index;
                if (holder.set_index(
                        create_index<TypeGroupVertex, std::nullptr_t>(id, t))) {
                    // Set closure which will fill index.
                    finish = [&](auto &t) mutable {
                        DbAccessor acc(t.db, t.trans);
                        auto &key = acc.vertex_property_family_get(
                            id.loc.property_name.get());

                        return fill_index(
                            t, holder,
                            acc.vertex_access().fill().has_property(key).map(
                                [&](auto ra) {
                                    return std::make_pair(std::move(ra),
                                                          std::nullptr_t());
                                }));
                    };
                }
                break;
            }
            default:
                logger.error("Unkown side: " + std::to_string(id.loc.side));
            };
            break;
        }

        // Index over label
        case 2: { // Always added
            finish = [](auto &t) { return true; };
            return;
        }

        // Index over property and label
        case 3: { // Not yet implemented
            logger.error("Remove of index over label and "
                         "property isn't yet implemented");
            break;
        }

        // Index over edge_type
        case 4: { // Always added
            finish = [](auto &t) { return true; };
            break;
        }

        // Index over property and edge_type
        case 5: { // Not yet implemented
            logger.error("Remove of index over edge_type and "
                         "property isn't yet implemented");
            break;
        }

        // Index over edge_type and label
        case 6: { // Not yet implemented
            logger.error("Remove of index over edge_type and "
                         "label isn't yet implemented");
            break;
        }

        // Index over property, edge_type and label
        case 7: { // Not yet implemented
            logger.error("Remove of index over label, edge_type "
                         "and property isn't yet implemented");
            break;
        }

        default:
            logger.error("Unkown index location code: " + std::to_string(code));
        }
    }));

    if (finish(t)) {
        t.trans.commit();

        logger.info("Success");
        return true;

    } else {
        t.trans.abort();

        logger.info("Failed");
        return false;
    }
}
