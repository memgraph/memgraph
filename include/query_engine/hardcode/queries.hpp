#pragma once

#include <iostream>
#include <map>

#include "barrier/barrier.hpp"

using namespace std;

namespace barrier
{
auto load_queries(Db &db)
{
    std::map<uint64_t, std::function<bool(const properties_t &)>> queries;

    // CREATE (n {prop: 0}) RETURN n)
    auto create_node = [&db](const properties_t &args) {
        DbAccessor t(db);
        auto prop_key = t.vertex_property_key("prop", args[0]->flags);

        auto vertex_accessor = t.vertex_insert();
        vertex_accessor.set(prop_key, args[0]);
        return t.commit();
    };
    queries[11597417457737499503u] = create_node;

    auto create_labeled_and_named_node = [&db](const properties_t &args) {
        DbAccessor t(db);
        auto prop_key = t.vertex_property_key("name", args[0]->flags);
        auto &label = t.label_find_or_create("LABEL");

        auto vertex_accessor = t.vertex_insert();
        vertex_accessor.set(prop_key, args[0]);
        vertex_accessor.add_label(label);
        // cout_properties(vertex_accessor.properties());
        return t.commit();
    };

    auto create_account = [&db](const properties_t &args) {
        DbAccessor t(db);
        auto prop_id = t.vertex_property_key("id", args[0]->flags);
        auto prop_name = t.vertex_property_key("name", args[1]->flags);
        auto prop_country = t.vertex_property_key("country", args[2]->flags);
        auto prop_created = t.vertex_property_key("created_at", args[3]->flags);
        auto &label = t.label_find_or_create("ACCOUNT");

        auto vertex_accessor = t.vertex_insert();
        vertex_accessor.set(prop_id, args[0]);
        vertex_accessor.set(prop_name, args[1]);
        vertex_accessor.set(prop_country, args[2]);
        vertex_accessor.set(prop_created, args[3]);
        vertex_accessor.add_label(label);
        // cout_properties(vertex_accessor.properties());
        return t.commit();
    };

    auto find_node_by_internal_id = [&db](const properties_t &args) {
        DbAccessor t(db);
        auto maybe_va = t.vertex_find(Id(args[0]->as<Int32>().value));
        if (!option_fill(maybe_va)) {
            cout << "vertex doesn't exist" << endl;
            t.commit();
            return false;
        }
        auto vertex_accessor = maybe_va.get();
        // cout_properties(vertex_accessor.properties());
        cout << "LABELS:" << endl;
        for (auto label_ref : vertex_accessor.labels()) {
            // cout << label_ref.get() << endl;
        }
        return t.commit();
    };

    auto create_edge = [&db](const properties_t &args) {
        DbAccessor t(db);
        auto &edge_type = t.type_find_or_create("IS");

        auto v1 = t.vertex_find(args[0]->as<Int32>().value);
        if (!option_fill(v1)) return t.commit(), false;

        auto v2 = t.vertex_find(args[1]->as<Int32>().value);
        if (!option_fill(v2)) return t.commit(), false;

        auto edge_accessor = t.edge_insert(v1.get(), v2.get());

        edge_accessor.edge_type(edge_type);

        bool ret = t.commit();

        // cout << edge_accessor.edge_type() << endl;

        // cout_properties(edge_accessor.properties());

        return ret;
    };

    auto find_edge_by_internal_id = [&db](const properties_t &args) {
        DbAccessor t(db);
        auto maybe_ea = t.edge_find(args[0]->as<Int32>().value);
        if (!option_fill(maybe_ea)) return t.commit(), false;
        auto edge_accessor = maybe_ea.get();

        // print edge type and properties
        // cout << "EDGE_TYPE: " << edge_accessor.edge_type() << endl;

        auto from = edge_accessor.from();
        if (!from.fill()) return t.commit(), false;

        cout << "FROM:" << endl;
        // cout_properties(from->data.props);

        auto to = edge_accessor.to();
        if (!to.fill()) return t.commit(), false;

        cout << "TO:" << endl;
        // cout_properties(to->data.props);

        return t.commit();
    };

    auto update_node = [&db](const properties_t &args) {
        DbAccessor t(db);
        auto prop_name = t.vertex_property_key("name", args[1]->flags);

        auto maybe_v = t.vertex_find(args[0]->as<Int32>().value);
        if (!option_fill(maybe_v)) return t.commit(), false;
        auto v = maybe_v.get();

        v.set(prop_name, args[1]);
        // cout_properties(v.properties());

        return t.commit();
    };

    // MATCH (n1), (n2) WHERE ID(n1)=0 AND ID(n2)=1 CREATE (n1)<-[r:IS {age: 25,
    // weight: 70}]-(n2) RETURN r
    auto create_edge_v2 = [&db](const properties_t &args) {
        DbAccessor t(db);

        auto prop_age = t.edge_property_key("age", args[2]->flags);
        auto prop_weight = t.edge_property_key("weight", args[3]->flags);

        auto n1 = t.vertex_find(args[0]->as<Int64>().value);
        if (!option_fill(n1)) return t.commit(), false;
        auto n2 = t.vertex_find(args[1]->as<Int64>().value);
        if (!option_fill(n2)) return t.commit(), false;
        auto r = t.edge_insert(n2.get(), n1.get());
        r.set(prop_age, args[2]);
        r.set(prop_weight, args[3]);
        auto &IS = t.type_find_or_create("IS");
        r.edge_type(IS);

        return t.commit();
    };

    // MATCH (n) RETURN n
    auto match_all_nodes = [&db](const properties_t &args) {
        DbAccessor t(db);

        t.vertex_access().fill().for_all(
            [&](auto vertex) { cout << vertex.id() << endl; });

        return t.commit();
    };

    // MATCH (n:LABEL) RETURN n
    auto match_by_label = [&db](const properties_t &args) {
        DbAccessor t(db);

        auto &label = t.label_find_or_create("LABEL");
        auto prop_key = t.vertex_property_key("name", Flags::String);

        cout << "VERTICES" << endl;
        label.index().for_range(t).for_all(
            [&](auto a) { cout << a.at(prop_key) << endl; });

        return t.commit();
    };

    // MATCH (n) DELETE n
    auto match_all_delete = [&db](const properties_t &args) {
        DbAccessor t(db);

        t.vertex_access().fill().isolated().for_all(
            [&](auto a) { a.remove(); });

        return t.commit();
    };

    // MATCH (n:LABEL) DELETE n
    auto match_label_delete = [&db](const properties_t &args) {
        DbAccessor t(db);

        auto &label = t.label_find_or_create("LABEL");

        label.index().for_range(t).isolated().for_all(
            [&](auto a) { a.remove(); });

        return t.commit();
    };

    // MATCH (n) WHERE ID(n) = id DELETE n
    auto match_id_delete = [&db](const properties_t &args) {
        DbAccessor t(db);

        auto ov = t.vertex_find(args[0]->as<Int64>().value);
        if (!option_fill(ov)) return t.commit(), false;
        auto v = ov.take();
        if (!v.isolated()) return t.commit(), false;
        v.remove();

        return t.commit();
    };

    // MATCH ()-[r]-() WHERE ID(r) = id DELETE r
    auto match_edge_id_delete = [&db](const properties_t &args) {
        DbAccessor t(db);

        auto ov = t.edge_find(args[0]->as<Int64>().value);
        if (!option_fill(ov)) return t.commit(), false;

        auto v = ov.take();
        v.remove();

        return t.commit();
    };

    // MATCH ()-[r]-() DELETE r
    auto match_edge_all_delete = [&db](const properties_t &args) {
        DbAccessor t(db);

        t.edge_access().fill().for_all([&](auto a) { a.remove(); });

        return t.commit();
    };

    // MATCH ()-[r:TYPE]-() DELETE r
    auto match_edge_type_delete = [&db](const properties_t &args) {
        DbAccessor t(db);

        auto &type = t.type_find_or_create("TYPE");

        type.index().for_range(t).for_all([&](auto a) { a.remove(); });

        return t.commit();
    };

    // MATCH (n)-[:TYPE]->(m) WHERE ID(n) = id RETURN m
    auto match_id_type_return = [&db](const properties_t &args) {
        DbAccessor t(db);

        auto &type = t.type_find_or_create("TYPE");

        auto ov = t.vertex_find(args[0]->as<Int64>().value);
        if (!option_fill(ov)) return t.commit(), false;
        auto v = ov.take();

        auto results = v.out().fill().type(type).to();

        // Example of Print resoult.
        // results.for_all([&](auto va) {
        //     va is VertexAccessor
        //     PRINT
        // });

        return t.commit();
    };

    // MATCH (n)-[:TYPE]->(m) WHERE n.name = "kruno" RETURN m
    auto match_name_type_return = [&db](const properties_t &args) {
        DbAccessor t(db);

        auto &type = t.type_find_or_create("TYPE");
        auto prop_name = t.vertex_property_key("name", args[0]->flags);

        Option<const EdgeAccessor> r;

        auto it_type = type.index()
                           .for_range(t)
                           .clone_to(r) // Savepoint
                           .from()      // Backtracing
                           .has_property(prop_name, *args[0].get())
                           .replace(r); // Load savepoint

        auto it_vertex = t.vertex_access()
                             .fill()
                             .has_property(prop_name, *args[0].get())
                             .out()
                             .type(type);

        if (it_type.count() > it_vertex.count()) {
            // Going through vertices wiil probably be faster
            it_vertex.to().for_all([&](auto m) {
                // PRINT m
            });

        } else {
            // Going through edges wiil probably be faster
            it_type.to().for_all([&](auto m) {
                // PRINT m
            });
        }

        return t.commit();
    };

    // MATCH (n)-[:TYPE]->(m) WHERE n.name = "kruno" RETURN n,m
    auto match_name_type_return_cross = [&db](const properties_t &args) {
        DbAccessor t(db);

        auto &type = t.type_find_or_create("TYPE");
        auto prop_name = t.vertex_property_key("name", args[0]->flags);

        Option<const VertexAccessor> n;
        Option<const EdgeAccessor> r;

        auto it_type = type.index()
                           .for_range(t)
                           .clone_to(r) // Savepoint
                           .from()      // Backtracing
                           .has_property(prop_name, *args[0].get())
                           .clone_to(n) // Savepoint
                           .replace(r); // Load savepoint

        auto it_vertex = t.vertex_access()
                             .fill()
                             .has_property(prop_name, *args[0].get())
                             .clone_to(n) // Savepoint
                             .out()
                             .type(type);

        if (it_type.count() > it_vertex.count()) {
            // Going through vertices wiil probably be faster
            it_vertex.to().for_all([&](auto m) {
                // PRINT n,m
            });

        } else {
            // Going through edges wiil probably be faster
            it_type.to().for_all([&](auto m) {
                // PRINT n,m
            });
        }

        return t.commit();
    };

    // MATCH (n:LABEL)-[:TYPE]->(m) RETURN n
    auto match_label_type_return = [&db](const properties_t &args) {
        DbAccessor t(db);

        try {
            auto &type = t.type_find_or_create("TYPE");
            auto &label = t.label_find_or_create("LABEL");

            Option<const VertexAccessor> bt;

            auto it_type = type.index().for_range(t).from().label(label);

            auto it_vertex = t.vertex_access()
                                 .fill()
                                 .label(label)
                                 .clone_to(bt) // Savepoint
                                 .out()
                                 .type(type)
                                 .replace(bt); // Load savepoint

            if (it_type.count() > it_vertex.count()) {
                // Going through vertices wiil probably be faster
                it_vertex.for_all([&](auto n) {
                    // PRINT n
                });

            } else {
                // Going through edges wiil probably be faster
                it_type.for_all([&](auto n) {
                    // PRINT n
                });
            }
            return t.commit();
        } catch (...) {
            // Catch all exceptions
            // Print something to logger
            t.abort();
            return false;
        }

    };

    // Blueprint:
    // auto  = [&db](const properties_t &args) {
    //     DbAccessor t(db);
    //
    //
    //     return t.commit();
    // };

    queries[15284086425088081497u] = match_all_nodes;
    queries[4857652843629217005u] = match_by_label;
    queries[15648836733456301916u] = create_edge_v2;
    queries[10597108978382323595u] = create_account;
    queries[5397556489557792025u] = create_labeled_and_named_node;
    queries[7939106225150551899u] = create_edge;
    queries[6579425155585886196u] = create_edge;
    queries[11198568396549106428u] = find_node_by_internal_id;
    queries[8320600413058284114u] = find_edge_by_internal_id;
    queries[6813335159006269041u] = update_node;
    queries[10506105811763742758u] = match_all_delete;
    queries[13742779491897528506u] = match_label_delete;
    queries[11349462498691305864u] = match_id_delete;
    queries[6963549500479100885u] = match_edge_id_delete;
    queries[14897166600223619735u] = match_edge_all_delete;
    queries[16888549834923624215u] = match_edge_type_delete;
    queries[11675960684124428508u] = match_id_type_return;
    queries[1554436524951961398u] = match_name_type_return;
    queries[8537338362659537296u] = match_name_type_return_cross;
    queries[8918221081398321263u] = match_label_type_return;

    return queries;
}
}
