#pragma once

#include "includes.hpp"

namespace hardcode
{

// TODO: decouple hashes from the code because hashes could and should be
// tested separately

auto load_dressipi_functions(Db &db)
{
    query_functions_t functions;

    // Query: CREATE (g:garment {garment_id: 1234, garment_category_id: 1}) RETURN g
    // Hash: 18071907865596388702
    functions[18071907865596388702u] = [&db](properties_t &&args) {
        DbAccessor t(db);

        auto garment_id =
            t.vertex_property_key("garment_id", args[0].key.flags());
        auto garment_category_id =
            t.vertex_property_key("garment_category_id", args[1].key.flags());

        // vertex_accessor
        auto va = t.vertex_insert();
        va.set(garment_id, std::move(args[0]));
        va.set(garment_category_id, std::move(args[1]));

        auto &garment = t.label_find_or_create("garment");
        va.add_label(garment);

        // stream.write_field("g");
        // stream.write_vertex_record(va);
        // stream.write_meta("w");

        print_entities(t);

        return t.commit();
    };

    // Query: CREATE (p:profile {profile_id: 111, partner_id: 55}) RETURN p
    // Hash: 17158428452166262783
    functions[17158428452166262783u] = [&db](properties_t &&args) {
        DbAccessor t(db);

        auto profile_id =
            t.vertex_property_key("profile_id", args[0].key.flags());
        auto partner_id =
            t.vertex_property_key("partner_id", args[1].key.flags());

        auto va = t.vertex_insert();
        va.set(profile_id, std::move(args[0]));
        va.set(partner_id, std::move(args[1]));

        auto &profile = t.label_find_or_create("profile");
        va.add_label(profile);

        // stream.write_field("p");
        // stream.write_vertex_record(va);
        // stream.write_meta("w");

        print_entities(t);

        return t.commit();
    };

    // Query: MATCH (g:garment {garment_id: 1234}) SET g:FF RETURN labels(g)
    // Hash: 11123780635391515946
    functions[11123780635391515946u] = [&db](properties_t &&args) {
        DbAccessor t(db);

        indices_t indices = {{"garment_id", 0}};
        auto properties   = query_properties(indices, args);

        auto &label       = t.label_find_or_create("garment");
        
        // stream.write_field("labels(g)");

        label.index()
            .for_range(t)
            .properties_filter(t, properties)
            .for_all([&](auto va) -> void {
                va.stream_repr(std::cout);
                auto &ff_label = t.label_find_or_create("FF");
                va.add_label(ff_label);
                auto &labels = va.labels();

                // stream.write_record();
                // stream.write_list_header(1);
                // stream.write_list_header(labels.size());

                for (auto &label : labels)
                {
                    // stream.write(label.get().str());
                    
                    utils::println("LABELS: ", label.get().str());
                }
                // stream.chunk();
            });
        // stream.write_meta(\"rw");

        print_entities(t);

        return t.commit();
    };

    // Query: MATCH (p:profile {profile_id: 111, partner_id:55})-[s:score]-(g:garment {garment_id: 1234}) SET s.score = 1550 RETURN s.score
    // Hash: 674581607834128909
    functions[674581607834128909u] = [&db](properties_t &&args) {
        DbAccessor t(db);

        auto &profile = t.label_find_or_create("profile");
        auto &score   = t.type_find_or_create("score");
        auto &garment = t.label_find_or_create("garment");

        indices_t profile_ind = {{"profile_id", 0}, {"partner_id", 1}};
        indices_t garment_ind = {{"garment_id", 2}};

        auto profile_prop = query_properties(profile_ind, args);
        auto garment_prop = query_properties(garment_ind, args);

        auto score_key = t.edge_property_key("score", args[3].key.flags());

        // TODO: decide path (which index is better)
        //       3 options p->s->g, g->s->p, g<-s->p
        //       NOTE! both direections have to be chacked
        //       because pattern is non directional
        //       OR
        //       even better, use index on label and property

        // just one option p->s->g!
        Option<const EdgeAccessor> e1;
        profile.index()
            .for_range(t)
            .properties_filter(t, profile_prop)
            .out()
            .type(score)
            .clone_to(e1)
            .to()
            .label(garment)
            .properties_filter(t, garment_prop)
            .for_all([&](auto va) -> void {
                auto ea = e1.get().update();
                ea.set(score_key, std::move(args[3]));
            });

        Option<const EdgeAccessor> e2;
        profile.index()
            .for_range(t)
            .properties_filter(t, profile_prop)
            .in()
            .type(score)
            .clone_to(e1)
            .from()
            .label(garment)
            .properties_filter(t, garment_prop)
            .for_all([&](auto va) -> void {
                auto ea = e2.get().update();
                ea.set(score_key, std::move(args[3]));
            });

        // stream.write_field("r.score");
        // write_record();
        // write_list_header(1);
        // write(Float(args[3]));
        // chunk();

        print_entities(t);

        return t.commit();
    };

    // Query: MATCH (g:garment {garment_id: 3456}) SET g.reveals = 50 RETURN g
    // Hash: 2839969099736071844
    functions[2839969099736071844u] = [&db](properties_t &&args) {
        DbAccessor t(db);

        auto reveals_key =
            t.vertex_property_key("reveals", args[1].key.flags());

        indices_t indices = {{"garment_id", 0}};
        auto properties   = query_properties(indices, args);

        auto &label       = t.label_find_or_create("garment");

        // stream.write_field("g");

        label.index()
            .for_range(t)
            .properties_filter(t, properties)
            .fill()
            .for_all([&](auto va) {
                va.set(reveals_key, args[1]);

                // stream.write_vertex_record(va); 
            });

        // stream.write_meta("w");

        print_entities(t);

        return t.commit();
    };

    // Query: MERGE (g1:garment {garment_id:1234})-[r:default_outfit]-(g2:garment {garment_id: 2345}) RETURN r
    // Hash: 3782642357973971504
    functions[3782642357973971504u] = [&db](properties_t &&args) {
        DbAccessor t(db);

        // TODO: support for index on label and property

        // prepare iterator for g1
        indices_t indices_1 = {{"garment_id", 0}};
        auto properties_1   = query_properties(indices_1, args);
        auto &label_1       = t.label_find_or_create("garment");

        auto it_vertex_1 =
            label_1.index().for_range(t).properties_filter(t, properties_1);

        // prepare iterator for g1
        indices_t indices_2 = {{"garment_id", 1}};
        auto properties_2   = query_properties(indices_2, args);
        auto &label_2       = t.label_find_or_create("garment");

        auto it_vertex_2 =
            label_2.index().for_range(t).properties_filter(t, properties_2);

        auto &edge_type = t.type_find_or_create("default_outfit");

        // TODO: create g1 and g2 if don't exist

        // TODO: figure out something better

        // stream.write_field("r");

        it_vertex_1.fill().for_all([&](auto va1) -> void {
            it_vertex_2.fill().for_all([&](auto va2) -> void {
                auto edge_accessor = t.edge_insert(va1, va2);
                edge_accessor.edge_type(edge_type);

                // stream.write_edge_record(edge_accessor);
            });
        });

        // stream.write_meta("w");

        print_entities(t);

        return t.commit();
    };

    // Query: MERGE (p:profile {profile_id: 111, partner_id: 55})-[s:score]-(g.garment {garment_id: 1234}) SET s.score=1500 RETURN s
    // Hash: 7871009397157280694
    functions[7871009397157280694u] = [&db](properties_t &&args) {
        DbAccessor t(db);

        auto &profile = t.label_find_or_create("profile");
        auto &score   = t.type_find_or_create("score");
        auto &garment = t.label_find_or_create("garment");

        indices_t profile_ind = {{"profile_id", 0}, {"partner_id", 1}};
        indices_t garment_ind = {{"garment_id", 2}};

        auto profile_prop = query_properties(profile_ind, args);
        auto garment_prop = query_properties(garment_ind, args);

        auto score_key = t.edge_property_key("score", args[3].key.flags());
        
        // stream.write_field("s");

        // TODO: implement
        bool exists = false;
        Option<const EdgeAccessor> e1;
        profile.index()
            .for_range(t)
            .properties_filter(t, profile_prop)
            .out()
            .type(score)
            .clone_to(e1)
            .to()
            .label(garment)
            .properties_filter(t, garment_prop)
            .for_all([&](auto va) -> void {
                exists = true;
                auto ea = e1.get().update();
                ea.set(score_key, args[3]);
                // stream.write_edge_record(ea);
            });

        Option<const EdgeAccessor> e2;
        profile.index()
            .for_range(t)
            .properties_filter(t, profile_prop)
            .in()
            .type(score)
            .clone_to(e1)
            .from()
            .label(garment)
            .properties_filter(t, garment_prop)
            .for_all([&](auto va) -> void {
                exists = true;
                auto ea = e2.get().update();
                ea.set(score_key, args[3]);
                // stream.write_edge_record(ea);
            });

        if (!exists) {
            auto it_vertex_garment =
                garment.index().for_range(t).properties_filter(t, garment_prop);
            auto it_vertex_profile =
                profile.index().for_range(t).properties_filter(t, profile_prop);

            it_vertex_profile.fill().for_all([&](auto va1) -> void {
                it_vertex_garment.fill().for_all([&](auto va2) -> void {
                    auto ea = t.edge_insert(va1, va2);
                    ea.edge_type(score);
                    ea.set(score_key, args[3]);
                    // stream.write_edge_record(ea);
                });
            });
        }

        // stream.write_field("w");

        print_entities(t);

        return t.commit();
    };

    // Query: MATCH (p:profile {profile_id: 111, partner_id:55})-[s:score]-(g.garment {garment_id: 1234}) DELETE s
    // Hash: 9459600951073026137
    functions[9459600951073026137u] = [&db](properties_t &&args) {
        DbAccessor t(db);

        auto &profile = t.label_find_or_create("profile");
        auto &score   = t.type_find_or_create("score");
        auto &garment = t.label_find_or_create("garment");

        indices_t profile_ind = {{"profile_id", 0}, {"partner_id", 1}};
        indices_t garment_ind = {{"garment_id", 2}};

        auto profile_prop = query_properties(profile_ind, args);
        auto garment_prop = query_properties(garment_ind, args);

        auto score_key = t.edge_property_key("score", args[3].key.flags());

        // TODO: decide path (which index is better)
        //       3 options p->s->g, g->s->p, g<-s->p
        //       NOTE! both direections have to be chacked
        //       because pattern is non directional
        //       OR
        //       even better, use index on label and property

        // just one option p->s->g!
        Option<const EdgeAccessor> e1;
        profile.index()
            .for_range(t)
            .properties_filter(t, profile_prop)
            .out()
            .type(score)
            .clone_to(e1)
            .to()
            .label(garment)
            .properties_filter(t, garment_prop)
            .for_all([&](auto va) -> void {
                auto ea = e1.get().update();
                ea.remove();
            });

        Option<const EdgeAccessor> e2;
        profile.index()
            .for_range(t)
            .properties_filter(t, profile_prop)
            .in()
            .type(score)
            .clone_to(e1)
            .from()
            .label(garment)
            .properties_filter(t, garment_prop)
            .for_all([&](auto va) -> void {
                auto ea = e2.get().update();
                ea.remove();
            });

        // stream.write_empty_fields();
        // stream.write_meta("w");

        print_entities(t);

        return t.commit();
    };

    // Query: MATCH (p:garment {garment_id: 1}) DELETE g
    // Hash: 11538263096707897794
    functions[11538263096707897794u] = [&db](properties_t &&args) {
        DbAccessor t(db);

        indices_t indices = {{"garment_id", 0}};
        auto properties   = query_properties(indices, args);

        auto &label = t.label_find_or_create("garment");

        label.index()
            .for_range(t)
            .properties_filter(t, properties)
            .for_all([&](auto va) { va.remove(); });

        // stream.write_empty_fields();
        // stream.write_meta("w");

        print_entities(t);

        return t.commit();
    };

    // Query: MATCH (p:profile {profile_id: 1}) DELETE p
    // Hash: 6763665709953344106
    functions[6763665709953344106u] = [&db](properties_t &&args) {
        DbAccessor t(db);

        indices_t indices = {{"profile_id", 0}};
        auto properties   = query_properties(indices, args);

        auto &label = t.label_find_or_create("profile");

        label.index()
            .for_range(t)
            .properties_filter(t, properties)
            .for_all([&](auto va) { va.remove(); });

        // stream.write_empty_fields();
        // stream.write_meta("w");

        print_entities(t);

        return t.commit();
    };

    // Query: MATCH (n) RETURN n
    // Hash: 5949923385370229113
    functions[5949923385370229113u] = [&db](properties_t &&args) {
        DbAccessor t(db);

        // stream.write_field("n");

        t.vertex_access().fill().for_all([&](auto va) {
            // stream.write_vertex_record(va);

            va.stream_repr(std::cout);
            utils::println("");
        });

        // stream.write_meta("r");

        return t.commit();
    };

    // Query: MATCH (g:garment) RETURN g
    // Hash: 2645003052555435977
    functions[2645003052555435977u] = [&db](properties_t &&args) {
        DbAccessor t(db);

        auto &label = t.label_find_or_create("garment");

        // stream.write_field("p");
        label.index().for_range(t).for_all([&](auto va) -> void {
            // stream.write_vertex_record(va);

            va.stream_repr(std::cout);
            utils::println("");
        });

        // stream.write_meta("r");

        return t.commit();
    };

    // Query: MATCH (p:profile) RETURN p
    // Hash: 15599970964909894866
    functions[15599970964909894866u] = [&db](properties_t &&args) {
        DbAccessor t(db);

        auto &label = t.label_find_or_create("profile");

        // stream.write_field("p");

        label.index().for_range(t).for_all([&](auto va) {

            // stream.write_vertex_record(va);

            va.stream_repr(std::cout);
            utils::println("");
        });

        // stream.write_meta("r");

        print_entities(t);

        return t.commit();
    };

    // Query: MATCH (g:garment {garment_id: 1}) RETURN g
    // Hash: 7756609649964321221
    functions[7756609649964321221u] = [&db](properties_t &&args) {
        DbAccessor t(db);

        indices_t indices = {{"garment_id", 0}};
        auto properties   = query_properties(indices, args);

        auto &label = t.label_find_or_create("garment");

        // stream.write_field("g");

        label.index()
            .for_range(t)
            .properties_filter(t, properties)
            .for_all([&](auto va) -> void {
                // stream.write_vertex_record(va);

                va.stream_repr(std::cout); 
            });

        // stream.write_meta("w");

        utils::println("");
        print_entities(t);

        return t.commit();
    };

    // Query: MATCH (p:profile {partner_id: 1}) RETURN p
    // Hash: 17506488413143988006
    functions[17506488413143988006u] = [&db](properties_t &&args) {
        DbAccessor t(db);

        indices_t indices = {{"partner_id", 0}};
        auto properties   = query_properties(indices, args);

        auto &label = t.label_find_or_create("profile");

        // stream.write_field("p");

        label.index()
            .for_range(t)
            .properties_filter(t, properties)
            .for_all([&](auto va) {
                // stream.write_vertex_record(va);

                va.stream_repr(std::cout);
            });

        // stream.write_meta("r");
        
        utils::println("");
        print_entities(t);

        return t.commit();
    };

    // Query: MATCH (n) RETURN count(n)
    // Hash: 10510787599699014973
    functions[10510787599699014973u] = [&db](properties_t &&args) {
        DbAccessor t(db);

        size_t count = 0;
        t.vertex_access().fill().for_all([&](auto va) { ++count; });

        // stream.write_field("count(n)");
        // stream.write_count(count);
        // stream.write_meta("r");

        utils::println("COUNT: ", count);
        print_entities(t);

        return t.commit();
    };

    // Query: MATCH (g:garment) RETURN count(g)
    // Hash: 11458306387621940265
    functions[11458306387621940265u] = [&db](properties_t &&args) {
        DbAccessor t(db);

        auto &label = t.label_find_or_create("garment");

        size_t count = 0;
        label.index().for_range(t).for_all([&](auto va) { ++count; });

        // stream.write_field("count(g)");
        // stream.write_count(count);
        // stream.write_meta("r");

        utils::println("COUNT: ", count);
        print_entities(t);

        return t.commit();
    };

    // Query: MATCH (p:profile) RETURN count(p)
    // Hash: 7104933247859974916
    functions[7104933247859974916u] = [&db](properties_t &&args) {
        DbAccessor t(db);

        auto &label = t.label_find_or_create("profile");

        size_t count = 0;
        label.index().for_range(t).for_all([&](auto va) { ++count; });

        // stream.write_field("count(p)");
        // stream.write_count(count);
        // stream.write_meta("r");

        utils::println("COUNT: ", count);
        print_entities(t);

        return t.commit();
    };

    // Query: MATCH (n) DETACH DELETE n
    // Hash: 4798158026600988079
    functions[4798158026600988079u] = [&db](properties_t &&args) {
        DbAccessor t(db);

        t.edge_access().fill().for_all([&](auto e) { e.remove(); });
        t.vertex_access().fill().isolated().for_all(
            [&](auto a) { a.remove(); });

        // stream.write_empty_fields();
        // stream.write_meta("w");

        print_entities(t);

        return t.commit();
    };

    return functions;
}
}
