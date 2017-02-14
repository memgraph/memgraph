#include <iostream>
#include <string>

#include "query/util.hpp"
#include "query/plan_interface.hpp"
#include "storage/model/properties/all.hpp"
#include "storage/edge_x_vertex.hpp"
#include "using.hpp"

using std::cout;
using std::endl;

// Query: MERGE (p:profile {profile_id: 111, partner_id: 55})-[s:score]-(g.garment {garment_id: 1234}) SET s.score=1500 RETURN s

class CPUPlan : public PlanInterface<Stream>
{
public:

    bool run(Db &db, const PlanArgsT &args, Stream &stream) override
    {
        DbAccessor t(db);

        auto &profile = t.label_find_or_create("profile");
        auto &score   = t.type_find_or_create("score");
        auto &garment = t.label_find_or_create("garment");

        indices_t profile_ind = {{"profile_id", 0}, {"partner_id", 1}};
        indices_t garment_ind = {{"garment_id", 2}};

        auto profile_prop = query_properties(profile_ind, args);
        auto garment_prop = query_properties(garment_ind, args);

        auto score_key = t.edge_property_key("score", args[3].key.flags());
        
        stream.write_field("s");

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
                stream.write_edge_record(ea);
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
                stream.write_edge_record(ea);
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
                    stream.write_edge_record(ea);
                });
            });
        }

        stream.write_field("w");

        return t.commit();
    }

    ~CPUPlan() {}
};

extern "C" PlanInterface<Stream>* produce()
{
    return new CPUPlan();
}

extern "C" void destruct(PlanInterface<Stream>* p)
{
    delete p;
}
