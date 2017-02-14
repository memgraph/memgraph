#include <iostream>
#include <string>

#include "query/util.hpp"
#include "query/plan_interface.hpp"
#include "storage/model/properties/all.hpp"
#include "using.hpp"

using std::cout;
using std::endl;

// Query: CREATE (p:profile {profile_id: 112, partner_id: 55}) RETURN p

class CPUPlan : public PlanInterface<Stream>
{
public:

    bool run(Db &db, const PlanArgsT &args, Stream &stream) override
    {
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

        stream.write_field("p");
        stream.write_vertex_record(va);
        stream.write_meta("w");

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
