#include <iostream>
#include <string>

#include "query/util.hpp"
#include "query/plan_interface.hpp"
#include "storage/model/properties/all.hpp"
#include "storage/edge_x_vertex.hpp"
#include "using.hpp"

using std::cout;
using std::endl;

// Query: CREATE (n:ACCOUNT {id: 2322, name: "TEST", country: "Croatia", "created_at": 2352352}) RETURN n

class CPUPlan : public PlanInterface<Stream>
{
public:

    bool run(Db &db, const PlanArgsT &args, Stream &stream) override
    {
        DbAccessor t(db);

        auto prop_id = t.vertex_property_key("id", args[0].key.flags());
        auto prop_name = t.vertex_property_key("name", args[1].key.flags());
        auto prop_country =
            t.vertex_property_key("country", args[2].key.flags());
        auto prop_created =
            t.vertex_property_key("created_at", args[3].key.flags());

        auto &label = t.label_find_or_create("ACCOUNT");

        auto vertex_accessor = t.vertex_insert();

        vertex_accessor.set(prop_id, std::move(args[0]));
        vertex_accessor.set(prop_name, std::move(args[1]));
        vertex_accessor.set(prop_country, std::move(args[2]));
        vertex_accessor.set(prop_created, std::move(args[3]));
        vertex_accessor.add_label(label);

        stream.write_field("p");
        stream.write_vertex_record(vertex_accessor);
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
