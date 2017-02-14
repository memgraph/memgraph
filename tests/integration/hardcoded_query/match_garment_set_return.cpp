#include <iostream>
#include <string>

#include "query/util.hpp"
#include "query/plan_interface.hpp"
#include "storage/model/properties/all.hpp"
#include "using.hpp"

using std::cout;
using std::endl;

// Query: MATCH (g:garment {garment_id: 3456}) SET g.reveals = 50 RETURN g

class CPUPlan : public PlanInterface<Stream>
{
public:

    bool run(Db &db, const PlanArgsT &args, Stream &stream) override
    {
        DbAccessor t(db);

        auto reveals_key =
            t.vertex_property_key("reveals", args[1].key.flags());

        indices_t indices = {{"garment_id", 0}};
        auto properties   = query_properties(indices, args);

        auto &label       = t.label_find_or_create("garment");

        stream.write_field("g");

        label.index()
            .for_range(t)
            .properties_filter(t, properties)
            .fill()
            .for_all([&](auto va) {
                va.set(reveals_key, args[1]);
                stream.write_vertex_record(va); 
            });

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
