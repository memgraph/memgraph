#include <iostream>
#include <string>

#include "query/util.hpp"
#include "query/plan_interface.hpp"
#include "storage/model/properties/all.hpp"
#include "storage/edge_x_vertex.hpp"
#include "using.hpp"

using std::cout;
using std::endl;

// Query: MATCH (a {id:0}), (p {id: 1}) CREATE (a)-[r:IS]->(p) RETURN r

class CPUPlan : public PlanInterface<Stream>
{
public:

    bool run(Db &db, const PlanArgsT &args, Stream &stream) override
    {
        DbAccessor t(db);
        auto &edge_type = t.type_find_or_create("IS");

        auto v1 = t.vertex_find(args[0].as<Int64>().value());
        if (!option_fill(v1)) return t.commit(), false;

        auto v2 = t.vertex_find(args[1].as<Int64>().value());
        if (!option_fill(v2)) return t.commit(), false;

        auto edge_accessor = t.edge_insert(v1.get(), v2.get());

        edge_accessor.edge_type(edge_type);

        stream.write_field("r");
        stream.write_edge_record(edge_accessor);
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
