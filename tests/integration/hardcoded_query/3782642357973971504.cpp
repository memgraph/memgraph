#include <iostream>
#include <string>

#include "query/util.hpp"
#include "query/i_plan_cpu.hpp"
#include "storage/model/properties/all.hpp"
#include "using.hpp"

using std::cout;
using std::endl;

// Query: MERGE (g1:garment {garment_id:1234})-[r:default_outfit]-(g2:garment {garment_id: 2345}) RETURN r
// Hash: 3782642357973971504

class CodeCPU : public IPlanCPU<Stream>
{
public:

    bool run(Db &db, plan_args_t &args, Stream &stream) override
    {
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

        stream.write_field("r");

        it_vertex_1.fill().for_all([&](auto va1) -> void {
            it_vertex_2.fill().for_all([&](auto va2) -> void {
                auto edge_accessor = t.edge_insert(va1, va2);
                edge_accessor.edge_type(edge_type);

                stream.write_edge_record(edge_accessor);
            });
        });

        stream.write_meta("w");

        return t.commit();
    }

    ~CodeCPU() {}
};

extern "C" IPlanCPU<Stream>* produce()
{
    return new CodeCPU();
}

extern "C" void destruct(IPlanCPU<Stream>* p)
{
    delete p;
}
