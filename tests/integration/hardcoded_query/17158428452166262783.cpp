#include <iostream>
#include <string>

#include "query/util.hpp"
#include "query/i_plan_cpu.hpp"
#include "storage/model/properties/all.hpp"
#include "using.hpp"

using std::cout;
using std::endl;

// Query: CREATE (p:profile {profile_id: 111, partner_id: 55}) RETURN p
// Hash: 17158428452166262783

class CodeCPU : public IPlanCPU<Stream>
{
public:

    bool run(Db &db, plan_args_t &args, Stream &stream) override
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
