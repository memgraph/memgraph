#include <iostream>
#include <string>

#include "query/util.hpp"
#include "query/i_plan_cpu.hpp"
#include "storage/model/properties/all.hpp"
#include "using.hpp"

using std::cout;
using std::endl;

// Query: MATCH (p:profile {profile_id: 1}) DELETE p
// Hash: 6763665709953344106

class CodeCPU : public IPlanCPU<Stream>
{
public:

    bool run(Db &db, plan_args_t &args, Stream &stream) override
    {
        DbAccessor t(db);

        indices_t indices = {{"profile_id", 0}};
        auto properties   = query_properties(indices, args);

        auto &label = t.label_find_or_create("profile");

        label.index()
            .for_range(t)
            .properties_filter(t, properties)
            .for_all([&](auto va) { va.remove(); });

        stream.write_empty_fields();
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
