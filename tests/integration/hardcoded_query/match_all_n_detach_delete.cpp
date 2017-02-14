#include <iostream>
#include <string>

#include "query/util.hpp"
#include "query/plan_interface.hpp"
#include "storage/model/properties/all.hpp"
#include "using.hpp"

using std::cout;
using std::endl;

// Query: MATCH (n) DETACH DELETE n

class CPUPlan : public PlanInterface<Stream>
{
public:

    bool run(Db &db, const PlanArgsT &args, Stream &stream) override
    {
        DbAccessor t(db);

        t.edge_access().fill().for_all([&](auto e) { e.remove(); });
        t.vertex_access().fill().isolated().for_all(
            [&](auto a) { a.remove(); });

        stream.write_empty_fields();
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
