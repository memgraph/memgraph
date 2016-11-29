#include <iostream>
#include <string>

#include "query/util.hpp"
#include "query/i_plan_cpu.hpp"
#include "storage/model/properties/all.hpp"
#include "using.hpp"

using std::cout;
using std::endl;

// Query: MATCH (n) DETACH DELETE n
// Hash: 4798158026600988079

class CodeCPU : public IPlanCPU<Stream>
{
public:

    bool run(Db &db, plan_args_t &args, Stream &stream) override
    {
        DbAccessor t(db);

        t.edge_access().fill().for_all([&](auto e) { e.remove(); });
        t.vertex_access().fill().isolated().for_all(
            [&](auto a) { a.remove(); });

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
