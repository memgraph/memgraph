#include <iostream>
#include <string>

#include "query/util.hpp"
#include "query/i_plan_cpu.hpp"
#include "storage/model/properties/all.hpp"

using std::cout;
using std::endl;

// query: {{query}}

class {{class_name}} : public IPlanCPU<{{stream}}>
{
public:

    bool run(Db &db, plan_args_t &args,
             {{stream}} &stream) override
    {
{{code}}
    }

    ~{{class_name}}() {}
};

extern "C" IPlanCPU<{{stream}}>* produce()
{
    return new {{class_name}}();
}

extern "C" void destruct(IPlanCPU<{{stream}}>* p)
{
    delete p;
}
