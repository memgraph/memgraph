#include <iostream>
#include <string>

#include "query/i_plan_cpu.hpp"
#include "storage/model/properties/all.hpp"

using std::cout;
using std::endl;

// query: {{query}}

// BARRIER!
namespace barrier
{

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

}


extern "C" IPlanCPU<barrier::{{stream}}>* produce()
{
    // BARRIER!
    return new barrier::{{class_name}}();
}

extern "C" void destruct(IPlanCPU<barrier::{{stream}}>* p)
{
    delete p;
}
