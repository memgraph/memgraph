#include <iostream>
#include <string>

#include "query_engine/i_code_cpu.hpp"
#include "storage/model/properties/all.hpp"

using std::cout;
using std::endl;

// query: {{query}}

// BARRIER!
namespace barrier
{

class {{class_name}} : public ICodeCPU<{{stream}}>
{
public:

    bool run(Db &db, code_args_t &args,
             {{stream}} &stream) override
    {
{{code}}
    }

    ~{{class_name}}() {}
};

}


extern "C" ICodeCPU<barrier::{{stream}}>* produce()
{
    // BARRIER!
    return new barrier::{{class_name}}();
}

extern "C" void destruct(ICodeCPU<barrier::{{stream}}>* p)
{
    delete p;
}
