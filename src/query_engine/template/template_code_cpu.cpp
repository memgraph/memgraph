#include <iostream>
#include <string>

#include "query_engine/util.hpp"
#include "query_engine/i_code_cpu.hpp"
#include "storage/model/properties/all.hpp"

using std::cout;
using std::endl;

// query: {{query}}

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


extern "C" ICodeCPU<{{stream}}>* produce()
{
    return new {{class_name}}();
}

extern "C" void destruct(ICodeCPU<{{stream}}>* p)
{
    delete p;
}
