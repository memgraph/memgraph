#include <iostream>
#include <string>

#include "query_engine/i_code_cpu.hpp"

//  TODO generate with the template engine 
// #include "storage/model/properties/jsonwriter.hpp"

using std::cout;
using std::endl;

// query: {{query}}

class {{class_name}} : public ICodeCPU
{
public:

    QueryResult::sptr run(Db& db) override
    {
{{code}}    }

    ~{{class_name}}() {}
};


extern "C" ICodeCPU* produce() 
{
    return new {{class_name}}();
}

extern "C" void destruct(ICodeCPU* p) 
{
    delete p;
}
