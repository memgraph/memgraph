#include <iostream>
#include <string>

#include "query_engine/i_code_cpu.hpp"

using std::cout;
using std::endl;

class {{class_name}} : public ICodeCPU
{
public:

    std::string query() const override
    {
        return {{query}};
    }

    void run(Db& db) const override
    {
        {{code}}
    }

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
