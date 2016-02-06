#include <iostream>

#include "query_engine/i_code_cpu.hpp"

using std::cout;
using std::endl;

class CreateReturn : public ICodeCPU
{
public:

    void name() const override
    {
        cout << "CRETE RETURN QUERY" << endl;
    }

    void run(Db& db) const override
    {
        cout << db.identifier() << endl;
    }

    ~CreateReturn() {}
};

extern "C" ICodeCPU* produce() 
{
    return new CreateReturn();
}

extern "C" void destruct(ICodeCPU* p) 
{
    delete p;
}
