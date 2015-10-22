#include "db.hpp"

class memsql : public db 
{
public:

    void name() const override
    {
        cout << "MemSQL" << endl;
    }

    void type() const override
    {
        cout << "InMemory" << endl;
    }

    ~memsql() {}
};

extern "C" db* produce() 
{
    return new memsql();
}

extern "C" void destruct(db* p) 
{
    delete p;
}
