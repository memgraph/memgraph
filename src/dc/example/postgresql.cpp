#include "db.hpp"

class postgresql : public db 
{
public:

    void name() const override
    {
        cout << "PostgreSQL" << endl;
    }

    void type() const override
    {
        cout << "Relational" << endl;
    }

    ~postgresql() {}
};

extern "C" db* produce() 
{
    return new postgresql();
}

extern "C" void destruct(db* p) 
{
    delete p;
}
