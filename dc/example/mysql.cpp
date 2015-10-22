#include "db.hpp"

class mysql : public db
{
public:

    virtual void name() const
    {
        cout << "MySQL" << endl;
    }

    virtual void type() const
    {
        cout << "Relational" << endl;
    }

    ~mysql() {}
};

extern "C" db* produce()
{
    return new mysql();
}

extern "C" void destruct(db* p)
{
    delete p;
}
