#include "db.hpp"

class neo4j : public db
{
public:

    void name() const override
    {
        cout << "Neo4j" << endl;
    }

    void type() const override
    {
        cout << "Graph" << endl;
    }

    ~neo4j() {}
};

extern "C" db* produce() 
{
    return new neo4j();
}

extern "C" void destruct(db* p) 
{
    delete p;
}
