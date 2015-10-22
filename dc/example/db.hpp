#ifndef MEMGRAPH_DL_EXAMPLE_DB_HPP
#define MEMGRAPH_DL_EXAMPLE_DB_HPP

#include <iostream>

using namespace std;

class db
{
public:
    virtual void name() const;
    virtual void type() const;
    virtual ~db() {}
};

typedef db* (*produce_t)();
typedef void (*destruct_t)(db*);

#endif
