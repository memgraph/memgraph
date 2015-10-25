#ifndef MEMGRAPH_DL_EXAMPLE_DB_HPP
#define MEMGRAPH_DL_EXAMPLE_DB_HPP

#include <iostream>

using namespace std;

class db
{
public:
    // If virtual methods don't have = 0 the compiler
    // won't create appropriate _ZTI symbol inside
    // the .so lib. That will lead to undefined symbol
    // error while the library is loading.
    //
    // TODO: why?
    virtual void name() const = 0;
    virtual void type() const = 0;
    virtual ~db() {}
};

typedef db* (*produce_t)();
typedef void (*destruct_t)(db*);

#endif
