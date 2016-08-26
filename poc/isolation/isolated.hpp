// This is the file of isolated code. It has access only to header.hpp

#include "isolation/header.hpp"

namespace sha
{
int do_something(Db &db)
{
    auto &name = db.get_name("name");

    auto acc = db.access();

    auto ret = acc.get_prop(name);

    return ret;
}
}
