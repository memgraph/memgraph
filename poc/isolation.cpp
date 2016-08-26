// Making it as first import will prevent accidentaly importing to isolated
// other code.
#include "isolation/isolated.hpp"

#include <iostream>

#include "isolation/db.hpp"
#include "isolation/header.hpp"

using namespace base;

int main()
{
    std::cout << sizeof(Accessor) << " : " << alignof(Accessor) << "\n";

    Db db;
    db.data = 207;

    auto ret = sha::do_something(reinterpret_cast<sha::Db &>(db));

    std::cout << ret << std::endl;

    return 0;
}
