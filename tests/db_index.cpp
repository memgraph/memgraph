#include <iostream>

#include "storage/indexes/index.hpp"

using std::cout;
using std::endl;

using StringUniqueKeyAsc = UniqueKeyAsc<std::string>;

int main(void)
{
    // index creation
    auto index = std::make_shared<Index<StringUniqueKeyAsc, std::string>>();

    return 0;
}
