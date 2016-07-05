#include <iostream>

#include "data_structures/concurrent/concurrent_set.hpp"
#include "utils/assert.hpp"

using std::cout;
using std::endl;

void print_skiplist(const ConcurrentSet<int>::Accessor &skiplist)
{
    cout << "---- skiplist set now has: ";

    for (auto &item : skiplist)
        cout << item << ", ";

    cout << "----" << endl;
}

int main(void)
{
    ConcurrentSet<int> set;

    auto accessor = set.access();

    cout << std::boolalpha;

    permanent_assert(accessor.insert(1).second == true,
                     "added non-existing 1? (true)");

    permanent_assert(accessor.insert(1).second == false,
                     "added already existing 1? (false)");

    permanent_assert(accessor.insert(2).second == true,
                     "added non-existing 2? (true)");

    permanent_assert(accessor.find(3) == accessor.end(),
                     "item 3 doesn't exist? (true)");

    permanent_assert(accessor.contains(3) == false, "item 3 exists? (false)");

    permanent_assert(accessor.find(2) != accessor.end(),
                     "item 2 exists? (true)");

    permanent_assert(*accessor.find(2) == 2, "find item 2");

    permanent_assert(accessor.remove(1) == true, "removed existing 1? (true)");

    permanent_assert(accessor.remove(3) == false,
                     "try to remove non existing element");

    permanent_assert(accessor.insert(1).second == true, "add 1 again");

    permanent_assert(accessor.insert(4).second == true, "add 4");

    print_skiplist(accessor);

    return 0;
}
