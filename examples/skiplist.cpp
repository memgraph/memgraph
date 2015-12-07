#include <iostream>

#include "data_structures/skiplist/skiplist.hpp"

using std::cout;
using std::endl;

using skiplist_t = SkipList<int, int>;

void print_skiplist(const skiplist_t::Accessor& skiplist)
{
    cout << "---- skiplist now has: ";

    for(auto& kv : skiplist)
        cout << "(" << kv.first << ", " << kv.second << ") ";

    cout << "----" << endl;
}

int main(void)
{
    cout << std::boolalpha;
    skiplist_t skiplist;

    auto accessor = skiplist.access();

    cout << "added non-existing (1, 10)? (true) "
         << accessor.insert_unique(1, 10).second << endl;

    cout << "added already existing (1, 10)? (false) "
         << accessor.insert_unique(1, 10).second << endl;

    accessor.insert_unique(2, 20);
    print_skiplist(accessor);

    cout << "value at key 3 exists? (false) "
         << (accessor.find(3) == accessor.end()) << endl;

    cout << "value at key 2 exists? (true) "
         << (accessor.find(2) != accessor.end()) << endl;

    cout << "at key 2 is? (20) " << accessor.find(2)->second << endl;

    cout << "removed existing (1)? (true) " << accessor.remove(1) << endl;
    cout << "removed non-existing (3)? (false) " << accessor.remove(3) << endl;

    accessor.insert_unique(1, 10);
    accessor.insert_unique(4, 40);

    print_skiplist(accessor);

    return 0;
}
