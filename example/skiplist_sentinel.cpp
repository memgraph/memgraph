#include <iostream>

#include "data_structures/skiplist/skiplist.hpp"
#include "storage/indexes/keys/unique_key.hpp"

using std::cout;
using std::endl;

using skiplist_t = SkipList<UniqueKeyAsc<int>, int>;

void print_skiplist(const skiplist_t::Accessor& skiplist)
{
    cout << "---- skiplist now has: ";

    for(auto& kv : skiplist)
        cout << "(" << kv.first << ", " << kv.second << ") ";

    cout << "----" << endl;
}

int main(void)
{
    skiplist_t skiplist;

    auto accessor = skiplist.access();

    // this has to be here since UniqueKey<> class takes references!
    int keys[] = {0, 1, 2, 3, 4, 5, 6, 7, 8};

    accessor.insert_unique(keys[1], 10);
    accessor.insert_unique(keys[2], 20);
    accessor.insert_unique(keys[7], 70);
    accessor.insert_unique(keys[4], 40);
    accessor.insert_unique(keys[8], 80);
    accessor.insert_unique(keys[3], 30);

    print_skiplist(accessor);

    return 0;
}
