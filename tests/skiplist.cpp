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
    skiplist_t skiplist;
    auto accessor = skiplist.access();

    // insert 10
    assert(accessor.insert_unique(1, 10).second == true);
    
    // try insert 10 again (should fail)
    assert(accessor.insert_unique(1, 10).second == false);

    // insert 20
    assert(accessor.insert_unique(2, 20).second == true);

    print_skiplist(accessor);
    
    // value at key 3 shouldn't exist
    assert((accessor.find(3) == accessor.end()) == true);

    // value at key 2 should exist
    assert((accessor.find(2) != accessor.end()) == true);

    // at key 2 is 20 (true)
    assert(accessor.find(2)->second == 20);
    
    // removed existing (1)
    assert(accessor.remove(1) == true);

    // removed non-existing (3)
    assert(accessor.remove(3) == false);

    // insert (1, 10)
    assert(accessor.insert_unique(1, 10).second == true);

    // insert (4, 40)
    assert(accessor.insert_unique(4, 40).second == true);

    print_skiplist(accessor);

    return 0;
}
