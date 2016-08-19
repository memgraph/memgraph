#include <iostream>

#include "data_structures/concurrent/concurrent_set.hpp"

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

    cout << "added non-existing 1? (true) " << accessor.insert(1).second
         << endl;

    cout << "added already existing 1? (false) " << accessor.insert(1).second
         << endl;

    accessor.insert(2);
    print_skiplist(accessor);

    cout << "item 3 doesn't exist? (true) "
         << (accessor.find(3) == accessor.end()) << endl;

    cout << "item 3 exists? (false) " << accessor.contains(3) << endl;

    cout << "item 2 exists? (true) " << (accessor.find(2) != accessor.end())
         << endl;

    cout << "at item 2 is? 2 " << *accessor.find(2) << endl;

    cout << "removed existing 1? (true) " << accessor.remove(1) << endl;
    cout << "removed non-existing 3? (false) " << accessor.remove(3) << endl;

    accessor.insert(1);
    accessor.insert(4);

    print_skiplist(accessor);

    return 0;
}
