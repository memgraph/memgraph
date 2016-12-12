#include <iostream>

#include "mvcc/id.hpp"

using std::cout;
using std::endl;

int main() {
    
    Id id0(0);
    Id id1(1);
    Id id2(1);
    Id id3(id2);
    Id id4 = id3;
    Id id5(5);

    cout << id5 << " " << id0 << endl;

    if (id0 < id5)
        cout << "id0 < id5" << endl;

    if (id1 == id2)
        cout << "are equal" << endl;

    if (id3 == id4)
        cout << "id3 == id4" << endl;

    if (id5 > id0)
        cout << "id5 > id0" << endl;

    if (id5 != id3)
        cout << "id5 != id3" << endl;

    if (id1 >= id2)
        cout << "id1 >= id2" << endl;

    if (id3 <= id4)
        cout << "id3 <= id4" << endl;

    return 0;
}
