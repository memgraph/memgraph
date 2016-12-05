#include <iostream>

#include "transactions/engine.hpp"
#include "mvcc/version_list.hpp"
#include "storage/vertex.hpp"

using std::cout;
using std::endl;

int main(void)
{
    tx::Engine engine;
    VertexRecord vertex;

    auto& t1 = engine.begin();
    auto a1 = vertex.access(t1);
    auto v1 = a1.insert();

    v1->data.props.set<String>("name", "buda");
    v1->data.props.set<Int32>("age", 23);

    cout << vertex;

    t1.commit();

    auto& t2 = engine.begin();
    auto a2 = vertex.access(t2);
    auto v2 = a2.update();

    v2->data.props.set<Int32>("age", 24);

    cout << vertex;

    t2.abort();

    auto& t3 = engine.begin();
    auto a3 = vertex.access(t3);
    auto v3 = a3.update();

    v3->data.props.set<Int32>("age", 25);

    cout << vertex;

    return 0;
}
