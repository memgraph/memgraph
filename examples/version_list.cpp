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

    v1->data.props.emplace<String>("name", "buda");
    v1->data.props.emplace<Int32>("age", 23);

    cout << vertex;

    t1.commit();

    auto& t2 = engine.begin();
    auto a2 = vertex.access(t2);
    a2.remove();

    /* v2->data.props.emplace<Int32>("age", 24); */

    cout << vertex;

    return 0;
}
