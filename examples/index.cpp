#include <iostream>

#include "transactions/engine.hpp"
#include "mvcc/version_list.hpp"
#include "storage/vertex.hpp"

#include "storage/indexes/property_index.hpp"

using std::cout;
using std::endl;

using Record = mvcc::VersionList<Vertex>;

tx::Engine engine;
Index<Vertex>* index = new PropertyIndex<Vertex>();

Record* create(int id, tx::Transaction& t)
{
    auto v = new record_t(id);
    auto a = v->access(t);
    a.insert();

    return v;
}

auto insert(int id, tx::Transaction& t)
{
    auto r = create(id, t);
    return index.insert(&r->id, r, t);
}

int main(void)
{
    /* v1->data.props.set<String>("name", "buda"); */
    /* v1->data.props.set<Int32>("age", 23); */

    auto& t1 = engine.begin();
    insert(0, t1);
    insert(1, t1);
    insert(2, t1);
    insert(3, t1);
    insert(6, t1);
    insert(7, t1);
    t1.commit();

    auto& t2 = engine.begin();
    insert(4, t2);
    insert(5, t2);
    insert(8, t2);
    t2.commit();

    auto& t3 = engine.begin();
    auto cursor = index.scan(8, t3);

    for(; not cursor.end(); cursor++)
        cout << cursor->id() << endl;

    return 0;
}
