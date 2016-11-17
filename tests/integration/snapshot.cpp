#include <random>

#include "logging/default.hpp"
#include "logging/streams/stdout.hpp"
#include "query/hardcode/basic.hpp"
#include "query/strip/stripper.hpp"
#include "storage/indexes/indexes.hpp"
#include "utils/sysinfo/memory.hpp"

// Returns uniform random size_t generator from range [0,n>
auto rand_gen(size_t n)
{
    std::default_random_engine generator;
    std::uniform_int_distribution<size_t> distribution(0, n - 1);
    return std::bind(distribution, generator);
}

void run(size_t n, std::string &query, Db &db)
{
    auto stripper = make_query_stripper(TK_LONG, TK_FLOAT, TK_STR, TK_BOOL);

    auto qf = hardcode::load_basic_functions(db);

    auto stripped = stripper.strip(query);
    std::cout << "Running query [" << stripped.hash << "] for " << n << " time."
              << std::endl;
    for (int i = 0; i < n; i++)
    {
        properties_t vec = stripped.arguments;
        assert(qf[stripped.hash](std::move(vec)));
    }
}

void add_edge(size_t n, Db &db)
{
    auto stripper = make_query_stripper(TK_LONG, TK_FLOAT, TK_STR, TK_BOOL);
    auto qf       = hardcode::load_basic_functions(db);

    std::string query = "MATCH (n1), (n2) WHERE ID(n1)=0 AND "
                        "ID(n2)=1 CREATE (n1)<-[r:IS {age: "
                        "25,weight: 70}]-(n2) RETURN r";

    auto stripped = stripper.strip(query);
    std::cout << "Running query [" << stripped.hash << "] for " << n
              << " time to add edge." << std::endl;

    std::vector<int64_t> vertices;
    for (auto &v : db.graph.vertices.access())
    {
        vertices.push_back(v.second.id);
    }

    auto rand = rand_gen(vertices.size());
    for (int i = 0; i < n; i++)
    {
        properties_t vec = stripped.arguments;
        vec[0]           = Property(Int64(vertices[rand()]), Flags::Int64);
        vec[1]           = Property(Int64(vertices[rand()]), Flags::Int64);
        assert(qf[stripped.hash](std::move(vec)));
    }
}

void add_property(Db &db, StoredProperty<TypeGroupVertex> &prop)
{
    DbAccessor t(db);

    t.vertex_access().fill().for_all([&](auto va) { va.set(prop); });

    assert(t.commit());
}

void add_property_different_int(Db &db, PropertyFamily<TypeGroupVertex> &f)
{
    DbAccessor t(db);

    auto key = f.get(Int64::type).family_key();

    size_t i = 0;
    t.vertex_access().fill().for_all([&](auto va) mutable {
        va.set(StoredProperty<TypeGroupVertex>(Int64(i), key));
        i++;
    });

    assert(t.commit());
}

size_t size(Db &db, IndexHolder<TypeGroupVertex, std::nullptr_t> &h)
{
    DbAccessor t(db);

    size_t count = 0;
    auto oin     = h.get_read();
    if (oin.is_present())
    {
        oin.get()->for_range(t).for_all([&](auto va) mutable { count++; });
    }

    t.commit();

    return count;
}

void assert_empty(Db &db)
{
    assert(db.graph.vertices.access().size() == 0);
    assert(db.graph.edges.access().size() == 0);
}

void clean_vertex(Db &db)
{
    DbTransaction t(db);
    t.clean_vertex_section();
    t.trans.commit();
}

void clean_edge(Db &db)
{
    DbTransaction t(db);
    t.clean_edge_section();
    t.trans.commit();
}

void clear_database(Db &db)
{
    std::string delete_all_vertices = "MATCH (n) DELETE n";
    std::string delete_all_edges    = "MATCH ()-[r]-() DELETE r";

    run(1, delete_all_edges, db);
    run(1, delete_all_vertices, db);
    clean_vertex(db);
    clean_edge(db);
    assert_empty(db);
}

bool equal(Db &a, Db &b)
{
    {
        auto acc_a = a.graph.vertices.access();
        auto acc_b = b.graph.vertices.access();

        if (acc_a.size() != acc_b.size())
        {
            return false;
        }

        auto it_a = acc_a.begin();
        auto it_b = acc_b.begin();

        for (auto i = acc_a.size(); i > 0; i--)
        {
            // TODO: compare
        }
    }

    {
        auto acc_a = a.graph.edges.access();
        auto acc_b = b.graph.edges.access();

        if (acc_a.size() != acc_b.size())
        {
            return false;
        }

        auto it_a = acc_a.begin();
        auto it_b = acc_b.begin();

        for (auto i = acc_a.size(); i > 0; i--)
        {
            // TODO: compare
        }
    }

    return true;
}

int main(void)
{
    logging::init_async();
    logging::log->pipe(std::make_unique<Stdout>());

    size_t cvl_n = 1000;

    std::string create_vertex_label =
        "CREATE (n:LABEL {name: \"cleaner_test\"}) RETURN n";
    std::string create_vertex_other =
        "CREATE (n:OTHER {name: \"cleaner_test\"}) RETURN n";
    std::string delete_label_vertices = "MATCH (n:LABEL) DELETE n";
    std::string delete_all_vertices   = "MATCH (n) DELETE n";

    // ******************************* TEST 1 ********************************//
    {
        std::cout << "TEST1" << std::endl;
        // make snapshot of empty db
        // add vertexs
        // add edges
        // empty database
        // import snapshot
        // assert database empty
        Db db("snapshot", false);
        db.snap_engine.make_snapshot();
        run(cvl_n, create_vertex_label, db);
        add_edge(cvl_n, db);
        clear_database(db);
        db.snap_engine.import();
        assert_empty(db);
    }

    // ******************************* TEST 2 ********************************//
    {
        std::cout << "TEST2" << std::endl;
        // add vertexs
        // add edges
        // make snapshot of db
        // empty database
        // import snapshot
        // create new db
        // compare database with new db
        Db db("snapshot", false);
        run(cvl_n, create_vertex_label, db);
        add_edge(cvl_n, db);
        db.snap_engine.make_snapshot();
        clear_database(db);
        db.snap_engine.import();
        {
            Db db2("snapshot");
            assert(equal(db, db2));
        }
    }

    // ******************************* TEST 3 ********************************//
    {
        std::cout << "TEST3" << std::endl;
        // add vertexs
        // add edges
        // make snapshot of db
        // compare database with different named database
        Db db("snapshot", false);
        run(cvl_n, create_vertex_label, db);
        add_edge(cvl_n, db);
        db.snap_engine.make_snapshot();
        {
            Db db2("not_snapshot");
            assert(!equal(db, db2));
        }
    }

    // ******************************* TEST 4 ********************************//
    {
        std::cout << "TEST4" << std::endl;
        // add vertices LABEL
        // add properties
        // add vertices LABEL
        // add index on proprety
        // assert index containts vertices
        // make snapshot
        // create new db
        // assert index on LABEL in new db exists
        // assert index in new db containts vertice
        Db db("snapshot", false);
        run(cvl_n, create_vertex_label, db);
        auto &family = db.graph.vertices.property_family_find_or_create("prop");
        add_property_different_int(db, family);
        run(cvl_n, create_vertex_other, db);
        IndexDefinition idef = {
            IndexLocation{VertexSide, Option<std::string>("prop"),
                          Option<std::string>(), Option<std::string>()},
            IndexType{false, None}};
        assert(db.indexes().add_index(idef));
        assert(cvl_n == size(db, family.index));
        db.snap_engine.make_snapshot();
        {
            Db db2("snapshot");
            assert(cvl_n == size(db, db2.graph.vertices
                                         .property_family_find_or_create("prop")
                                         .index));
        }
    }

    // TODO: more tests

    return 0;
}
