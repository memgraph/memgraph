#include "query_engine/hardcode/queries.hpp"

#include <random>

#include "barrier/barrier.cpp"

#include "logging/default.hpp"
#include "logging/streams/stdout.hpp"
#include "query_engine/query_stripper.hpp"
#include "utils/sysinfo/memory.hpp"

// Returns uniform random size_t generator from range [0,n>
auto rand_gen(size_t n)
{
    std::default_random_engine generator;
    std::uniform_int_distribution<size_t> distribution(0, n - 1);
    return std::bind(distribution, generator);
}

template <class S, class Q>
void run(size_t n, std::string &query, S &stripper, Q &qf)
{
    auto stripped = stripper.strip(query);
    std::cout << "Running query [" << stripped.hash << "] for " << n << " time."
              << std::endl;
    for (int i = 0; i < n; i++) {
        properties_t vec = stripped.arguments;
        assert(qf[stripped.hash](std::move(vec)));
    }
}

template <class S, class Q>
void add_edge(size_t n, Db &db, S &stripper, Q &qf)
{

    std::string query = "MATCH (n1), (n2) WHERE ID(n1)=0 AND "
                        "ID(n2)=1 CREATE (n1)<-[r:IS {age: "
                        "25,weight: 70}]-(n2) RETURN r";

    auto stripped = stripper.strip(query);
    std::cout << "Running query [" << stripped.hash << "] for " << n
              << " time to add edge." << std::endl;

    std::vector<int64_t> vertices;
    for (auto &v : db.graph.vertices.access()) {
        vertices.push_back(v.second.id);
    }

    auto rand = rand_gen(vertices.size());
    for (int i = 0; i < n; i++) {
        properties_t vec = stripped.arguments;
        vec[0] = Property(Int64(vertices[rand()]), Flags::Int64);
        vec[1] = Property(Int64(vertices[rand()]), Flags::Int64);
        assert(qf[stripped.hash](std::move(vec)));
    }
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

template <class S, class Q>
void clear_database(Db &db, S &stripper, Q &qf)
{
    std::string delete_all_vertices = "MATCH (n) DELETE n";
    std::string delete_all_edges = "MATCH ()-[r]-() DELETE r";

    run(1, delete_all_edges, stripper, qf);
    run(1, delete_all_vertices, stripper, qf);
    clean_vertex(db);
    clean_edge(db);
    assert_empty(db);
}

bool equal(Db &a, Db &b)
{
    {
        auto acc_a = a.graph.vertices.access();
        auto acc_b = b.graph.vertices.access();

        if (acc_a.size() != acc_b.size()) {
            return false;
        }

        auto it_a = acc_a.begin();
        auto it_b = acc_b.begin();

        for (auto i = acc_a.size(); i > 0; i--) {
            // TODO: compare
        }
    }

    {
        auto acc_a = a.graph.edges.access();
        auto acc_b = b.graph.edges.access();

        if (acc_a.size() != acc_b.size()) {
            return false;
        }

        auto it_a = acc_a.begin();
        auto it_b = acc_b.begin();

        for (auto i = acc_a.size(); i > 0; i--) {
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

    Db db("snapshot");

    auto query_functions = load_queries(barrier::trans(db));

    auto stripper = make_query_stripper(TK_LONG, TK_FLOAT, TK_STR, TK_BOOL);

    std::string create_vertex_label =
        "CREATE (n:LABEL {name: \"cleaner_test\"}) RETURN n";
    std::string create_vertex_other =
        "CREATE (n:OTHER {name: \"cleaner_test\"}) RETURN n";
    std::string delete_label_vertices = "MATCH (n:LABEL) DELETE n";
    std::string delete_all_vertices = "MATCH (n) DELETE n";

    // ********************* MAKE SURE THAT DB IS EMPTY **********************//
    clear_database(db, stripper, query_functions);

    std::cout << "TEST1" << std::endl;
    // ******************************* TEST 1 ********************************//
    // make snapshot of empty db
    // add vertexs
    // add edges
    // empty database
    // import snapshot
    // assert database empty

    db.snap_engine.make_snapshot();
    run(cvl_n, create_vertex_label, stripper, query_functions);
    add_edge(cvl_n, db, stripper, query_functions);
    clear_database(db, stripper, query_functions);
    db.snap_engine.import();
    assert_empty(db);

    std::cout << "TEST2" << std::endl;
    // ******************************* TEST 2 ********************************//
    // add vertexs
    // add edges
    // make snapshot of db
    // empty database
    // import snapshot
    // create new db
    // compare database with new db

    run(cvl_n, create_vertex_label, stripper, query_functions);
    add_edge(cvl_n, db, stripper, query_functions);
    db.snap_engine.make_snapshot();
    clear_database(db, stripper, query_functions);
    db.snap_engine.import();
    {
        Db db2("snapshot");
        assert(equal(db, db2));
    }

    std::cout << "TEST3" << std::endl;
    // ******************************* TEST 3 ********************************//
    // compare database with different named database
    {
        Db db2("not_snapshot");
        assert(!equal(db, db2));
    }

    // TODO: more tests

    return 0;
}
