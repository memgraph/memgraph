#include "logging/default.hpp"
#include "logging/streams/stdout.hpp"
#include "query/hardcode/basic.hpp"
#include "query/strip/stripper.hpp"
#include "utils/sysinfo/memory.hpp"

template <class S, class Q>
void run(size_t n, std::string &query, S &stripper, Q &qf)
{
    auto stripped = stripper.strip(query);
    std::cout << "Running query [" << stripped.hash << "] for " << n << " time."
              << std::endl;
    for (int i = 0; i < n; i++)
    {
        properties_t vec = stripped.arguments;
        assert(qf[stripped.hash](std::move(vec)));
    }
}

void clean_vertex(Db &db)
{
    DbTransaction t(db);
    t.clean_vertex_section();
    t.trans.commit();
}

int main(void)
{
    logging::init_async();
    logging::log->pipe(std::make_unique<Stdout>());

    size_t entities_number = 1000;

    Db db("cleaning");

    auto query_functions = hardcode::load_basic_functions(db);

    auto stripper = make_query_stripper(TK_LONG, TK_FLOAT, TK_STR, TK_BOOL);

    std::string create_vertex_label =
        "CREATE (n:LABEL {name: \"cleaner_test\"}) RETURN n";
    std::string create_vertex_other =
        "CREATE (n:OTHER {name: \"cleaner_test\"}) RETURN n";
    std::string delete_label_vertices = "MATCH (n:LABEL) DELETE n";
    std::string delete_all_vertices   = "MATCH (n) DELETE n";

    // ******************************* TEST 1 ********************************//
    // add vertices a
    // clean vertices
    // delete vertices a
    // clean vertices
    run(entities_number, create_vertex_label, stripper, query_functions);
    assert(db.graph.vertices.access().size() == entities_number);

    clean_vertex(db);
    assert(db.graph.vertices.access().size() == entities_number);

    run(1, delete_label_vertices, stripper, query_functions);
    assert(db.graph.vertices.access().size() == entities_number);

    clean_vertex(db);
    assert(db.graph.vertices.access().size() == 0);

    // ******************************* TEST 2 ********************************//
    // add vertices a
    // add vertices b
    // clean vertices
    // delete vertices a
    // clean vertices
    // delete vertices all
    run(entities_number, create_vertex_label, stripper, query_functions);
    assert(db.graph.vertices.access().size() == entities_number);

    run(entities_number, create_vertex_other, stripper, query_functions);
    assert(db.graph.vertices.access().size() == entities_number * 2);

    clean_vertex(db);
    assert(db.graph.vertices.access().size() == entities_number * 2);

    run(1, delete_label_vertices, stripper, query_functions);
    assert(db.graph.vertices.access().size() == entities_number * 2);

    clean_vertex(db);
    assert(db.graph.vertices.access().size() == entities_number);

    run(1, delete_all_vertices, stripper, query_functions);
    assert(db.graph.vertices.access().size() == entities_number);

    clean_vertex(db);
    assert(db.graph.vertices.access().size() == 0);

    // TODO: more tests

    return 0;
}
