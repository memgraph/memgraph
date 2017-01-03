#include "_hardcoded_query/basic.hpp"
#include "logging/default.hpp"
#include "logging/streams/stdout.hpp"
#include "query/preprocesor.hpp"
#include "query/strip/stripper.hpp"
#include "utils/assert.hpp"
#include "utils/sysinfo/memory.hpp"

QueryPreprocessor preprocessor;

template <class Q>
void run(size_t n, std::string &query, Q &qf)
{
    auto stripped = preprocessor.preprocess(query);

    logging::info("Running query [{}] x {}.", stripped.hash, n);

    for (int i = 0; i < n; i++)
    {
        properties_t vec = stripped.arguments;
        permanent_assert(qf[stripped.hash](std::move(vec)), "Query failed!");
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

    Db db("cleaning");

    size_t entities_number = 1000;
    auto query_functions   = hardcode::load_basic_functions(db);

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
    run(entities_number, create_vertex_label, query_functions);
    permanent_assert(db.graph.vertices.access().size() == entities_number,
                     "Entities number doesn't match");

    clean_vertex(db);
    permanent_assert(db.graph.vertices.access().size() == entities_number,
                     "Entities number doesn't match (after cleaning)");

    run(1, delete_label_vertices, query_functions);
    permanent_assert(db.graph.vertices.access().size() == entities_number,
                     "Entities number doesn't match (delete label vertices)");

    clean_vertex(db);
    permanent_assert(db.graph.vertices.access().size() == 0,
                     "Db should be empty");

    // ******************************* TEST 2 ********************************//
    // add vertices a
    // add vertices b
    // clean vertices
    // delete vertices a
    // clean vertices
    // delete vertices all
    run(entities_number, create_vertex_label, query_functions);
    permanent_assert(db.graph.vertices.access().size() == entities_number,
                     "Entities number doesn't match");

    run(entities_number, create_vertex_other, query_functions);
    permanent_assert(db.graph.vertices.access().size() == entities_number * 2,
                     "Entities number doesn't match");

    clean_vertex(db);
    permanent_assert(db.graph.vertices.access().size() == entities_number * 2,
                     "Entities number doesn't match");

    run(1, delete_label_vertices, query_functions);
    permanent_assert(db.graph.vertices.access().size() == entities_number * 2,
                     "Entities number doesn't match");

    clean_vertex(db);
    permanent_assert(db.graph.vertices.access().size() == entities_number,
                     "Entities number doesn't match");

    run(1, delete_all_vertices, query_functions);
    permanent_assert(db.graph.vertices.access().size() == entities_number,
                     "Entities number doesn't match");

    clean_vertex(db);
    permanent_assert(db.graph.vertices.access().size() == 0,
                     "Db should be empty");

    // TODO: more tests

    return 0;
}
