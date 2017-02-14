// TODO: refactor (backlog task)

#include <random>

#include "_hardcoded_query/basic.hpp"
#include "logging/default.hpp"
#include "logging/streams/stdout.hpp"
#include "query/preprocessor.hpp"
#include "query/stripper.hpp"
#include "storage/indexes/indexes.hpp"
#include "utils/assert.hpp"
#include "utils/signals/handler.hpp"
#include "utils/stacktrace/log.hpp"
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
    auto qf       = hardcode::load_basic_functions(db);
    QueryPreprocessor preprocessor;
    auto stripped = preprocessor.preprocess(query);

    logging::info("Running query [{}] x {}.", stripped.hash, n);

    for (int i = 0; i < n; i++)
    {
        properties_t vec = stripped.arguments;
        auto commited    = qf[stripped.hash](std::move(vec));
        permanent_assert(commited, "Query execution failed");
    }
}

void add_edge(size_t n, Db &db)
{
    auto qf = hardcode::load_basic_functions(db);
    std::string query = "MATCH (n1), (n2) WHERE ID(n1)=0 AND "
                        "ID(n2)=1 CREATE (n1)<-[r:IS {age: "
                        "25,weight: 70}]-(n2) RETURN r";
    QueryPreprocessor preprocessor;
    auto stripped = preprocessor.preprocess(query);

    logging::info("Running query [{}] (add edge) x {}", stripped.hash, n);

    std::vector<int64_t> vertices;
    for (auto &v : db.graph.vertices.access())
    {
        vertices.push_back(v.second.id);
    }
    permanent_assert(vertices.size() > 0, "Vertices size is zero");

    auto rand = rand_gen(vertices.size());
    for (int i = 0; i < n; i++)
    {
        properties_t vec = stripped.arguments;
        vec[0]           = Property(Int64(vertices[rand()]), Flags::Int64);
        vec[1]           = Property(Int64(vertices[rand()]), Flags::Int64);
        permanent_assert(qf[stripped.hash](std::move(vec)), "Add edge failed");
    }
}

void add_property(Db &db, StoredProperty<TypeGroupVertex> &prop)
{
    DbAccessor t(db);

    t.vertex_access().fill().update().for_all([&](auto va) { va.set(prop); });

    permanent_assert(t.commit(), "Add property failed");
}

void add_vertex_property_serial_int(Db &db, PropertyFamily<TypeGroupVertex> &f)
{
    DbAccessor t(db);

    auto key = f.get(Int64::type).family_key();

    size_t i = 0;
    t.vertex_access().fill().update().for_all([&](auto va) mutable {
        va.set(StoredProperty<TypeGroupVertex>(Int64(i), key));
        i++;
    });

    permanent_assert(t.commit(), "Add vertex property serial int failed");
}

void add_edge_property_serial_int(Db &db, PropertyFamily<TypeGroupEdge> &f)
{
    DbAccessor t(db);

    auto key = f.get(Int64::type).family_key();

    size_t i = 0;
    t.edge_access().fill().update().for_all([&](auto va) mutable {
        va.set(StoredProperty<TypeGroupEdge>(Int64(i), key));
        i++;
    });

    permanent_assert(t.commit(), "Add Edge property serial int failed");
}

template <class TG>
size_t size(Db &db, IndexHolder<TG, std::nullptr_t> &h)
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
    permanent_assert(db.graph.vertices.access().size() == 0,
                     "DB isn't empty (vertices)");
    permanent_assert(db.graph.edges.access().size() == 0,
                     "DB isn't empty (edges)");
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
    logging::init_sync();
    logging::log->pipe(std::make_unique<Stdout>());

    SignalHandler::register_handler(Signal::SegmentationFault, []() {
        log_stacktrace("SegmentationFault signal raised");
        std::exit(EXIT_FAILURE);
    });

    SignalHandler::register_handler(Signal::BusError, []() {
        log_stacktrace("Bus error signal raised");
        std::exit(EXIT_FAILURE);
    });

    size_t cvl_n = 1;

    std::string create_vertex_label =
        "CREATE (n:LABEL {name: \"cleaner_test\"}) RETURN n";
    std::string create_vertex_other =
        "CREATE (n:OTHER {name: \"cleaner_test\"}) RETURN n";
    std::string delete_label_vertices = "MATCH (n:LABEL) DELETE n";
    std::string delete_all_vertices   = "MATCH (n) DELETE n";

    IndexDefinition vertex_property_nonunique_unordered = {
        IndexLocation{VertexSide, Option<std::string>("prop"),
                      Option<std::string>(), Option<std::string>()},
        IndexType{false, None}};
    IndexDefinition edge_property_nonunique_unordered = {
        IndexLocation{EdgeSide, Option<std::string>("prop"),
                      Option<std::string>(), Option<std::string>()},
        IndexType{false, None}};
    IndexDefinition edge_property_unique_ordered = {
        IndexLocation{EdgeSide, Option<std::string>("prop"),
                      Option<std::string>(), Option<std::string>()},
        IndexType{true, Ascending}};
    IndexDefinition vertex_property_unique_ordered = {
        IndexLocation{VertexSide, Option<std::string>("prop"),
                      Option<std::string>(), Option<std::string>()},
        IndexType{true, Ascending}};

    // ******************************* TEST 1 ********************************//
    {
        logging::info("TEST 1");
        // add indexes
        // add vertices LABEL
        // add edges
        // add vertices property
        // assert index size.
        Db db("index", false);
        permanent_assert(
            db.indexes().add_index(vertex_property_nonunique_unordered),
            "Add vertex index failed");
        permanent_assert(
            db.indexes().add_index(edge_property_nonunique_unordered),
            "Add edge index failed");

        run(cvl_n, create_vertex_label, db);
        auto sp = StoredProperty<TypeGroupVertex>(
            Int64(0), db.graph.vertices.property_family_find_or_create("prop")
                          .get(Int64::type)
                          .family_key());
        add_property(db, sp);

        permanent_assert(
            cvl_n == size(db, db.graph.vertices
                                  .property_family_find_or_create("prop")
                                  .index),
            "Create vertex property failed");

        add_edge(cvl_n, db);
        add_edge_property_serial_int(
            db, db.graph.edges.property_family_find_or_create("prop"));

        permanent_assert(
            cvl_n ==
                size(db, db.graph.edges.property_family_find_or_create("prop")
                             .index),
            "Create edge property failed");
    }

    // TODO: more tests

    return 0;
}
