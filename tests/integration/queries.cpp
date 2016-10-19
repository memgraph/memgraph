#include "query_engine/hardcode/queries.hpp"

#ifdef BARRIER
#include "barrier/barrier.cpp"
#endif

#include "communication/bolt/v1/serialization/bolt_serializer.hpp"
#include "database/db.hpp"
#include "logging/default.hpp"
#include "logging/streams/stdout.hpp"
#include "query_engine/query_stripper.hpp"

int main(void)
{
    logging::init_async();
    logging::log->pipe(std::make_unique<Stdout>());

    Db db;

#ifdef BARRIER
    auto query_functions = load_queries(barrier::trans(db));
#else
    auto query_functions = load_queries(db);
#endif

    auto stripper = make_query_stripper(TK_LONG, TK_FLOAT, TK_STR, TK_BOOL);

    // TODO: put all queries into a file
    std::vector<std::string> queries = {

        // CREATE and MATCH by label and property
        "CREATE (n:LABEL {name: \"TEST01\"}) RETURN n",
        "CREATE (n:LABEL {name: \"TEST02\"}) RETURN n",
        // "MATCH (n:LABEL {name: \"TEST01\"}) RETURN n",

        "CREATE (n:LABEL {name: \"TEST2\"}) RETURN n",
        "CREATE (n:LABEL {name: \"TEST3\"}) RETURN n",
        "CREATE (n:OTHER {name: \"TEST4\"}) RETURN n"
        "CREATE (n:ACCOUNT {id: 2322, name: \"TEST\", country: \"Croatia\", created_at: 2352352}) RETURN n",
        "MATCH (n {id: 0}) RETURN n",
        "MATCH (n {id: 1}) RETURN n",
        "MATCH (n {id: 2}) RETURN n",
        "MATCH (n {id: 3}) RETURN n",
        "MATCH (a {id:0}), (p {id: 1}) CREATE (a)-[r:IS]->(p) RETURN r",
        "MATCH (a {id:1}), (p {id: 2}) CREATE (a)-[r:IS]->(p) RETURN r",
        "MATCH ()-[r]-() WHERE ID(r)=0 RETURN r",
        "MATCH ()-[r]-() WHERE ID(r)=1 RETURN r",
        "MATCH (n: {id: 0}) SET n.name = \"TEST100\" RETURN n",
        "MATCH (n: {id: 1}) SET n.name = \"TEST101\" RETURN n",
        "MATCH (n: {id: 0}) SET n.name = \"TEST102\" RETURN n",
        "MATCH (n:LABEL) RETURN n",
        "MATCH (n1), (n2) WHERE ID(n1)=0 AND ID(n2)=1 CREATE (n1)<-[r:IS {age: 25,weight: 70}]-(n2) RETURN r",
        "MATCH (n) RETURN n", "MATCH (n:LABEL) RETURN n", "MATCH (n) DELETE n ",
        "MATCH (n:LABEL) DELETE n", "MATCH (n) WHERE ID(n) = 0 DELETE n",
        "MATCH ()-[r]-() WHERE ID(r) = 0 DELETE r", "MATCH ()-[r]-() DELETE r",
        "MATCH ()-[r:TYPE]-() DELETE r",
        "MATCH (n)-[:TYPE]->(m) WHERE ID(n) = 0 RETURN m",
        "MATCH (n)-[:TYPE]->(m) WHERE n.name = \"kruno\" RETURN m",
        "MATCH (n)-[:TYPE]->(m) WHERE n.name = \"kruno\" RETURN n,m",
        "MATCH (n:LABEL)-[:TYPE]->(m) RETURN n"

        // CREATE and MATCH multiple labels and properties
        // "CREATE (n:LABEL1:LABEL2 {name: \"TEST01\", age: 20}) RETURN n",
        // "MATCH (n:LABEL1:LABEL2 {name: \"TEST01\", age: 20}) RETURN n"
    };

    for (auto &query : queries) {
        auto stripped = stripper.strip(query);
        std::cout << "Query hash: " << stripped.hash << std::endl;
        auto result =
            query_functions[stripped.hash](std::move(stripped.arguments));
        permanent_assert(result == true,
                         "Result retured from query function is not true");
    }

    return 0;
}
