#include "query_engine/hardcode/queries.hpp"
#include "storage/edges.cpp"
#include "storage/edges.hpp"
#include "storage/vertices.cpp"
#include "storage/vertices.hpp"
#include "utils/assert.hpp"

int main(void)
{
    Db db;

    auto query_functions = load_queries(db);

    auto stripper = make_query_stripper(TK_LONG, TK_FLOAT, TK_STR, TK_BOOL);

    // TODO: put all queries into a file
    std::vector<std::string> queries = {
        "CREATE (n:LABEL {name: \"TEST1\"}) RETURN n",
        "CREATE (n:LABEL {name: \"TEST2\"}) RETURN n",
        "CREATE (n:LABEL {name: \"TEST3\"}) RETURN n",
        "CREATE (n:ACCOUNT {id: 2322, name: \"TEST\", country: \"Croatia\", "
        "created_at: 2352352}) RETURN n",
        "MATCH (n {id: 0}) RETURN n", "MATCH (n {id: 1}) RETURN n",
        "MATCH (n {id: 2}) RETURN n", "MATCH (n {id: 3}) RETURN n",
        "MATCH (a {id:0}), (p {id: 1}) CREATE (a)-[r:IS]->(p) RETURN r",
        "MATCH (a {id:1}), (p {id: 2}) CREATE (a)-[r:IS]->(p) RETURN r",
        "MATCH ()-[r]-() WHERE ID(r)=0 RETURN r",
        "MATCH ()-[r]-() WHERE ID(r)=1 RETURN r",
        "MATCH (n: {id: 0}) SET n.name = \"TEST100\" RETURN n",
        "MATCH (n: {id: 1}) SET n.name = \"TEST101\" RETURN n",
        "MATCH (n: {id: 0}) SET n.name = \"TEST102\" RETURN n",
        "MATCH (n:LABEL) RETURN n"};

    for (auto &query : queries) {
        auto stripped = stripper.strip(query);
        auto result = query_functions[stripped.hash](stripped.arguments);
        permanent_assert(result == true,
                         "Result retured from query function is not true");
    }

    return 0;
}
