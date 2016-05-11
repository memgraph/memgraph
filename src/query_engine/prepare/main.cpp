#include "database/db.hpp"
#include "query_engine/query_stripper.hpp"
#include "utils/hashing/fnv.hpp"
#include "storage/model/properties/all.hpp"
#include "query_engine/query_result.hpp"
#define DEBUG
#include "query_engine/debug.hpp"

using std::cout;
using std::endl;

int main()
{
    Db db;

    std::vector<std::string> queries = {{
        std::string("CREATE (n{id:3}) RETURN n"),
        std::string("MATCH  (n{id:3}) RETURN n"),
        std::string("MATCH  (n{id:3}) SET n.prop = \"\" RETURN n"),
        std::string("MATCH  (n{id:3}) DELETE n"),
        std::string("MATCH  (n{id:3}),(m{id:2}) CREATE (n)-[r:test]->(m) RETURN r"),
        std::string("MATCH  (n{id:3})-[r]->(m) RETURN count(r)")
    }};

    auto stripper = make_query_stripper(TK_INT, TK_FLOAT, TK_STR, TK_BOOL);

    for(auto query : queries) {
        auto strip = stripper.strip(query).query;
        auto hash = std::to_string(fnv(strip));
        cout << "QUERY: " << query <<
                "    STRIP: " << strip <<
                "    HASH: " << hash << endl;
    }

    auto create_node = [&db](uint64_t id) {
        auto& t = db.tx_engine.begin();
        auto vertex_accessor = db.graph.vertices.insert(t);
        vertex_accessor.property<Int64>(
            "id", Int64(id)
        );
        t.commit();
        return vertex_accessor;
    };

    auto find_node = [&db](uint64_t id) {
        auto& t = db.tx_engine.begin();
        auto vertex_accessor = db.graph.vertices.find(t, id);
        t.commit();
        return vertex_accessor;
    };

    auto edit_node = [&db](uint64_t id, std::string prop) {
        auto& t = db.tx_engine.begin();
        auto vertex_accessor = db.graph.vertices.find(t, id);
        if (!vertex_accessor) {
            t.commit();
            return vertex_accessor;
        }
        vertex_accessor.property<String>(
            "prop", String(prop)
        );
        t.commit();
        return vertex_accessor;
    };

    auto delete_node = [&db](uint64_t id) {
        auto& t = db.tx_engine.begin();
        auto vertex_accessor = db.graph.vertices.find(t, id);
        if (!vertex_accessor)
            return vertex_accessor;
        vertex_accessor.remove(t);
        t.commit();
        return vertex_accessor;
    };

    auto create_edge = [&db](uint64_t id1, uint64_t id2, std::string type) {
        auto& t = db.tx_engine.begin();
        auto v1 = db.graph.vertices.find(t, id1);
        if (!v1)
            return Edge::Accessor();
        auto v2 = db.graph.vertices.find(t, id2);
        if (!v2)
            return Edge::Accessor();
        auto edge_accessor = db.graph.edges.insert(t);
        v1.vlist->access(t).update()->data.out.add(edge_accessor.vlist);
        v2.vlist->access(t).update()->data.in.add(edge_accessor.vlist);
        edge_accessor.from(v1.vlist);
        edge_accessor.to(v2.vlist);
        edge_accessor.edge_type(EdgeType(type));
        t.commit();
        return edge_accessor;
    };

    auto vertex_out_degree = [&db](uint64_t id) {
        auto& t = db.tx_engine.begin();
        auto v = db.graph.vertices.find(t, id);
        t.commit();
        return v.out_degree();
    };

    auto created_node1 = create_node(100);
    PRINT_PROPS(created_node1.properties());
    auto created_node2 = create_node(200);
    PRINT_PROPS(created_node2.properties());
    auto found_node = find_node(0);
    PRINT_PROPS(found_node.properties());
    auto edited_node = edit_node(0, "test");
    PRINT_PROPS(edited_node.properties());
    // delete_node(0);
    if (!find_node(0))
        cout << "node doesn't exist" << endl;
    auto edge_accessor = create_edge(0, 1, "test");
    if (edge_accessor)
        cout << edge_accessor.record->data.edge_type << endl;
    cout << vertex_out_degree(0) << endl;

    return 0;
}
