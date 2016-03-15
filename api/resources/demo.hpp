#pragma once

#include <unordered_map>

#include "api/restful/resource.hpp"
#include "query_engine/query_stripper.hpp"
#include "query_engine/query_stripped.hpp"
#include "database/db.hpp"
#include "utils/hashing/fnv.hpp"
#include "threading/task.hpp"
#include "storage/model/properties/all.hpp"

#pragma url /transaction/commit
class Demo : public Resource<Demo, POST>
{
public:
    QueryStripper<int, int, int, int> stripper;
    std::map<uint64_t, std::function<std::string(const code_args_t&)>> query_f;

    Demo(Task::sptr task, Db::sptr db) :
        Resource(task, db),
        stripper(make_query_stripper(TK_INT, TK_FLOAT, TK_STR, TK_BOOL))
    {
        query_f[7130961997552177283] = [db](const code_args_t& args) {
            /* std::cout << "PRVI" << std::endl; */
            auto& t = db->tx_engine.begin();
            /* auto vertex_accessor = db->graph.vertices.insert(t); */
            /* vertex_accessor.property( */
            /*     "id", args[0] */
            /* ); */
            t.commit();
            return "EXECUTED: CREATE (n{id:X}) RETURN n";
        };

        query_f[11198568396549106428ull] = [db](const code_args_t& args) {
            /* std::cout << "DRUGI" << std::endl; */
            auto& t = db->tx_engine.begin();
            auto id = args[0]->as<Int32>();
            auto vertex_accessor = db->graph.vertices.find(t, id.value);
            t.commit();
            return "EXECUTED: MATCH (n{id:X}) RETURN n";
        };

        query_f[11637140396624918705ull] = [db](const code_args_t& args) {
            /* std::cout << "TRECI" << std::endl; */
            auto& t = db->tx_engine.begin();
            auto id = args[0]->as<Int32>();
            auto vertex_accessor = db->graph.vertices.find(t, id.value);
            if (!vertex_accessor) {
                t.commit();
                return "FAIL TO FIND NODE";
            }
            vertex_accessor.property(
                "prop", args[1]
            );
            t.commit();
            return "EXECUTED: MATCH (n{id:X}) SET n.prop=Y RETURN n";
        };

        query_f[784140140862470291ull] = [db](const code_args_t& args) {
            /* std::cout << "CETVRTI" << std::endl; */
            auto& t = db->tx_engine.begin();
            auto id1 = args[0]->as<Int32>();
            auto v1 = db->graph.vertices.find(t, id1.value);

            if (!v1) {
                t.commit();
                return "FAIL TO FIND NODE";
            }
            auto id2 = args[1]->as<Int32>();
            auto v2 = db->graph.vertices.find(t, id2.value);
            if (!v2) {
                t.commit();
                return "FAIL TO FIND NODE";
            }
            auto edge_accessor = db->graph.edges.insert(t);


            return t.commit(), "AAA";

            v1.vlist->access(t).update()->data.out.add(edge_accessor.vlist);
            v2.vlist->access(t).update()->data.in.add(edge_accessor.vlist);

            edge_accessor.from(v1.vlist);
            edge_accessor.to(v2.vlist);
            edge_accessor.edge_type(EdgeType("test"));

            t.commit();
            return "EXECUTED: EDGE CREATED";
        };

        query_f[16940444920835972350ull] = [db](const code_args_t& args) {
            /* std::cout << "PETI" << std::endl; */
            auto& t = db->tx_engine.begin();
            auto id = args[0]->as<Int32>();
            auto v = db->graph.vertices.find(t, id.value);
            t.commit();
            return "EXECUTED: QUERY: MATCH (n{id:X})-[r]->(m) RETURN count(r)";
        };
    }

    int x{0};

    void post(sp::Request& req, sp::Response& res)
    {
        task->run([this, &req]() {
            //return res.send(http::Status::Ok, "alo");
            auto query = req.json["statements"][0]["statement"].GetString();
            auto strip = stripper.strip(query);
            auto hash = fnv(strip.query);

            /* std::cout << "'" << query << "'" << std::endl; */

            auto it = query_f.find(hash);

            if(it == query_f.end())
            {
                std::cout << "NOT FOUND" << std::endl;
                std::cout << query << std::endl;
                std::cout << hash << std::endl;
                return std::string("NOT FOUND");
            }

            return it->second(strip.arguments);
        },
        [&req, &res](std::string str) {
            return res.send(http::Status::Ok, str);
        });
    }
};
