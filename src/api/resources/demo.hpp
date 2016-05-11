#pragma once

#include <unordered_map>

#include "api/restful/resource.hpp"
#include "query_engine/query_stripper.hpp"
#include "query_engine/query_stripped.hpp"
#include "database/db.hpp"
#include "utils/hashing/fnv.hpp"
#include "threading/task.hpp"
#include "storage/model/properties/all.hpp"

std::vector<std::string> queries {
    "CREATE (n:Item{id:1}) RETURN n",
    "MATCH (n:Item{id:1}),(m:Item{id:1}) CREATE (n)-[r:test]->(m) RETURN r",
    "MATCH (n:Item{id:1}) SET n.prop = 7 RETURN n",
    "MATCH (n:Item{id:1}) RETURN n",
    "MATCH (n:Item{id:1})-[r]->(m) RETURN count(r)"
};

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
        std::vector<uint64_t> hashes;

        for(auto& query : queries)
        {
            auto strip = stripper.strip(query);
            auto hash = fnv(strip.query);

            hashes.push_back(hash);
        }

        query_f[hashes[0]] = [db](const code_args_t& args) {
            auto& t = db->tx_engine.begin();

            auto v = db->graph.vertices.insert(t);
            v.property("id", args[0]);

            t.commit();
            return std::to_string(v.property("id").as<Int32>().value);
        };

        query_f[hashes[1]] = [db](const code_args_t& args) {
            return std::string("ALO");
            auto& t = db->tx_engine.begin();

            auto v1 = db->graph.vertices.find(t, args[0]->as<Int32>().value);

            if(!v1)
                return t.commit(), std::string("not found");

            auto v2 = db->graph.vertices.find(t, args[1]->as<Int32>().value);

            if(!v2)
                return t.commit(), std::string("not found");

            auto e = db->graph.edges.insert(t);

            v1.vlist->access(t).update()->data.out.add(e.vlist);
            v2.vlist->access(t).update()->data.in.add(e.vlist);

            e.from(v1.vlist);
            e.to(v2.vlist);
            e.edge_type(EdgeType("test"));

            t.commit();
            return e.edge_type();
        };

        query_f[hashes[2]] = [db](const code_args_t& args) {
            auto& t = db->tx_engine.begin();

            auto id = args[0]->as<Int32>();
            auto v = db->graph.vertices.find(t, id.value);

            if (!v)
                return t.commit(), std::string("not found");

            v.property("prop", args[1]);

            t.commit();
            return std::to_string(v.property("id").as<Int32>().value);
        };

        query_f[hashes[3]] = [db](const code_args_t& args) {
            auto& t = db->tx_engine.begin();

            auto id = args[0]->as<Int32>();
            auto v = db->graph.vertices.find(t, id.value);

            t.commit();

            if(!v)
                return std::string("not found");

            return std::to_string(v.property("id").as<Int32>().value);
        };

        query_f[hashes[4]] = [db](const code_args_t& args) {
            auto& t = db->tx_engine.begin();

            auto id = args[0]->as<Int32>();
            auto v = db->graph.vertices.find(t, id.value);

            t.commit();

            if(!v)
                return std::string("not found");

            return std::to_string(v.out_degree());
        };

    }

    int x{0};

    void post(sp::Request& req, sp::Response& res)
    {
        task->run([this, &req]() {
            auto query = req.json["statements"][0]["statement"].GetString();
            auto strip = stripper.strip(query);
            auto hash = fnv(strip.query);

            auto it = query_f.find(hash);

            if(it == query_f.end())
            {
                std::cout << "Unrecognized query '"
                          << query << "' with hash "
                          << hash << "." << std::endl;

                return std::string("Unrecognized Query");
            }

            return it->second(strip.arguments);
        },
        [&req, &res](std::string str) {
            return res.send(http::Status::Ok, str);
        });
    }
};
