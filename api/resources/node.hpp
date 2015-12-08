#pragma once

#include <random>

#include "api/restful/resource.hpp"
#include "mvcc/version_list.hpp"
#include "debug/log.hpp"

#pragma url /node
class Nodes : public Resource<Nodes, POST>
{
public:
    using Resource::Resource;
        
    void post(sp::Request& req, sp::Response& res)
    {
        task->run([this, &req]() {
            // create transaction
            auto& transaction = db->tx_engine.begin();

            // insert a new vertex
            auto vertex = db->graph.vertex_store.insert(transaction);

            // map fields
            for(auto it = req.json.MemberBegin(); it != req.json.MemberEnd(); ++it)
            {
                 vertex->data.props.set<String>(it->name.GetString(), it->value.GetString());
            }

            transaction.commit();

            return vertex;
        }, 
        [&req, &res](Vertex* node) {
            return res.send(properties_to_string(node));
        });
    }
};

#pragma url /node/{id:\\d+}
class Node : public Resource<Node, GET, PUT, DELETE>
{
public:
    using Resource::Resource;
        
    void get(sp::Request& req, sp::Response& res)
    {
        task->run([this, &req]() {
            // create transaction
            auto& transaction = db->tx_engine.begin();

            // read id param
            Id id(std::stoull(req.params[0])); 

            // find node
            auto vertex = db->graph.vertex_store.find(transaction, id);

            transaction.commit();

            return vertex;
        },
        [&req, &res](const Vertex* node) {
            if (node == nullptr) {
                return res.send(http::Status::NotFound, "The node was not found");
            }
            return res.send(properties_to_string(node));
        });
    }

    void put(sp::Request& req, sp::Response& res)
    {
        return res.send("TODO");
    }

    void del(sp::Request& req, sp::Response& res)
    {
        return res.send("TODO");
    }
};
