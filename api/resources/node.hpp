#ifndef MEMGRAPH_API_RESOURCES_NODE_HPP
#define MEMGRAPH_API_RESOURCES_NODE_HPP

#include <random>

#include "api/restful/resource.hpp"
#include "debug/log.hpp"

#pragma url /node
class Nodes : public Resource<Nodes, POST>
{
public:
    using Resource::Resource;
        
    void post(sp::Request& req, sp::Response& res)
    {
        task->run([this, &req]() {
            // start a new transaction and obtain a reference to it
            auto t = db->tx_engine.begin();

            // insert a new vertex in the graph
            auto atom = db->graph.vertices.insert(t);

            // a new version was created and we got an atom along with the
            // first version. obtain a pointer to the first version
            //
            //        nullptr
            //           ^
            //           |
            //      [Vertex  v1]
            //           ^
            //           |
            //      [Atom id=k]   k {1, 2, ...}
            //
            auto node = atom->first();

            // TODO: req.json can be empty
            // probably there is some other place to handle
            // emptiness of req.json

            // first version
            //
            // for(key, value in body)
            //     node->properties[key] = value;
            for(auto it = req.json.MemberBegin(); it != req.json.MemberEnd(); ++it)
            {
                node->properties.emplace<String>(it->name.GetString(), it->value.GetString());
            }

            // commit the transaction
            db->tx_engine.commit(t);

            // return the node we created so we can send it as a response body
            return node;
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
        task->run([this, &req]() -> Vertex* {
            // read id param
            Id id(std::stoull(req.params[0])); 
            // TODO: transaction?
            return db->graph.find_vertex(id);
        },
        [&req, &res](Vertex* node) {
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

#endif
