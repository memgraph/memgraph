#ifndef MEMGRAPH_API_RESOURCES_NODE_HPP
#define MEMGRAPH_API_RESOURCES_NODE_HPP

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
            // VertexRecord vertex;
            // auto& transaction = db->tx_engine.begin();
            // auto accessor = vertex.access(transaction);
            // auto node = accessor.insert();

            // // TODO: req.json can be empty
            // // probably there is some other place to handle
            // // emptiness of req.json

            // // first version
            // //
            // // for(key, value in body)
            // //     node->properties[key] = value;
            // for(auto it = req.json.MemberBegin(); it != req.json.MemberEnd(); ++it)
            // {
            //     vertex->data.props.set<String>(it->name.GetString(), it->value.GetString());
            // }

            // transaction.commit();

            // return the node we created so we can send it as a response body
            //return node;
            return nullptr;
        }, 
        [&req, &res](Vertex* node) {
            return res.send("TODO");
            // return res.send(properties_to_string(node));
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
            // // read id param
            // Id id(std::stoull(req.params[0])); 
            // // TODO: transaction?
            // return db->graph.find_vertex(id);
            return nullptr;
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
