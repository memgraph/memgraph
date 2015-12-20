#pragma once

#include <random>

#include "api/restful/resource.hpp"
#include "mvcc/version_list.hpp"
#include "debug/log.hpp"
#include "api/response_json.hpp"

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
            auto vertex_proxy = db->graph.vertices.insert(transaction);

            // map fields
            for(auto it = req.json.MemberBegin(); it != req.json.MemberEnd(); ++it)
            {
                vertex_proxy.property<std::string, std::string>(
                    it->name.GetString(),
                    it->value.GetString()
                );
            }
            
            // commit the transaction
            transaction.commit();

            return std::move(vertex_proxy);
        }, 
        [&req, &res](VertexProxy&& vertex_proxy) {
            return res.send(http::Status::Created, vertex_create_response(vertex_proxy));
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
            auto vertex = db->graph.vertices.find(transaction, id);

            // commit the transaction
            transaction.commit();

            return vertex;
        },
        [&req, &res](const Vertex* vertex) {
            if (vertex == nullptr) {
                return res.send(http::Status::NotFound, "The node was not found");
            }
            return res.send(vertex_props_to_string(vertex));
        });
    }

    void put(sp::Request& req, sp::Response& res)
    {
        task->run([this, &req]() -> Vertex* {
             // create transaction
             auto& transaction = db->tx_engine.begin();
 
             // read id param
             Id id(std::stoull(req.params[0])); 
 
             // find node
             auto vertex = db->graph.vertices.update(transaction, id);
 
             if (vertex == nullptr)
                 return nullptr;
 
             // map fields
             for(auto it = req.json.MemberBegin(); it != req.json.MemberEnd(); ++it)
             {
                  vertex->data.props.set<String>(it->name.GetString(), it->value.GetString());
             }
             
             // commit the transaction
             transaction.commit();
 
             return vertex;
        }, 
        [&req, &res](Vertex* vertex) {
            if (vertex == nullptr) {
                return res.send(http::Status::NotFound, "The node was not found");
            }
            return res.send(vertex_props_to_string(vertex));
        });
    }

    void del(sp::Request& req, sp::Response& res)
    {
         task->run([this, &req]() -> bool {
             // create transaction
             auto& transaction = db->tx_engine.begin();
 
             // read id param
             Id id(std::stoull(req.params[0]));

             auto is_deleted = db->graph.vertices.remove(transaction, id);

             // commit the transaction
             transaction.commit();

             return is_deleted;
         },
         [&req, &res](bool is_deleted) {
            if (is_deleted)
                return res.send(http::Status::Ok, "The node was deleted");

            return res.send(http::Status::NotFound, "The node was not found");
         });
    }
};
