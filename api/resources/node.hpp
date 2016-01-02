#pragma once

#include <random>

#include "debug/log.hpp"
#include "mvcc/version_list.hpp"
#include "api/response_json.hpp"
#include "api/restful/resource.hpp"
#include "storage/model/properties/property.hpp"

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
            auto vertex_accessor = db->graph.vertices.insert(transaction);

            auto begin_it = req.json.MemberBegin();
            auto end_it = req.json.MemberEnd();
            for(auto it = begin_it; it != end_it; ++it)
            {
                vertex_accessor.template property<String>(
                    it->name.GetString(), it->value.GetString()
                );
            }
            
            // commit the transaction
            transaction.commit();

            return std::move(vertex_accessor);
        }, 
        [&req, &res](Vertex::Accessor&& vertex_accessor) {
            return res.send(
                http::Status::Created,
                vertex_create_response(vertex_accessor)
            );
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
            auto vertex_accessor = db->graph.vertices.find(transaction, id);

            // commit the transaction
            transaction.commit();

            return std::move(vertex_accessor);
        },
        [&req, &res](Vertex::Accessor&& vertex_accessor) {
            if (vertex_accessor.empty()) {
                return res.send(http::Status::NotFound, "The node was not found");
            }
            return res.send(
                vertex_create_response(vertex_accessor)
            );
        });
    }

    void put(sp::Request& req, sp::Response& res)
    {
        // task->run([this, &req]() -> Vertex* {
        //      // create transaction
        //      auto& transaction = db->tx_engine.begin();
 
        //      // read id param
        //      Id id(std::stoull(req.params[0])); 
 
        //      // find node
        //      auto vertex = db->graph.vertices.update(transaction, id);
 
        //      if (vertex == nullptr)
        //          return nullptr;
 
        //      // map fields
        //      for(auto it = req.json.MemberBegin(); it != req.json.MemberEnd(); ++it)
        //      {
        //           vertex->data.props.set<String>(it->name.GetString(), it->value.GetString());
        //      }
        //      
        //      // commit the transaction
        //      transaction.commit();
 
        //      return vertex;
        // }, 
        // [&req, &res](Vertex* vertex) {
        //     if (vertex == nullptr) {
        //         return res.send(http::Status::NotFound, "The node was not found");
        //     }
        //     return res.send(vertex_props_to_string(vertex));
        // });
    }

    void del(sp::Request& req, sp::Response& res)
    {
         // task->run([this, &req]() -> bool {
         //     // create transaction
         //     auto& transaction = db->tx_engine.begin();
 
         //     // read id param
         //     Id id(std::stoull(req.params[0]));

         //     auto is_deleted = db->graph.vertices.remove(transaction, id);

         //     // commit the transaction
         //     transaction.commit();

         //     return is_deleted;
         // },
         // [&req, &res](bool is_deleted) {
         //    if (is_deleted)
         //        return res.send(http::Status::Ok, "The node was deleted");

         //    return res.send(http::Status::NotFound, "The node was not found");
         // });
    }
};
