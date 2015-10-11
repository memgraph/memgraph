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

            // TODO read the JSON body and store the properties in the
            // first version
            //
            // for(key, value in body)
            //     node->properties[key] = value;

            // commit the transaction
            db->tx_engine.commit(t);

            // return the node we created so we can send it as a response body
            return node;
        }, 
        [&req, &res](Vertex* node) {
            // make a string buffer
            std::string buffer;

            // dump properties in this buffer
            node->properties.dump(buffer);
            
            // respond to the use with the buffer
            return res.send(buffer);
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
        return res.send(req.url);
    }

    void put(sp::Request& req, sp::Response& res)
    {
        return res.send(req.url);
    }

    void del(sp::Request& req, sp::Response& res)
    {
        return res.send(req.url);
    }
};

#endif
