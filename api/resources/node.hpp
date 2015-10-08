#ifndef MEMGRAPH_API_RESOURCES_NODE_HPP
#define MEMGRAPH_API_RESOURCES_NODE_HPP

#include "api/restful/resource.hpp"

class Node : public Resource<Node, GET, POST>
{
public:
    Node(sp::Speedy& app) : Resource(app, "/node") {}

    void get(sp::Request& req, sp::Response& res)
    {
        return res.send("Ok, here is a Dog");
    }
        
    void post(sp::Request& req, sp::Response& res)
    {
        return res.send("Ok, here is a Dog");
    }
    
    void put(sp::Request& req, sp::Response& res)
    {
        return res.send("Ok, here is a Dog");
    }

    void del(sp::Request& req, sp::Response& res)
    {
        return res.send("Ok, here is a Dog");
    }
};

#endif
