#ifndef MEMGRAPH_ANIMAL_HPP
#define MEMGRAPH_ANIMAL_HPP

#include "speedy/speedy.hpp"
#include "api/restful/resource.hpp"

class Animal : public api::Resource<Animal, api::GET, api::POST>
{
public:
    Animal(speedy::Speedy& app) : Resource(app, "/animal") {}

    void get(http::Request& req, http::Response& res)
    {
        return res.send("Ok, here is a Dog");
    }

    void post(http::Request& req, http::Response& res)
    {
        return res.send("Oh, you gave me an animal?");
    }
};

#endif
