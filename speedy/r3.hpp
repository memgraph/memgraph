#ifndef MEMGRAPH_SPEEDY_R3_HPP
#define MEMGRAPH_SPEEDY_R3_HPP

#include <list>
#include <cassert>

#include "http/http.hpp"

namespace r3 {
#include "r3_include.h"
}

namespace speedy
{

class R3
{
public:
    enum Method : int
    {
        GET    = METHOD_GET,
        POST   = METHOD_POST,
        PUT    = METHOD_PUT,
        DELETE = METHOD_DELETE,
        HEAD   = METHOD_HEAD
    };

    static Method to_r3_method(http::Method method)
    {
        switch (method)
        {
            case http::Method::GET:    return Method::GET;
            case http::Method::POST:   return Method::POST;
            case http::Method::PUT:    return Method::PUT;
            case http::Method::DELETE: return Method::DELETE;
            case http::Method::HEAD:   return Method::HEAD;
        }
    }

private:
    class MatchEntry
    {
    public:
        MatchEntry(Method method, const std::string& path)
        {
            entry = r3::match_entry_createl(path.c_str(), path.size());
            entry->request_method = method;
        }

        ~MatchEntry()
        {
            r3::match_entry_free(entry);
        }

        operator r3::match_entry*()
        {
            return entry;
        }

    private:
        r3::match_entry* entry;
        r3::route* route;
    };

public:
    class Route
    {
    public:
        Route(r3::route* route) : route(route) {}

        bool exists() const
        {
            return route != nullptr;
        }

        void operator()(http::Request& req, http::Response& res)
        {
            assert(route != nullptr);

            auto cb = reinterpret_cast<http::request_cb_t*>(route->data);
            cb->operator()(req, res);
        }

    private:
        r3::route* route;
    };

    R3(size_t capacity)
    {
        root = r3::r3_tree_create(capacity);
    }

    ~R3()
    {
        r3::r3_tree_free(root);
    }

    R3(R3&) = delete;
    
    R3(R3&& other)
    {
        this->root = other.root;
        other.root = nullptr;
    }

    void insert(Method method, const std::string& path, http::request_cb_t cb)
    {
        routes.push_back(cb);

        r3::r3_tree_insert_routel(root, method, path.c_str(), path.size(),
            &routes.back());
    }

    Route match(Method method, const std::string& path)
    {
        auto entry = MatchEntry(method, path);
        return Route(r3::r3_tree_match_route(root, entry));
    }

private:
    std::list<http::request_cb_t> routes;
    r3::node* root;
};

}

#endif
