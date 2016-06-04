#pragma once

#include <list>
#include <cassert>

#include "request.hpp"
#include "response.hpp"

#include "utils/auto_scope.hpp"
#include "http/http.hpp"

namespace r3 {
#include "r3_include.h"
}

namespace sp
{

class R3Error : public std::runtime_error
{
public:
    using runtime_error::runtime_error;
};

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

public:
    class Route
    {
    public:
        Route(r3::node* node, Method method, const std::string& path)
        {
            entry = r3::match_entry_createl(path.c_str(), path.size());
            entry->request_method = method;
            route = r3::r3_tree_match_route(node, entry);
        }

        ~Route()
        {
            r3::match_entry_free(entry);
        }

        operator r3::match_entry*()
        {
            return entry;
        }

        bool exists() const
        {
            return route != nullptr;
        }

        void populate(sp::Request& req)
        {
            req.params.clear();
            for(int i = 0; i < entry->vars->len; ++i)
                req.params.emplace_back(entry->vars->tokens[i]);
        }

        void operator()(sp::Request& req, sp::Response& res)
        {
            assert(route != nullptr);

            auto cb = reinterpret_cast<sp::request_cb_t*>(route->data);
            cb->operator()(req, res);
        }

    private:
        r3::match_entry* entry;
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
        this->routes = std::move(other.routes);
        this->root = other.root;
        other.root = nullptr;
    }

    void insert(Method method, const std::string& path, sp::request_cb_t cb)
    {
        routes.push_back(cb);

        r3::r3_tree_insert_routel(root, method, path.c_str(), path.size(),
            &routes.back());
    }

    Route match(Method method, const std::string& path)
    {
        return Route(root, method, path);
    }

    void compile()
    {
        char* error_string = nullptr;
        bool error = r3_tree_compile(root, &error_string);

        if(!error)
            return;

        Auto(free(error_string));

        throw R3Error(std::string(error_string));
    }

private:
    std::list<sp::request_cb_t> routes;
    r3::node* root;
};

}
