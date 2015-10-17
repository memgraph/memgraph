/** @file speedy.hpp
 *  @brief Speedy - Cpp Web Application Framework
 *
 *  Blazingly fast web application framework. Designed like
 *  http://expressjs.com, one of its mail goal is also simple usage.
 *
 *  @author Dominik Tomicevic (domko)
 *  @author Marko Budiselic (buda)
 */
#ifndef MEMGRAPH_SPEEDY_SPEEDY_HPP
#define MEMGRAPH_SPEEDY_SPEEDY_HPP

#include "io/uv/uv.hpp"
#include "http/http.hpp"
#include "r3.hpp"

#include "request.hpp"
#include "response.hpp"

#include "middleware.hpp"
#include "rapidjson_middleware.hpp"

namespace sp
{

class Speedy
{
public:
    using sptr = std::shared_ptr<Speedy>;

    using server_t = http::HttpServer<Request, Response>;
    using request_cb_t = server_t::request_cb_t;

    Speedy(uv::UvLoop::sptr loop, const std::string& prefix = "",
           size_t capacity = 100)
        : server(*loop), prefix(std::move(prefix)), router(capacity)
    {
        use(rapidjson_middleware);
    }

    Speedy(const Speedy&) = delete;
    Speedy(Speedy&&) = delete;

    void use(Middlewares::middleware_cb_t cb)
    {
        middlewares.push_back(cb);
    }

    void get(const std::string& path, server_t::request_cb_t cb)
    {
        router.insert(R3::Method::GET, join(prefix, path), cb);
    }

    void post(const std::string& path, server_t::request_cb_t cb)
    {
        router.insert(R3::Method::POST, join(prefix, path), cb);
    }

    void put(const std::string& path, server_t::request_cb_t cb)
    {
        router.insert(R3::Method::PUT, join(prefix, path), cb);
    }

    void del(const std::string& path, server_t::request_cb_t cb)
    {
        router.insert(R3::Method::DELETE, join(prefix, path), cb);
    }

    void listen(const http::Ipv4& ip)
    {
        router.compile();

        server.listen(ip, [this](Request& req, Response& res) {
            auto route = router.match(R3::to_r3_method(req.method), req.url);
            
            if(!route.exists())
                return res.send(http::Status::NotFound, "Resource not found");

            // parse url params
            route.populate(req);

            // run middlewares
            auto result = middlewares.run(req, res);

            // if they signaled false, stop the further execution
            if(!result)
                return;

            route(req, res);
        });
    }

private:
    server_t server;
    std::string prefix;
    R3 router;
    Middlewares middlewares;

    std::string join(const std::string& prefix, const std::string& path)
    {
        // check if prefix has a trailing /
        if(prefix.back() == '/')
        {
            // prefix has a / on the end so remove the / from the path
            // if it has it too. e.g  /db/data/ + /node = /db/data/node
            if(path.front() == '/')
                return prefix + path.substr(1);

            // there is only one / so it's ok to concat them
            return prefix + path;
        }

        // the prefix doesn't have a slash
        if(path.front() == '/')
            // the path has though, so it's ok to concat them
            return prefix + path;

        // neither the prefix or the path have a / so we need to add it
        return prefix + '/' + path;
    }
};

}

#endif
