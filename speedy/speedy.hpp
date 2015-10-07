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

namespace speedy
{

class Speedy
{
public:
    using sptr = std::shared_ptr<Speedy>;

    Speedy(uv::UvLoop& loop, size_t capacity = 100)
        : server(loop), router(capacity) {}

    Speedy(Speedy&) = delete;
    Speedy(Speedy&&) = delete;

    void get(const std::string& path, http::request_cb_t cb)
    {
        router.insert(R3::Method::GET, path, cb);
    }

    void post(const std::string& path, http::request_cb_t cb)
    {
        router.insert(R3::Method::POST, path, cb);
    }

    void put(const std::string& path, http::request_cb_t cb)
    {
        router.insert(R3::Method::PUT, path, cb);
    }

    void del(const std::string& path, http::request_cb_t cb)
    {
        router.insert(R3::Method::DELETE, path, cb);
    }

    void listen(const http::Ipv4& ip)
    {
        server.listen(ip, [this](http::Request& req, http::Response& res) {
            auto route = router.match(R3::to_r3_method(req.method), req.url);
            
            if(!route.exists())
                return res.send(http::Status::NotFound, "Resource not found");

            route(req, res);
        });
    }

private:
    http::HttpServer server;
    R3 router;
};

}

#endif
