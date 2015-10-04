/** @file speedy.hpp
 *  @brief Speedy - Cpp Web Application Framework
 *
 *  Blazingly fast web application framework. Designed like
 *  http://expressjs.com, one of its mail goal is also simple usage.
 *
 *  @author Dominik Tomicevic (domko)
 *  @author Marko Budiselic (buda)
 */
#ifndef MEMGRAPH_SPEEDY_HPP
#define MEMGRAPH_SPEEDY_HPP

#include <vector>
#include "io/uv/uv.hpp"
#include "http/http.hpp"
#include "r3_include.h"

namespace speedy
{

typedef unsigned int uint;

class Speedy
{
private:
    /*
     * http server instance that contains all logic related
     * to the http protocol
     */
    http::HttpServer server;

    /*
     * ip address of the server
     */
    http::Ipv4 ip;

    /*
     * root node of r3 decision tree
     */
    node *n;

    /*
     * callbacks container 
     */
    std::vector<http::request_cb_t> callbacks;

    /** @brief Store a http callback.
     *
     *  Every callback for now has receiving method and path (url).
     *  So, the implementation of this method saves callback for 
     *  the method and the path.
     *
     *  @param method int http defined method
     *  @param path std::string path (url)
     *  @param callback http::request_cb_t callback which will be called
     *  on a http request
     */
    void store_callback(int method,
        const std::string &path,
        http::request_cb_t callback);
public:
    Speedy(uv::UvLoop& loop, const http::Ipv4& ip);
    void get(const std::string &path, http::request_cb_t callback);
    void post(const std::string &path, http::request_cb_t callback);
    void put(const std::string &path, http::request_cb_t callback);
    void del(const std::string &path, http::request_cb_t callback);
    void listen();
};

}

#include "speedy.inl"

#endif
