#ifndef MEMGRAPH_SPEEDY_INL
#define MEMGRAPH_SPEEDY_INL

#include "speedy.hpp"
#include <http_parser.h>

namespace speedy
{

int r3_request_method(http::Method method)
{
    switch (method) {
        case http::Method::GET: return METHOD_GET;
        case http::Method::POST: return METHOD_POST;
        case http::Method::PUT: return METHOD_PUT;
        case http::Method::DELETE: return METHOD_DELETE;
        case http::Method::HEAD: return METHOD_HEAD;
    }
}

// TODO: better implementation

Speedy::Speedy(uv::UvLoop& loop, const http::Ipv4& ip) : server(loop), ip(ip)
{
    n = r3_tree_create(100);
}

void Speedy::store_index(int method, const std::string &path)
{
    void *ptr = malloc(sizeof(uint));
    *((uint *)ptr) = callbacks.size() - 1;
    r3_tree_insert_routel(n, method, path.c_str(), path.size(), ptr);
}

void Speedy::get(const std::string &path, http::request_cb_t callback)
{
    callbacks.push_back(callback);
    store_index(METHOD_GET, path);

    // TODO: something like this
    // this solution doesn't work, currenlty I don't know why
    // r3_tree_insert_pathl(n, path.c_str(), path.size(), &callbacks.back());
}

void Speedy::post(const std::string &path, http::request_cb_t callback)
{
    callbacks.push_back(callback);
    store_index(METHOD_POST, path);
}

void Speedy::put(const std::string &path, http::request_cb_t callback)
{
    callbacks.push_back(callback);
    store_index(METHOD_PUT, path);
}

void Speedy::del(const std::string &path, http::request_cb_t callback)
{
    callbacks.push_back(callback);
    store_index(METHOD_DELETE, path);
}

void Speedy::listen()
{
    char *errstr = NULL;
    int err = r3_tree_compile(n, &errstr);
    if (err) {
        std::cout << "R3 compile error" << std::endl;
    }

    server.listen(ip, [this](http::Request& req, http::Response& res) {
        auto url = req.url;
        auto c_url = url.c_str();
        match_entry *entry = match_entry_create(c_url);
        entry->request_method = r3_request_method(req.method);
        route *r = r3_tree_match_route(this->n, entry);
        match_entry_free(entry);
        if (r) {
            int index = *((int *)r->data);
            auto callback = this->callbacks[index];
            callback(req, res);
            // TODO: and something like this
            // auto callback = *reinterpret_cast<http::request_cb_t*>(n->data);
            // callback(req, res);
        } else {
            res.send("Not found");
        }
    });

    std::cout << "Server is UP" << std::endl;
}

}

#endif
