#ifndef MEMGRAPH_SPEEDY_INL
#define MEMGRAPH_SPEEDY_INL

#include "speedy.hpp"

namespace speedy
{

Speedy::Speedy(uv::UvLoop& loop, const http::Ipv4& ip) : server(loop), ip(ip)
{
    n = r3_tree_create(100);
}

void Speedy::get(const std::string &path, http::request_cb_t callback)
{
    // this->callbacks.push_back(callback);
    // r3_tree_insert_pathl(n, path.c_str(), path.size(), &callbacks[callbacks.size() - 1]);
    r3_tree_insert_pathl(n, path.c_str(), path.size(), &callback);
    // r3_tree_insert_pathl(n, path.c_str(), path.size(), &callbacks.back());
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
        node *n = r3_tree_matchl(this->n, url.c_str(), url.size(), NULL);
        if (n) {
            auto callback = *reinterpret_cast<http::request_cb_t*>(n->data);
             callback(req, res);
        } else {
            res.send("Not found");
        }
    });

    std::cout << "Server is UP" << std::endl;
}

}

#endif
