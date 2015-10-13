#ifndef MEMGRAPH_SPEEDY_MIDDLEWARE_HPP
#define MEMGRAPH_SPEEDY_MIDDLEWARE_HPP

#include "request.hpp"
#include "response.hpp"

namespace sp
{

class Middlewares
{
public:
    using middleware_cb_t = std::function<bool(sp::Request&, sp::Response&)>;

    bool run(sp::Request& req, sp::Response& res)
    {
        for(auto& middleware : middlewares)
            if(!middleware(req, res))
                return false;

        return true;
    }

    void push_back(middleware_cb_t cb)
    {
        middlewares.push_back(cb);
    }

private:
    std::vector<middleware_cb_t> middlewares;
};

}

#endif
