#ifndef MEMGRAPH_SPEEDY_RESPONSE_HPP
#define MEMGRAPH_SPEEDY_RESPONSE_HPP

#include "request.hpp"
#include "http/response.hpp"

namespace sp
{

class Response : public http::Response<Request, Response>
{
public:
    using http::Response<Request, Response>::Response;
};

using request_cb_t = std::function<void(Request&, Response&)>;

}

#endif
