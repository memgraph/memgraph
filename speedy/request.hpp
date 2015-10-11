#ifndef MEMGRAPH_SPEEDY_REQUEST_HPP
#define MEMGRAPH_SPEEDY_REQUEST_HPP

#include <vector>

#include "http/request.hpp"

namespace sp
{

class Request : public http::Request
{
public:
    using http::Request::Request;

    std::vector<std::string> params;
};

}

#endif
