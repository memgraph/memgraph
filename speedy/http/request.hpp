#ifndef UVMACHINE_HTTP_REQUEST_HPP
#define UVMACHINE_HTTP_REQUEST_HPP

#include <string>
#include <map>

#include "version.hpp"
#include "method.hpp"

namespace http
{

struct Request
{
    Version version;
    Method method;

    std::string url;

    std::map<std::string, std::string> headers;
    
    std::string body;
};

}

#endif
