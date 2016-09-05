#pragma once

#include <string>

namespace web
{

class Client
{
    void post(const std::string& url, const std::string& body);
};

}
