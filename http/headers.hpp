#pragma once

#include <array>

#include "utils/string/weak_string.hpp"

namespace http
{

class Headers
{
    static constexpr WeakString empty = WeakString();

public:
    struct Header
    {
        WeakString key;
        WeakString value;
    };

    std::pair<bool, WeakString&> operator[](const std::string& key)
    {
        auto ww = WeakString();

        for(auto& header : headers)
            if(key == header.key)
                return {true, header.value};

        return {false, ww};
    }

private:
    std::array<Header, 64> headers;
};


}
