#pragma once

#include <ostream>

#include <cstdint>
#include <string>

struct Token
{
    unsigned long id;
    std::string value;

    friend std::ostream& operator<<(std::ostream& stream, const Token& token)
    {
        return stream << "TOKEN id = " << token.id
                      << ", value = '" << token.value << "'";
    }
};
