#pragma once

#include <ostream>

#include <cstdint>
#include <string>

struct Token
{
    unsigned long id;
    std::string value;

    /*
     * Token is "True" if it's id is bigger than zero. Because
     * lexer ids are all bigger than zero.
     *
     * This object could be used in while loop as a condition. 
     * E.g.:
     * while (auto token = ...)
     * {
     * }
     */
    operator bool() const
    {
        return id > 0;
    }

    /*
     * Ostream operator
     *
     * Prints token id and value in single line.
     */
    friend std::ostream& operator<<(std::ostream& stream, const Token& token)
    {
        return stream << "TOKEN id = " << token.id
                      << ", value = '" << token.value << "'";
    }
};
