#pragma once

#include <iostream>
#include <ostream>
#include <cstdint>
#include <string>
#include <fmt/format.h>

struct Token
{
    unsigned long id;
    std::string value;

    /*
     * Token is "True" if it's id is bigger than zero. Because
     * lexer ids are all bigger than zero.
     *
     * This object could be used in while loop as a conditional element. 
     * E.g.:
     * while (auto token = ...)
     * {
     * }
     */
    explicit operator bool() const
    {
        return id > 0;
    }

    /*
     * String representation.
     */
    std::string repr() const
    {
        // TODO: wrap fmt format
        // return fmt::format("TOKEN id = {}, value = {}", id, value);
        return "";
    }

    /*
     * Ostream operator
     *
     * Prints token id and value in single line.
     */
    friend std::ostream& operator<<(std::ostream& stream, const Token& token)
    {
        return stream << token.repr();
    }
};
