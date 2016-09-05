#pragma once

#include <memory>
#include <ostream>
#include <string>

#include "storage/model/properties/flags.hpp"

class String
{
public:
    const static Type type;

    String(std::string const &d) : data(d) {}
    String(std::string &&d) : data(std::move(d)) {}

    std::string &value() { return data; }

    std::string const &value() const { return data; }

    std::ostream &print(std::ostream &stream) const;

    friend std::ostream &operator<<(std::ostream &stream, const String &prop);

    bool operator==(const String &other) const;

    bool operator==(const std::string &other) const;

    // NOTE: OTHER METHODS WILL AUTOMATICALLY USE THIS IN CERTAIN SITUATIONS
    // TO MOVE STD::STRING OUT OF SHARED_PTR WHICH IS BAD.
    // operator const std::string &() const;

private:
    std::string data;
};
