#pragma once

#include <ostream>
#include <string>
#include "storage/model/properties/flags.hpp"
#include "utils/void.hpp"

class Null
{
public:
    const static Type type;

    Void &value() { return Void::_void; }
    Void const &value() const { return Void::_void; }

    std::ostream &print(std::ostream &stream) const;

    friend std::ostream &operator<<(std::ostream &stream, const Null &prop);

    bool operator==(const Null &) const;

    explicit operator bool();
};
