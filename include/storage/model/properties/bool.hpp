#pragma once

#include "storage/model/properties/property.hpp"

class Bool : public Property
{
public:
    static constexpr Flags type = Flags::Bool;

    Bool(bool value);
    Bool(const Bool& other) = default;

    bool value() const;

    explicit operator bool() const;

    bool operator==(const Property& other) const override;

    bool operator==(const Bool& other) const;

    bool operator==(bool v) const;

    std::ostream& print(std::ostream& stream) const override;

    friend std::ostream& operator<<(std::ostream& stream, const Bool& prop);
};

