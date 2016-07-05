#pragma once

#include "storage/model/properties/property.hpp"

class Null : public Property
{
public:
    friend class Property;

    static constexpr Flags type = Flags::Null;

    Null(const Null&) = delete;
    Null(Null&&) = delete;

    Null operator=(const Null&) = delete;

    bool operator==(const Property& other) const override;

    bool operator==(const Null&) const;

    explicit operator bool();

    friend std::ostream& operator<<(std::ostream& stream, const Null&);

    std::ostream& print(std::ostream& stream) const override;

private:
    // the constructor for null is private, it can be constructed only as a
    // value inside the Property class, Property::Null
    Null();
};
