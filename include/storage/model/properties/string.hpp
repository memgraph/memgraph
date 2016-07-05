#pragma once

#include "storage/model/properties/property.hpp"

class String : public Property
{
public:
    static constexpr Flags type = Flags::String;

    String(const String&) = default;
    String(String&&) = default;

    String(const std::string& value);
    String(std::string&& value);

    operator const std::string&() const;

    bool operator==(const Property& other) const override;

    bool operator==(const String& other) const;

    bool operator==(const std::string& other) const;

    friend std::ostream& operator<<(std::ostream& stream, const String& prop);

    std::ostream& print(std::ostream& stream) const override;

    std::string value;
};
