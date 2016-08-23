#pragma once

#include "import/fillings/common.hpp"
#include "import/fillings/filler.hpp"
#include "storage/model/properties/all.hpp"
#include "storage/model/properties/flags.hpp"
#include "storage/model/properties/property_family.hpp"

class Int32Filler : public Filler
{

public:
    Int32Filler(PropertyFamily::PropertyType::PropertyFamilyKey key) : key(key)
    {
    }

    // Fills skeleton with data from str. Returns error description if
    // error occurs.
    Option<std::string> fill(ElementSkeleton &data, char *str) final
    {
        if (str[0] != '\0') {
            data.add_property(key, std::make_shared<Int32>(to_int32(str)));
        }

        return make_option<std::string>();
    }

private:
    PropertyFamily::PropertyType::PropertyFamilyKey key;
};
