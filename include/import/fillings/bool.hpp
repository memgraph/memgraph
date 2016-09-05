#pragma once

#include "import/fillings/common.hpp"
#include "import/fillings/filler.hpp"
#include "storage/model/properties/all.hpp"
#include "storage/model/properties/flags.hpp"
#include "storage/model/properties/property_family.hpp"

template <class TG>
class BoolFiller : public Filler
{

public:
    BoolFiller(typename PropertyFamily<TG>::PropertyType::PropertyFamilyKey key)
        : key(key)
    {
    }

    // Fills skeleton with data from str. Returns error description if
    // error occurs.
    Option<std::string> fill(ElementSkeleton &data, char *str) final
    {
        if (str[0] != '\0') {
            data.add_property(StoredProperty<TG>(Bool(to_bool(str)), key));
        }

        return make_option<std::string>();
    }

private:
    typename PropertyFamily<TG>::PropertyType::PropertyFamilyKey key;
};
