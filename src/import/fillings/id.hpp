#pragma once

#include "import/fillings/filler.hpp"

template <class TG>
class IdFiller : public Filler
{

public:
    IdFiller()
        : key(make_option<
              typename PropertyFamily<TG>::PropertyType::PropertyFamilyKey>())
    {
    }

    IdFiller(
        Option<typename PropertyFamily<TG>::PropertyType::PropertyFamilyKey>
            key)
        : key(key)
    {
        assert(!key.is_present() ||
               key.get().prop_type() == Type(Flags::Int64));
    }

    // Fills skeleton with data from str. Returns error description if
    // error occurs.
    Option<std::string> fill(ElementSkeleton &data, char *str) final
    {
        if (str[0] != '\0') {
            data.set_element_id(atol(str));
            if (key.is_present()) {
                data.add_property(key.get(),
                                  std::make_shared<Int64>(to_int64(str)));
            }
        }

        return make_option<std::string>();
    }

private:
    Option<typename PropertyFamily<TG>::PropertyType::PropertyFamilyKey> key;
};
