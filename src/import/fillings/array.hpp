#pragma once

#include "database/db_accessor.hpp"
#include "import/fillings/common.hpp"
#include "import/fillings/filler.hpp"

template <class T, class A>
class ArrayFiller : public Filler
{

public:
    ArrayFiller(BaseImporter &db,
                PropertyFamily::PropertyType::PropertyFamilyKey key,
                T (*f)(const char *))
        : bim(db), key(key), f(f)
    {
    }

    // Fills skeleton with data from str. Returns error description if
    // error occurs.
    Option<std::string> fill(ElementSkeleton &data, char *str) final
    {
        sub_str.clear();
        std::vector<T> vec;
        bim.extract(str, bim.parts_array_mark, sub_str);
        for (auto s : sub_str) {
            if (s[0] != '\0') {
                vec.push_back(f(s));
            }
        }
        if (vec.size() > 0) {
            data.add_property(key, make_shared<A>(std::move(vec)));
        }
        return make_option<std::string>();
    }

private:
    BaseImporter &bim;
    PropertyFamily::PropertyType::PropertyFamilyKey key;
    vector<char *> sub_str;
    T (*f)(const char *);
};

template <class T, class A>
auto make_array_filler(BaseImporter &db,
                       PropertyFamily::PropertyType::PropertyFamilyKey key,
                       T (*f)(const char *))
{
    return new ArrayFiller<T, A>(db, key, f);
}
