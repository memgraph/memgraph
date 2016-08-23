#pragma once

#include "database/db_accessor.hpp"
#include "import/fillings/filler.hpp"

class LabelFiller : public Filler
{

public:
    LabelFiller(BaseImporter &db) : bim(db) {}

    // Fills skeleton with data from str. Returns error description if
    // error occurs.
    Option<std::string> fill(ElementSkeleton &data, char *str) final
    {
        sub_str.clear();
        bim.extract(str, bim.parts_array_mark, sub_str);
        for (auto s : sub_str) {
            if (s[0] != '\0') {
                data.add_label(bim.db.label_find_or_create(s));
            }
        }
        return make_option<std::string>();
    }

private:
    BaseImporter &bim;
    vector<char *> sub_str;
};
