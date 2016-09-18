#pragma once

#include "database/db_accessor.hpp"
#include "import/fillings/filler.hpp"

// Parses type of edge.
class TypeFiller : public Filler
{

public:
    TypeFiller(BaseImporter &db) : bim(db) {}

    // Fills skeleton with data from str. Returns error description if
    // error occurs.
    Option<std::string> fill(ElementSkeleton &data, char *str) final
    {
        if (str[0] != '\0') {
            data.set_type(bim.db.type_find_or_create(str));
        }

        return make_option<std::string>();
    }

private:
    BaseImporter &bim;
};
