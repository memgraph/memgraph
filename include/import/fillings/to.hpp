#pragma once

#include "import/fillings/common.hpp"
#include "import/fillings/filler.hpp"
#include "storage/model/properties/all.hpp"
#include "storage/model/properties/flags.hpp"
#include "storage/model/properties/property_family.hpp"

// Parses to import local id of vertex for edge.
class ToFiller : public Filler
{

public:
    ToFiller(BaseImporter &db) : bim(db) {}

    // Fills skeleton with data from str. Returns error description if
    // error occurs.
    Option<std::string> fill(ElementSkeleton &data, char *str) final
    {
        if (str[0] != '\0') {
            auto id = atol(str);
            Option<VertexAccessor> const &oav = bim.get_vertex(id);
            if (oav.is_present()) {
                data.set_to(VertexAccessor(oav.get()));
                return make_option<std::string>();
            } else {
                return make_option(
                    std::string("Unknown vertex in to field with id: ") + str);
            }
        } else {
            return make_option(std::string("To field must be spceified"));
        }
    }

private:
    BaseImporter &bim;
};
