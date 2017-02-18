#pragma once

#include "import/fillings/common.hpp"
#include "import/fillings/filler.hpp"
#include "storage/model/properties/all.hpp"
#include "storage/model/properties/flags.hpp"
#include "storage/model/properties/property_family.hpp"

// Parses from id of vertex for edge.
class FromFiller : public Filler {
 public:
  FromFiller(BaseImporter &db) : bim(db) {}

  // Fills skeleton with data from str. Returns error description if
  // error occurs.
  Option<std::string> fill(ElementSkeleton &data, char *str) final {
    if (str[0] != '\0') {
      auto id = atol(str);
      Option<VertexAccessor> const &oav = bim.get_vertex(id);
      if (oav.is_present()) {
        data.set_from(VertexAccessor(oav.get()));
        return make_option<std::string>();
      } else {
        return make_option(
            std::string("Unknown vertex in from field with id: ") + str);
      }
    } else {
      return make_option(std::string("From field must be spceified"));
    }
  }

 private:
  BaseImporter &bim;
};
