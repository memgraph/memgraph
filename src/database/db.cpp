#include "database/db.hpp"

#include "snapshot/snapshoter.hpp"
#include "storage/indexes/indexes.hpp"
#include "storage/model/properties/property_family.hpp"

Db::Db() : Db("default") {}

Db::Db(const std::string &name) : name_(name) { snap_engine.import(); }

Indexes Db::indexes() { return Indexes(*this); }

std::string const &Db::name() const { return name_; }
