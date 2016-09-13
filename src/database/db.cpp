#include "database/db.hpp"

#include "snapshot/snapshoter.hpp"
#include "storage/indexes/indexes.hpp"
#include "storage/model/properties/property_family.hpp"

Db::Db(bool import_snapshot) : Db("default", import_snapshot) {}

Db::Db(const std::string &name, bool import_snapshot)
    : Db(name.c_str(), import_snapshot)
{
}

Db::Db(const char *name, bool import_snapshot) : name_(name)
{
    if (import_snapshot) snap_engine.import();
}

Indexes Db::indexes() { return Indexes(*this); }

std::string const &Db::name() const { return name_; }
