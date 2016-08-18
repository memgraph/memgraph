#include "database/db.hpp"
#include "storage/model/properties/property_family.hpp"

Db::Db() = default;
Db::Db(const std::string &name) : name_(name) {}

std::string &Db::name() { return name_; }
