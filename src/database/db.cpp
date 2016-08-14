#include "database/db.hpp"

Db::Db() = default;
Db::Db(const std::string &name) : name_(name) {}

std::string &Db::name() { return name_; }
