#include "glue/auth.hpp"

namespace glue {

auth::Permission PrivilegeToPermission(query::AuthQuery::Privilege privilege) {
  switch (privilege) {
    case query::AuthQuery::Privilege::MATCH:
      return auth::Permission::MATCH;
    case query::AuthQuery::Privilege::CREATE:
      return auth::Permission::CREATE;
    case query::AuthQuery::Privilege::MERGE:
      return auth::Permission::MERGE;
    case query::AuthQuery::Privilege::DELETE:
      return auth::Permission::DELETE;
    case query::AuthQuery::Privilege::SET:
      return auth::Permission::SET;
    case query::AuthQuery::Privilege::REMOVE:
      return auth::Permission::REMOVE;
    case query::AuthQuery::Privilege::INDEX:
      return auth::Permission::INDEX;
    case query::AuthQuery::Privilege::STATS:
      return auth::Permission::STATS;
    case query::AuthQuery::Privilege::CONSTRAINT:
      return auth::Permission::CONSTRAINT;
    case query::AuthQuery::Privilege::DUMP:
      return auth::Permission::DUMP;
    case query::AuthQuery::Privilege::REPLICATION:
      return auth::Permission::REPLICATION;
    case query::AuthQuery::Privilege::LOCK_PATH:
      return auth::Permission::LOCK_PATH;
    case query::AuthQuery::Privilege::AUTH:
      return auth::Permission::AUTH;
  }
}
}  // namespace glue
