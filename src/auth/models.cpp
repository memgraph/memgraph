// Copyright 2026 Memgraph Ltd.
//
// Licensed as a Memgraph Enterprise file under the Memgraph Enterprise
// License (the "License"); by using this file, you agree to be bound by the terms of the License, and you may not use
// this file except in compliance with the License. You may obtain a copy of the License at https://memgraph.com/legal.
//
//

#include "auth/models.hpp"

#include <cstdint>
#include <optional>
#include <utility>
#include <variant>

#include <gflags/gflags.h>
#include <nlohmann/json.hpp>

#include "auth/crypto.hpp"
#include "auth/exceptions.hpp"
#include "dbms/constants.hpp"
#include "license/license.hpp"
#include "nlohmann/detail/exceptions.hpp"
#include "query/constants.hpp"
#include "spdlog/spdlog.h"
#include "utils/string.hpp"
#include "utils/uuid.hpp"
#include "utils/variant_helpers.hpp"

namespace memgraph::auth {
namespace {

constexpr auto kRoleName = "rolename";
constexpr auto kPermissions = "permissions";
constexpr auto kGrants = "grants";
constexpr auto kDenies = "denies";
constexpr auto kUsername = "username";
constexpr auto kUUID = "uuid";
constexpr auto kPasswordHash = "password_hash";

#ifdef MG_ENTERPRISE

namespace r = std::ranges;

constexpr auto kGlobalGrants = "global_grants";
constexpr auto kGlobalDenies = "global_denies";
constexpr auto kFineGrainedPermissions = "fine_grained_permissions";
constexpr auto kLabelPermissions = "label_permissions";
constexpr auto kEdgeTypePermissions = "edge_type_permissions";
constexpr auto kAllowAll = "allow_all";
constexpr auto kDefault = "default";
constexpr auto kDatabases = "databases";
constexpr auto kUserImp = "user_imp";
constexpr auto kUserImpGranted = "user_imp_granted";
constexpr auto kUserImpDenied = "user_imp_denied";
constexpr auto kUserImpId = "user_imp_id";
constexpr auto kUserImpName = "user_imp_name";
constexpr auto kSymbols = "symbols";
constexpr auto kGranted = "granted";
constexpr auto kDenied = "denied";
constexpr auto kMatching = "matching";
constexpr auto kPermissionsKey = "permissions";
#endif

// Constant list of all available permissions.
const std::vector<Permission> kPermissionsAll = {
    Permission::MATCH,
    Permission::CREATE,
    Permission::MERGE,
    Permission::DELETE,
    Permission::SET,
    Permission::REMOVE,
    Permission::INDEX,
    Permission::STATS,
    Permission::CONSTRAINT,
    Permission::DUMP,
    Permission::AUTH,
    Permission::REPLICATION,
    Permission::DURABILITY,
    Permission::READ_FILE,
    Permission::FREE_MEMORY,
    Permission::TRIGGER,
    Permission::CONFIG,
    Permission::STREAM,
    Permission::MODULE_READ,
    Permission::MODULE_WRITE,
    Permission::WEBSOCKET,
    Permission::TRANSACTION_MANAGEMENT,
    Permission::STORAGE_MODE,
    Permission::MULTI_DATABASE_EDIT,
    Permission::MULTI_DATABASE_USE,
    Permission::COORDINATOR,
    Permission::IMPERSONATE_USER,
    Permission::PROFILE_RESTRICTION,
    Permission::PARALLEL_EXECUTION,
    Permission::SERVER_SIDE_PARAMETERS,
};

#ifdef MG_ENTERPRISE
const FineGrainedAccessPermissions empty_permissions{};
#endif

}  // namespace

#ifdef MG_ENTERPRISE
void to_json(nlohmann::json &data, const std::optional<UserImpersonation> &usr_imp) {
  if (usr_imp) {
    data = *usr_imp;
  } else {
    data = nlohmann::json();  // null
  }
}

void from_json(const nlohmann::json &data, std::optional<UserImpersonation> &usr_imp) {
  if (data.is_null()) {
    usr_imp.reset();
  } else {
    usr_imp = std::make_optional<UserImpersonation>(data);
  }
}
#endif

std::string PermissionToString(Permission permission) {
  switch (permission) {
    case Permission::MATCH:
      return "MATCH";
    case Permission::CREATE:
      return "CREATE";
    case Permission::MERGE:
      return "MERGE";
    case Permission::DELETE:
      return "DELETE";
    case Permission::SET:
      return "SET";
    case Permission::REMOVE:
      return "REMOVE";
    case Permission::INDEX:
      return "INDEX";
    case Permission::STATS:
      return "STATS";
    case Permission::CONSTRAINT:
      return "CONSTRAINT";
    case Permission::DUMP:
      return "DUMP";
    case Permission::REPLICATION:
      return "REPLICATION";
    case Permission::DURABILITY:
      return "DURABILITY";
    case Permission::READ_FILE:
      return "READ_FILE";
    case Permission::FREE_MEMORY:
      return "FREE_MEMORY";
    case Permission::TRIGGER:
      return "TRIGGER";
    case Permission::CONFIG:
      return "CONFIG";
    case Permission::AUTH:
      return "AUTH";
    case Permission::STREAM:
      return "STREAM";
    case Permission::MODULE_READ:
      return "MODULE_READ";
    case Permission::MODULE_WRITE:
      return "MODULE_WRITE";
    case Permission::WEBSOCKET:
      return "WEBSOCKET";
    case Permission::TRANSACTION_MANAGEMENT:
      return "TRANSACTION_MANAGEMENT";
    case Permission::STORAGE_MODE:
      return "STORAGE_MODE";
    case Permission::MULTI_DATABASE_EDIT:
      return "MULTI_DATABASE_EDIT";
    case Permission::MULTI_DATABASE_USE:
      return "MULTI_DATABASE_USE";
    case Permission::COORDINATOR:
      return "COORDINATOR";
    case Permission::IMPERSONATE_USER:
      return "IMPERSONATE_USER";
    case Permission::PROFILE_RESTRICTION:
      return "PROFILE_RESTRICTION";
    case Permission::PARALLEL_EXECUTION:
      return "PARALLEL_EXECUTION";
    case Permission::SERVER_SIDE_PARAMETERS:
      return "SERVER_SIDE_PARAMETERS";
  }
}

std::string PermissionLevelToString(PermissionLevel level) {
  switch (level) {
    case PermissionLevel::GRANT:
      return "GRANT";
    case PermissionLevel::NEUTRAL:
      return "NEUTRAL";
    case PermissionLevel::DENY:
      return "DENY";
  }
}

#ifdef MG_ENTERPRISE
std::string FineGrainedPermissionToString(uint64_t const permission) {
  if (permission == 0) {
    return "";
  }

  std::vector<std::string> permissions;
  if (permission & FineGrainedPermission::CREATE) {
    permissions.emplace_back("CREATE");
  }
  if (permission & FineGrainedPermission::READ) {
    permissions.emplace_back("READ");
  }
  if (permission & FineGrainedPermission::SET_LABEL) {
    permissions.emplace_back("SET LABEL");
  }
  if (permission & FineGrainedPermission::REMOVE_LABEL) {
    permissions.emplace_back("REMOVE LABEL");
  }
  if (permission & FineGrainedPermission::SET_PROPERTY) {
    permissions.emplace_back("SET PROPERTY");
  }
  if (permission & FineGrainedPermission::DELETE) {
    permissions.emplace_back("DELETE");
  }

  return utils::Join(permissions, ", ");
}

FineGrainedAccessPermissions Merge(FineGrainedAccessPermissions const &first,
                                   FineGrainedAccessPermissions const &second) {
  // Merge global grants: OR them together
  std::optional<uint64_t> global_grants;
  auto first_grants = first.GetGlobalGrants();
  auto second_grants = second.GetGlobalGrants();
  if (first_grants.has_value() && second_grants.has_value()) {
    global_grants = first_grants.value() | second_grants.value();
  } else if (first_grants.has_value()) {
    global_grants = first_grants;
  } else if (second_grants.has_value()) {
    global_grants = second_grants;
  }

  // Merge global denies: OR them together (deny from either source wins)
  std::optional<uint64_t> global_denies;
  auto first_denies = first.GetGlobalDenies();
  auto second_denies = second.GetGlobalDenies();
  if (first_denies.has_value() && second_denies.has_value()) {
    global_denies = first_denies.value() | second_denies.value();
  } else if (first_denies.has_value()) {
    global_denies = first_denies;
  } else if (second_denies.has_value()) {
    global_denies = second_denies;
  }

  // Merge rules
  auto merged_rules = first.GetRules();
  for (auto &&rule2 : second.GetRules()) {
    auto it = r::find_if(merged_rules, [&](auto const &rule1) {
      return rule1.symbols == rule2.symbols && rule1.matching_mode == rule2.matching_mode;
    });

    if (it != merged_rules.end()) {
      // Merge grants and denies separately
      it->grants = it->grants | rule2.grants;
      it->denies = it->denies | rule2.denies;
    } else {
      merged_rules.push_back(rule2);
    }
  }

  return FineGrainedAccessPermissions(global_grants, global_denies, std::move(merged_rules));
}
#endif

Permissions::Permissions(uint64_t grants, uint64_t denies) {
  // The deny bitmask has higher priority than the grant bitmask.
  denies_ = denies;
  // Mask out the grant bitmask to make sure that it is correct.
  grants_ = grants & (~denies);
}

PermissionLevel Permissions::Has(Permission permission) const {
  // Check for the deny first because it has greater priority than a grant.
  if (denies_ & std::to_underlying(permission)) {
    return PermissionLevel::DENY;
  }
  if (grants_ & std::to_underlying(permission)) {
    return PermissionLevel::GRANT;
  }
  return PermissionLevel::NEUTRAL;
}

void Permissions::Grant(Permission permission) {
  // Remove the possible deny.
  denies_ &= ~std::to_underlying(permission);
  // Now we grant the permission.
  grants_ |= std::to_underlying(permission);
}

void Permissions::Revoke(Permission permission) {
  // Remove the possible grant.
  grants_ &= ~std::to_underlying(permission);
  // Remove the possible deny.
  denies_ &= ~std::to_underlying(permission);
}

void Permissions::Deny(Permission permission) {
  // First deny the permission.
  denies_ |= std::to_underlying(permission);
  // Remove the possible grant.
  grants_ &= ~std::to_underlying(permission);
}

std::vector<Permission> Permissions::GetGrants() const {
  std::vector<Permission> ret;
  for (const auto &permission : kPermissionsAll) {
    if (Has(permission) == PermissionLevel::GRANT) {
      ret.push_back(permission);
    }
  }
  return ret;
}

std::vector<Permission> Permissions::GetDenies() const {
  std::vector<Permission> ret;
  for (const auto &permission : kPermissionsAll) {
    if (Has(permission) == PermissionLevel::DENY) {
      ret.push_back(permission);
    }
  }
  return ret;
}

nlohmann::json Permissions::Serialize() const {
  nlohmann::json data = nlohmann::json::object();

  data[kGrants] = grants_;
  data[kDenies] = denies_;
  return data;
}

Permissions Permissions::Deserialize(const nlohmann::json &data) {
  if (!data.is_object()) {
    throw AuthException("Couldn't load permissions data!");
  }
  auto grants = data.find(kGrants);
  auto denies = data.find(kDenies);
  if (grants == data.end() || denies == data.end()) {
    throw AuthException("Couldn't load permissions data!");
  }
  if (!grants->is_number_unsigned() || !denies->is_number_unsigned()) {
    throw AuthException("Couldn't load permissions data!");
  }
  return Permissions{data[kGrants], data[kDenies]};
}

uint64_t Permissions::grants() const { return grants_; }

uint64_t Permissions::denies() const { return denies_; }

bool operator==(const Permissions &first, const Permissions &second) {
  return first.grants() == second.grants() && first.denies() == second.denies();
}

bool operator!=(const Permissions &first, const Permissions &second) { return !(first == second); }

#ifdef MG_ENTERPRISE
FineGrainedAccessPermissions::FineGrainedAccessPermissions(std::optional<uint64_t> global_grants,
                                                           std::optional<uint64_t> global_denies,
                                                           std::vector<FineGrainedAccessRule> rules)
    : global_grants_(global_grants), global_denies_(global_denies), rules_{std::move(rules)} {}

PermissionLevel FineGrainedAccessPermissions::Has(std::span<std::string const> symbols,
                                                  const FineGrainedPermission fine_grained_permission) const {
  if (!memgraph::license::global_license_checker.IsEnterpriseValidFast()) {
    return PermissionLevel::GRANT;
  }

  if (symbols.empty()) {
    return PermissionLevel::GRANT;
  }

  auto const rule_matches = [&](auto &&rule) -> bool {
    if (rule.matching_mode == MatchingMode::EXACTLY) {
      return rule.symbols.size() == symbols.size() &&
             r::all_of(symbols, [&](auto const &symbol) { return rule.symbols.contains(symbol); });
    }
    return r::any_of(symbols, [&](auto const &symbol) { return rule.symbols.contains(symbol); });
  };

  // DENYs take priority over GRANTs, and specific rules on labels or edge types
  // take priority over global rules. Finally, if no rules apply, this is a
  // silent DENY.

  bool any_rule_matched = false;
  bool any_grant = false;

  for (auto &&rule : rules_) {
    if (rule_matches(rule)) {
      any_rule_matched = true;
      if ((rule.denies & fine_grained_permission) != FineGrainedPermission::NONE) {
        return PermissionLevel::DENY;
      }
      if ((rule.grants & fine_grained_permission) != FineGrainedPermission::NONE) {
        any_grant = true;
      }
    }
  }

  if (any_grant) {
    return PermissionLevel::GRANT;
  }

  if (any_rule_matched) {
    return PermissionLevel::DENY;
  }

  return HasGlobal(fine_grained_permission);
}

PermissionLevel FineGrainedAccessPermissions::HasGlobal(const FineGrainedPermission fine_grained_permission) const {
  if (!memgraph::license::global_license_checker.IsEnterpriseValidFast()) {
    return PermissionLevel::GRANT;
  }

  if (global_denies_.has_value() && (global_denies_.value() & static_cast<uint64_t>(fine_grained_permission)) != 0) {
    return PermissionLevel::DENY;
  }

  if (global_grants_.has_value() && (global_grants_.value() & static_cast<uint64_t>(fine_grained_permission)) != 0) {
    return PermissionLevel::GRANT;
  }

  return PermissionLevel::DENY;
}

void FineGrainedAccessPermissions::Grant(std::unordered_set<std::string> const &symbols,
                                         FineGrainedPermission const fine_grained_permission,
                                         MatchingMode const matching_mode) {
  auto it = r::find_if(
      rules_, [&](auto const &rule) { return rule.symbols == symbols && rule.matching_mode == matching_mode; });

  if (it != rules_.end()) {
    it->denies = it->denies & ~fine_grained_permission;
    it->grants = it->grants | fine_grained_permission;
  } else {
    rules_.push_back({symbols, fine_grained_permission, FineGrainedPermission::NONE, matching_mode});
  }
}

void FineGrainedAccessPermissions::GrantGlobal(FineGrainedPermission const fine_grained_permission) {
  if (global_denies_.has_value()) {
    global_denies_ = global_denies_.value() & ~static_cast<uint64_t>(fine_grained_permission);
    if (global_denies_.value() == 0) {
      global_denies_ = std::nullopt;
    }
  }

  global_grants_ = global_grants_.value_or(0) | static_cast<uint64_t>(fine_grained_permission);
}

void FineGrainedAccessPermissions::Deny(std::unordered_set<std::string> const &symbols,
                                        FineGrainedPermission const fine_grained_permission,
                                        MatchingMode const matching_mode) {
  auto it = r::find_if(
      rules_, [&](auto const &rule) { return rule.symbols == symbols && rule.matching_mode == matching_mode; });

  if (it != rules_.end()) {
    it->grants = it->grants & ~fine_grained_permission;
    it->denies = it->denies | fine_grained_permission;
  } else {
    rules_.push_back({symbols, FineGrainedPermission::NONE, fine_grained_permission, matching_mode});
  }
}

void FineGrainedAccessPermissions::DenyGlobal(FineGrainedPermission const fine_grained_permission) {
  if (global_grants_.has_value()) {
    global_grants_ = global_grants_.value() & ~static_cast<uint64_t>(fine_grained_permission);
    if (global_grants_.value() == 0) {
      global_grants_ = std::nullopt;
    }
  }

  global_denies_ = global_denies_.value_or(0) | static_cast<uint64_t>(fine_grained_permission);
}

void FineGrainedAccessPermissions::Revoke(std::unordered_set<std::string> const &symbols,
                                          FineGrainedPermission const fine_grained_permission,
                                          MatchingMode const matching_mode) {
  auto it = r::find_if(
      rules_, [&](auto const &rule) { return rule.symbols == symbols && rule.matching_mode == matching_mode; });

  if (it != rules_.end()) {
    it->grants = it->grants & ~fine_grained_permission;
    it->denies = it->denies & ~fine_grained_permission;

    if (it->grants == FineGrainedPermission::NONE && it->denies == FineGrainedPermission::NONE) {
      rules_.erase(it);
    }
  }
}

void FineGrainedAccessPermissions::RevokeGlobal(FineGrainedPermission const fine_grained_permission) {
  if (global_grants_.has_value()) {
    global_grants_ = global_grants_.value() & ~static_cast<uint64_t>(fine_grained_permission);
    if (global_grants_.value() == 0) {
      global_grants_ = std::nullopt;
    }
  }

  if (global_denies_.has_value()) {
    global_denies_ = global_denies_.value() & ~static_cast<uint64_t>(fine_grained_permission);
    if (global_denies_.value() == 0) {
      global_denies_ = std::nullopt;
    }
  }
}

void FineGrainedAccessPermissions::RevokeAll() {
  global_grants_ = std::nullopt;
  global_denies_ = std::nullopt;
  rules_.clear();
}

void FineGrainedAccessPermissions::RevokeAll(FineGrainedPermission const fine_grained_permission) {
  if (global_grants_.has_value()) {
    global_grants_ = global_grants_.value() & ~static_cast<uint64_t>(fine_grained_permission);
    if (global_grants_.value() == 0) {
      global_grants_ = std::nullopt;
    }
  }

  if (global_denies_.has_value()) {
    global_denies_ = global_denies_.value() & ~static_cast<uint64_t>(fine_grained_permission);
    if (global_denies_.value() == 0) {
      global_denies_ = std::nullopt;
    }
  }

  for (auto it = rules_.begin(); it != rules_.end();) {
    it->grants = it->grants & ~fine_grained_permission;
    it->denies = it->denies & ~fine_grained_permission;
    if (it->grants == FineGrainedPermission::NONE && it->denies == FineGrainedPermission::NONE) {
      it = rules_.erase(it);
    } else {
      ++it;
    }
  }
}

nlohmann::json FineGrainedAccessPermissions::Serialize() const {
  if (!memgraph::license::global_license_checker.IsEnterpriseValidFast()) {
    return {};
  }
  nlohmann::json data = nlohmann::json::object();
  data[kGlobalGrants] = global_grants_.value_or(-1);
  data[kGlobalDenies] = global_denies_.value_or(-1);

  nlohmann::json rules_json = nlohmann::json::array();
  for (const auto &rule : rules_) {
    nlohmann::json rule_json;
    rule_json[kSymbols] = std::vector<std::string>(rule.symbols.begin(), rule.symbols.end());
    rule_json[kGranted] = static_cast<uint64_t>(rule.grants);
    rule_json[kDenied] = static_cast<uint64_t>(rule.denies);
    rule_json[kMatching] = (rule.matching_mode == MatchingMode::EXACTLY) ? "EXACTLY" : "ANY";
    rules_json.push_back(rule_json);
  }
  data[kPermissionsKey] = rules_json;

  return data;
}

FineGrainedAccessPermissions FineGrainedAccessPermissions::Deserialize(const nlohmann::json &data) {
  if (!data.is_object()) {
    throw AuthException("Couldn't load permissions data!");
  }
  if (!memgraph::license::global_license_checker.IsEnterpriseValidFast()) {
    return FineGrainedAccessPermissions{};
  }

  std::optional<uint64_t> global_grants;
  std::optional<uint64_t> global_denies;

  if (data.contains(kGlobalGrants)) {
    auto val = data.find(kGlobalGrants);
    if (val != data.end() && val->is_number_integer() && *val != -1) {
      global_grants = *val;
    }
  }
  if (data.contains(kGlobalDenies)) {
    auto val = data.find(kGlobalDenies);
    if (val != data.end() && val->is_number_integer() && *val != -1) {
      global_denies = *val;
    }
  }

  std::vector<FineGrainedAccessRule> rules;

  auto rules_json = data.find(kPermissionsKey);
  if (rules_json != data.end() && rules_json->is_array()) {
    for (const auto &rule_json : *rules_json) {
      if (!rule_json.is_object()) continue;

      std::unordered_set<std::string> symbols;
      if (rule_json.contains(kSymbols) && rule_json[kSymbols].is_array()) {
        for (const auto &symbol : rule_json[kSymbols]) {
          if (symbol.is_string()) {
            symbols.insert(symbol.get<std::string>());
          }
        }
      }

      auto grants = FineGrainedPermission::NONE;
      auto denies = FineGrainedPermission::NONE;

      if (rule_json.contains(kGranted) && rule_json[kGranted].is_number()) {
        grants = static_cast<FineGrainedPermission>(rule_json[kGranted].get<uint64_t>());
      }
      if (rule_json.contains(kDenied) && rule_json[kDenied].is_number()) {
        denies = static_cast<FineGrainedPermission>(rule_json[kDenied].get<uint64_t>());
      }

      auto matching_mode = MatchingMode::ANY;
      if (rule_json.contains(kMatching) && rule_json[kMatching].is_string()) {
        std::string const mode_str = rule_json[kMatching].get<std::string>();
        if (mode_str == "EXACTLY") {
          matching_mode = MatchingMode::EXACTLY;
        }
      }

      rules.push_back({symbols, grants, denies, matching_mode});
    }
  }

  return FineGrainedAccessPermissions(global_grants, global_denies, std::move(rules));
}

std::optional<uint64_t> const &FineGrainedAccessPermissions::GetGlobalGrants() const { return global_grants_; }

std::optional<uint64_t> const &FineGrainedAccessPermissions::GetGlobalDenies() const { return global_denies_; }

std::vector<FineGrainedAccessRule> const &FineGrainedAccessPermissions::GetRules() const { return rules_; }

bool operator==(const FineGrainedAccessPermissions &first, const FineGrainedAccessPermissions &second) {
  return first.GetGlobalGrants() == second.GetGlobalGrants() && first.GetGlobalDenies() == second.GetGlobalDenies() &&
         first.GetRules() == second.GetRules();
}

bool operator!=(const FineGrainedAccessPermissions &first, const FineGrainedAccessPermissions &second) {
  return !(first == second);
}

FineGrainedAccessHandler::FineGrainedAccessHandler(FineGrainedAccessPermissions labelPermissions,
                                                   FineGrainedAccessPermissions edgeTypePermissions)
    : label_permissions_(std::move(labelPermissions)), edge_type_permissions_(std::move(edgeTypePermissions)) {}

const FineGrainedAccessPermissions &FineGrainedAccessHandler::label_permissions() const { return label_permissions_; }

FineGrainedAccessPermissions &FineGrainedAccessHandler::label_permissions() { return label_permissions_; }

const FineGrainedAccessPermissions &FineGrainedAccessHandler::edge_type_permissions() const {
  return edge_type_permissions_;
}

FineGrainedAccessPermissions &FineGrainedAccessHandler::edge_type_permissions() { return edge_type_permissions_; }

nlohmann::json FineGrainedAccessHandler::Serialize() const {
  if (!memgraph::license::global_license_checker.IsEnterpriseValidFast()) {
    return {};
  }
  nlohmann::json data = nlohmann::json::object();
  data[kLabelPermissions] = label_permissions_.Serialize();
  data[kEdgeTypePermissions] = edge_type_permissions_.Serialize();
  return data;
}

FineGrainedAccessHandler FineGrainedAccessHandler::Deserialize(const nlohmann::json &data) {
  if (!memgraph::license::global_license_checker.IsEnterpriseValidFast()) {
    return FineGrainedAccessHandler{};
  }
  if (!data.is_object()) {
    throw AuthException("Couldn't load fine grained access data!");
  }

  auto label_permissions = data.find(kLabelPermissions);
  if (label_permissions == data.end() || !label_permissions->is_object()) {
    throw AuthException("Couldn't load fine grained access data!");
  }

  auto edge_type_permissions = data.find(kEdgeTypePermissions);
  if (edge_type_permissions == data.end() || !edge_type_permissions->is_object()) {
    throw AuthException("Couldn't load fine grained access data!");
  }

  return FineGrainedAccessHandler(FineGrainedAccessPermissions::Deserialize(*label_permissions),
                                  FineGrainedAccessPermissions::Deserialize(*edge_type_permissions));
}

bool operator==(const FineGrainedAccessHandler &first, const FineGrainedAccessHandler &second) {
  return first.label_permissions_ == second.label_permissions_ &&
         first.edge_type_permissions_ == second.edge_type_permissions_;
}

bool operator!=(const FineGrainedAccessHandler &first, const FineGrainedAccessHandler &second) {
  return !(first == second);
}
#endif

Role::Role(const std::string &rolename) : rolename_(utils::ToLowerCase(rolename)) {}

Role::Role(const std::string &rolename, const Permissions &permissions)
    : rolename_(utils::ToLowerCase(rolename)), permissions_(permissions) {}
#ifdef MG_ENTERPRISE
Role::Role(const std::string &rolename, const Permissions &permissions,
           FineGrainedAccessHandler fine_grained_access_handler, Databases db_access,
           std::optional<UserImpersonation> usr_imp)
    : rolename_(utils::ToLowerCase(rolename)),
      permissions_(permissions),
      fine_grained_access_handler_(std::move(fine_grained_access_handler)),
      db_access_(std::move(db_access)),
      user_impersonation_{std::move(usr_imp)} {}
#endif

const std::string &Role::rolename() const { return rolename_; }

const Permissions &Role::permissions() const { return permissions_; }

Permissions &Role::permissions() { return permissions_; }
#ifdef MG_ENTERPRISE
const FineGrainedAccessHandler &Role::fine_grained_access_handler() const { return fine_grained_access_handler_; }

FineGrainedAccessHandler &Role::fine_grained_access_handler() { return fine_grained_access_handler_; }

const FineGrainedAccessPermissions &Role::GetFineGrainedAccessLabelPermissions(
    std::optional<std::string_view> db_name) const {
  if (!memgraph::license::global_license_checker.IsEnterpriseValidFast()) {
    return empty_permissions;
  }

  // If db_name is provided, check if the role has access to that database
  if (db_name && !HasAccess(*db_name)) {
    return empty_permissions;
  }

  return fine_grained_access_handler_.label_permissions();
}

const FineGrainedAccessPermissions &Role::GetFineGrainedAccessEdgeTypePermissions(
    std::optional<std::string_view> db_name) const {
  if (!memgraph::license::global_license_checker.IsEnterpriseValidFast()) {
    return empty_permissions;
  }

  // If db_name is provided, check if the role has access to that database
  if (db_name && !HasAccess(*db_name)) {
    return empty_permissions;
  }

  return fine_grained_access_handler_.edge_type_permissions();
}
#endif

nlohmann::json Role::Serialize() const {
  nlohmann::json data = nlohmann::json::object();
  data[kRoleName] = rolename_;
  data[kPermissions] = permissions_.Serialize();
#ifdef MG_ENTERPRISE
  if (memgraph::license::global_license_checker.IsEnterpriseValidFast()) {
    data[kFineGrainedPermissions] = fine_grained_access_handler_.Serialize();
    data[kDatabases] = db_access_.Serialize();
    data[kUserImp] = user_impersonation_;
  } else {
    data[kFineGrainedPermissions] = {};
    data[kDatabases] = {};
    data[kUserImp] = {};
  }
#endif
  return data;
}

Role Role::Deserialize(const nlohmann::json &data) {
  if (!data.is_object()) {
    throw AuthException("Couldn't load role data!");
  }
  auto role_name_it = data.find(kRoleName);
  auto permissions_it = data.find(kPermissions);
  if (role_name_it == data.end() || permissions_it == data.end()) {
    throw AuthException("Couldn't load role data!");
  }
  if (!role_name_it->is_string() || !permissions_it->is_object()) {
    throw AuthException("Couldn't load role data!");
  }
  auto permissions = Permissions::Deserialize(*permissions_it);
#ifdef MG_ENTERPRISE
  if (memgraph::license::global_license_checker.IsEnterpriseValidFast()) {
    Databases db_access;
    auto db_access_it = data.find(kDatabases);
    if (db_access_it != data.end() && db_access_it->is_structured()) {
      db_access = Databases::Deserialize(*db_access_it);
    } else {
      spdlog::warn("Role without specified database access. Given access to the default database.");
    }

    FineGrainedAccessHandler fine_grained_access_handler{};
    // We can have an empty fine_grained if the user was created without a valid license
    auto fine_grainged_access_it = data.find(kFineGrainedPermissions);
    if (fine_grainged_access_it != data.end() && fine_grainged_access_it->is_object()) {
      fine_grained_access_handler = FineGrainedAccessHandler::Deserialize(*fine_grainged_access_it);
    } else {
      spdlog::warn("Role without fine grained access. Defaulting to none.");
    }

    std::optional<UserImpersonation> usr_imp = std::nullopt;
    auto imp_data = data.find(kUserImp);
    if (imp_data != data.end()) {
      usr_imp = imp_data->get<std::optional<UserImpersonation>>();
    } else {
      spdlog::warn("Role without impersonation information; defaulting to no impersonation ability.");
    }
    return {
        *role_name_it, permissions, std::move(fine_grained_access_handler), std::move(db_access), std::move(usr_imp)};
  }
#endif
  return {*role_name_it, permissions};
}

bool operator==(const Role &first, const Role &second) {
#ifdef MG_ENTERPRISE
  if (memgraph::license::global_license_checker.IsEnterpriseValidFast()) {
    return first.rolename_ == second.rolename_ && first.permissions_ == second.permissions_ &&
           first.fine_grained_access_handler_ == second.fine_grained_access_handler_;
  }
#endif
  return first.rolename_ == second.rolename_ && first.permissions_ == second.permissions_;
}

#ifdef MG_ENTERPRISE
void Databases::Grant(std::string_view db) {
  if (allow_all_) {
    grants_dbs_.clear();
    allow_all_ = false;
  }
  grants_dbs_.emplace(db);
  denies_dbs_.erase(std::string{db});  // TODO: C++23 use transparent key compare
}

void Databases::Deny(const std::string &db) {
  denies_dbs_.emplace(db);
  grants_dbs_.erase(db);
}

void Databases::Revoke(const std::string &db) {
  denies_dbs_.erase(db);
  if (!allow_all_) {
    grants_dbs_.erase(db);
  }
  // Reset if default deleted
  if (main_db_ == db) {
    main_db_ = "";
  }
}

void Databases::GrantAll() {
  allow_all_ = true;
  grants_dbs_.clear();
  denies_dbs_.clear();
}

void Databases::DenyAll() {
  allow_all_ = false;
  grants_dbs_.clear();
  denies_dbs_.clear();
}

void Databases::RevokeAll() {
  allow_all_ = false;
  grants_dbs_.clear();
  denies_dbs_.clear();
  main_db_ = "";
}

bool Databases::SetMain(std::string_view db) {
  if (!Contains(db)) return false;
  main_db_ = db;
  return true;
}

[[nodiscard]] bool Databases::Contains(std::string_view db) const {
  return !denies_dbs_.contains(db) && (allow_all_ || grants_dbs_.contains(db));
}

const std::string &Databases::GetMain() const {
  if (!Contains(main_db_)) {
    throw AuthException("No access to the set default database \"{}\".", main_db_);
  }
  return main_db_;
}

nlohmann::json Databases::Serialize() const {
  nlohmann::json data = nlohmann::json::object();
  data[kGrants] = grants_dbs_;
  data[kDenies] = denies_dbs_;
  data[kAllowAll] = allow_all_;
  data[kDefault] = main_db_;
  return data;
}

Databases Databases::Deserialize(const nlohmann::json &data) {
  if (!data.is_object()) {
    throw AuthException("Couldn't load database data!");
  }
  auto grants_it = data.find(kGrants);
  auto denies_it = data.find(kDenies);
  auto allow_all_it = data.find(kAllowAll);
  auto default_it = data.find(kDefault);
  if (grants_it == data.end() || denies_it == data.end() || allow_all_it == data.end() || default_it == data.end()) {
    throw AuthException("Couldn't load database data!");
  }
  if (!grants_it->is_structured() || !denies_it->is_structured() || !allow_all_it->is_boolean() ||
      !default_it->is_string()) {
    throw AuthException("Couldn't load database data!");
  }
  return {*allow_all_it, *grants_it, *denies_it, *default_it};
}
#endif

User::User() = default;

User::User(const std::string &username) : username_(utils::ToLowerCase(username)) {}

User::User(const std::string &username, std::optional<HashedPassword> password_hash, const Permissions &permissions,
           utils::UUID uuid)
    : username_(utils::ToLowerCase(username)),
      password_hash_(std::move(password_hash)),
      permissions_(permissions),
      uuid_(uuid) {}

#ifdef MG_ENTERPRISE
User::User(const std::string &username, std::optional<HashedPassword> password_hash, const Permissions &permissions,
           FineGrainedAccessHandler fine_grained_access_handler, Databases db_access, utils::UUID uuid,
           std::optional<UserImpersonation> usr_imp)
    : username_(utils::ToLowerCase(username)),
      password_hash_(std::move(password_hash)),
      permissions_(permissions),
      fine_grained_access_handler_(std::move(fine_grained_access_handler)),
      database_access_(std::move(db_access)),
      user_impersonation_{std::move(usr_imp)},
      uuid_(uuid) {}
#endif

bool User::CheckPassword(const std::string &password) {
  return password_hash_ ? password_hash_->VerifyPassword(password) : true;
}

bool User::CheckPasswordExplicit(const std::string &password) {
  return password_hash_ ? password_hash_->VerifyPassword(password) : password.empty();
}

void User::UpdatePassword(const std::optional<std::string> &password,
                          std::optional<PasswordHashAlgorithm> algo_override) {
  if (!password) {
    password_hash_.reset();
    return;
  }
  password_hash_ = HashPassword(*password, algo_override);
}

void User::UpdateHash(HashedPassword hashed_password) { password_hash_ = std::move(hashed_password); }

void User::ClearAllRoles() {
  // Clear all roles by creating a new empty Roles object
  roles_ = Roles{};
#ifdef MG_ENTERPRISE
  // Clear all multi-tenant mappings
  role_db_map_.clear();
  db_role_map_.clear();
#endif
}

// New methods for multiple roles
void User::AddRole(const Role &role) {
  // If role is set to a specific database, convert it to a global role
#ifdef MG_ENTERPRISE
  if (role_db_map_.contains(role.rolename())) {
    throw AuthException("Role '{}' is already specified as a multi-tenant role", role.rolename());
  }
#endif
  roles_.AddRole(role);
}

#ifdef MG_ENTERPRISE
void User::AddMultiTenantRole(Role role, const std::string &db_name) {
  // Nothing to do if role is already in the map
  if (db_role_map_.contains(db_name) && db_role_map_[db_name].contains(role.rolename())) {
    return;
  }

  // Global roles are not allowed to be specified on a database
  if (!role_db_map_.contains(role.rolename()) && roles_.GetRole(role.rolename())) {
    throw AuthException("Role '{}' is already specified as a global role", role.rolename());
  }

  // Role has to have access to the database in question
  if (!role.HasAccess(db_name)) {
    throw AuthException("Roles need access to the database to be specified on it");
  }

  // Check if user already has this role
  if (auto it = std::find_if(
          roles().begin(), roles().end(), [&role](const auto &in) { return role.rolename() == in.rolename(); });
      it != roles().end()) {
    // Role is already present (the original role has access to the database)
    // Add access if the user's role does't already have access to the database
    if (!it->HasAccess(db_name)) {
      role = *it;
      role.db_access().Grant(db_name);
    }
  } else {
    // Specify role to the target database and add role to user
    role.db_access().DenyAll();
    role.db_access().Grant(db_name);
    role.db_access().SetMain(db_name);
  }

  // Add role to map
  role_db_map_[role.rolename()].insert(db_name);
  db_role_map_[db_name].insert(role.rolename());
  // Update role in roles_
  roles_.RemoveRole(role.rolename());
  roles_.AddRole(role);
}

void User::ClearMultiTenantRoles(const std::string &db_name) {
  // If role is specified on a database, that means it couldn't have been specified as a global role
  // In turn this means that we need to:
  // 1. Remove the role from the role_db_map_
  // 2. Remove the role from the roles_, but only if it's not specified on any other database
  auto it = db_role_map_.find(db_name);
  if (it != db_role_map_.end()) {
    for (const auto &role_name : it->second) {
      // Remove from the reverse map
      role_db_map_[role_name].erase(db_name);
      // Remove from the roles_
      auto role = roles_.GetRole(role_name);
      if (!role) {
        // This should never happen, but we'll just continue
        continue;
      }
      roles_.RemoveRole(role_name);
      // If user had role on other databases, we need to add it back
      if (role->HasAccess(db_name) && role->db_access().GetGrants().size() > 1) {
        role->db_access().Deny(db_name);
        roles_.AddRole(*role);
      }
    }
    db_role_map_.erase(it);
  }
}
#endif

#ifdef MG_ENTERPRISE
FineGrainedAccessPermissions User::GetFineGrainedAccessLabelPermissions(std::optional<std::string_view> db_name) const {
  if (!memgraph::license::global_license_checker.IsEnterpriseValidFast()) {
    return FineGrainedAccessPermissions{};
  }

  if (db_name && !HasAccess(*db_name)) return FineGrainedAccessPermissions{};  // Users+role level access check
  return Merge(GetUserFineGrainedAccessLabelPermissions(), GetRoleFineGrainedAccessLabelPermissions(db_name));
}

FineGrainedAccessPermissions User::GetFineGrainedAccessEdgeTypePermissions(
    std::optional<std::string_view> db_name) const {
  if (!memgraph::license::global_license_checker.IsEnterpriseValidFast()) {
    return FineGrainedAccessPermissions{};
  }

  if (db_name && !HasAccess(*db_name)) return FineGrainedAccessPermissions{};  // Users+role level access check
  return Merge(GetUserFineGrainedAccessEdgeTypePermissions(), GetRoleFineGrainedAccessEdgeTypePermissions(db_name));
}

FineGrainedAccessPermissions User::GetUserFineGrainedAccessEdgeTypePermissions(
    std::optional<std::string_view> db_name) const {
  if (!memgraph::license::global_license_checker.IsEnterpriseValidFast()) {
    return FineGrainedAccessPermissions{};
  }

  if (db_name && !HasAccess(*db_name)) return FineGrainedAccessPermissions{};  // Users+role level access check
  return fine_grained_access_handler_.edge_type_permissions();
}

FineGrainedAccessPermissions User::GetUserFineGrainedAccessLabelPermissions(
    std::optional<std::string_view> db_name) const {
  if (!memgraph::license::global_license_checker.IsEnterpriseValidFast()) {
    return FineGrainedAccessPermissions{};
  }

  if (db_name && !HasAccess(*db_name)) return FineGrainedAccessPermissions{};  // Users+role level access check
  return fine_grained_access_handler_.label_permissions();
}

FineGrainedAccessPermissions User::GetRoleFineGrainedAccessEdgeTypePermissions(
    std::optional<std::string_view> db_name) const {
  if (!memgraph::license::global_license_checker.IsEnterpriseValidFast()) {
    return FineGrainedAccessPermissions{};
  }

  FineGrainedAccessPermissions combined_permissions{};
  for (const auto &role : roles_) {
    // If db_name is provided, only include roles that grant access to that database
    if (!db_name || role.HasAccess(*db_name)) {  // Role level access check
      combined_permissions = Merge(combined_permissions, role.fine_grained_access_handler().edge_type_permissions());
    }
  }
  return combined_permissions;
}

FineGrainedAccessPermissions User::GetRoleFineGrainedAccessLabelPermissions(
    std::optional<std::string_view> db_name) const {
  if (!memgraph::license::global_license_checker.IsEnterpriseValidFast()) {
    return FineGrainedAccessPermissions{};
  }

  FineGrainedAccessPermissions combined_permissions{};
  for (const auto &role : roles_) {
    // If db_name is provided, only include roles that grant access to that database
    if (!db_name || role.HasAccess(*db_name)) {  // Role level access check
      combined_permissions = Merge(combined_permissions, role.fine_grained_access_handler().label_permissions());
    }
  }
  return combined_permissions;
}
#endif

const std::string &User::username() const { return username_; }

const Permissions &User::permissions() const { return permissions_; }

Permissions &User::permissions() { return permissions_; }
#ifdef MG_ENTERPRISE
const FineGrainedAccessHandler &User::fine_grained_access_handler() const { return fine_grained_access_handler_; }

FineGrainedAccessHandler &User::fine_grained_access_handler() { return fine_grained_access_handler_; }
#endif

nlohmann::json User::Serialize() const {
  // NOTE: Role and Profile are stored as links to the role and profile lists.
  nlohmann::json data = nlohmann::json::object();
  data[kUsername] = username_;
  data[kUUID] = uuid_;
  if (password_hash_) {
    data[kPasswordHash] = *password_hash_;
  } else {
    data[kPasswordHash] = nullptr;
  }
  data[kPermissions] = permissions_.Serialize();
#ifdef MG_ENTERPRISE
  if (memgraph::license::global_license_checker.IsEnterpriseValidFast()) {
    data[kFineGrainedPermissions] = fine_grained_access_handler_.Serialize();
    data[kDatabases] = database_access_.Serialize();
    data[kUserImp] = user_impersonation_;
  } else {
    data[kFineGrainedPermissions] = {};
    data[kDatabases] = {};
    data[kUserImp] = {};
  }
#endif
  // The role shouldn't be serialized here, it is stored as a foreign key.
  return data;
}

User User::Deserialize(const nlohmann::json &data) {
  if (!data.is_object()) {
    throw AuthException("Couldn't load user data!");
  }
  auto username_it = data.find(kUsername);
  auto hash_it = data.find(kPasswordHash);
  auto permissions_it = data.find(kPermissions);
  if (username_it == data.end() || hash_it == data.end() || permissions_it == data.end()) {
    throw AuthException("Couldn't load user data!");
  }
  if (!username_it->is_string() || !(hash_it->is_object() || hash_it->is_null()) || !permissions_it->is_object()) {
    throw AuthException("Couldn't load user data!");
  }

  // If UUID is not present, default to an auto-generated one
  utils::UUID uuid{};
  auto uuid_it = data.find(kUUID);
  if (uuid_it != data.end() && uuid_it->is_array()) uuid = *uuid_it;

  std::optional<HashedPassword> password_hash{};
  if (hash_it->is_object()) {
    try {
      password_hash = hash_it->get<HashedPassword>();
    } catch (const nlohmann::detail::exception & /* unused */) {
      throw AuthException("Failed to read user's password hash.");
    }
  }

  auto permissions = Permissions::Deserialize(*permissions_it);

#ifdef MG_ENTERPRISE
  if (memgraph::license::global_license_checker.IsEnterpriseValidFast()) {
    // Set initially to default database access; overwrite if access present in the json
    Databases db_access;
    auto db_access_it = data.find(kDatabases);
    if (db_access_it != data.end() && db_access_it->is_structured()) {
      db_access = Databases::Deserialize(*db_access_it);
    } else {
      spdlog::warn("User without specified database access. Given access to the default database.");
    }

    // We can have an empty fine_grained if the user was created without a valid license
    FineGrainedAccessHandler fine_grained_access_handler{};
    auto fine_grainged_access_it = data.find(kFineGrainedPermissions);
    if (fine_grainged_access_it != data.end() && fine_grainged_access_it->is_object()) {
      fine_grained_access_handler = FineGrainedAccessHandler::Deserialize(*fine_grainged_access_it);
    } else {
      spdlog::warn("User without fine grained access. Defaulting to none.");
    }

    std::optional<UserImpersonation> usr_imp = std::nullopt;
    auto imp_data = data.find(kUserImp);
    if (imp_data != data.end()) {
      usr_imp = imp_data->get<std::optional<UserImpersonation>>();
    } else {
      spdlog::warn("User without impersonation information; defaulting to no impersonation ability.");
    }

    return {*username_it,
            std::move(password_hash),
            permissions,
            std::move(fine_grained_access_handler),
            std::move(db_access),
            uuid,
            std::move(usr_imp)};
  }
#endif
  return {*username_it, std::move(password_hash), permissions, uuid};
}

bool operator==(const User &first, const User &second) {
#ifdef MG_ENTERPRISE
  if (memgraph::license::global_license_checker.IsEnterpriseValidFast()) {
    return first.username_ == second.username_ && first.password_hash_ == second.password_hash_ &&
           first.permissions_ == second.permissions_ && first.roles_ == second.roles_ &&
           first.fine_grained_access_handler_ == second.fine_grained_access_handler_;
  }
#endif
  return first.username_ == second.username_ && first.password_hash_ == second.password_hash_ &&
         first.permissions_ == second.permissions_ && first.roles_ == second.roles_;
}

#ifdef MG_ENTERPRISE
// Overrides any pervious granted or denied sets
void UserImpersonation::GrantAll() {
  // Remove any denied users
  denied_.clear();
  // Set granted to all
  granted_ = GrantAllUsers{};
}

// Overrides the previous granted set, but not the denied set
void UserImpersonation::Grant(const std::vector<User> &users) {
  granted_ = std::set<UserId>{};
  for (const auto &user : users) {
    grant_one(user);
  }
}

// Overrides the previous denied set, but not the granted set
void UserImpersonation::Deny(const std::vector<User> &users) {
  denied_.clear();
  for (const auto &user : users) {
    deny_one(user);
  }
}

bool UserImpersonation::CanImpersonate(const User &user) const { return !IsDenied(user) && IsGranted(user); }

bool UserImpersonation::IsDenied(const User &user) const {
  const std::string_view username = user.username();
  // Check denied
  //  check if username is denied
  //    check if uuid is the same
  //      yes -> return true
  //      no -> remove user from the list and return false
  auto user_denied = find_denied(username);
  if (user_denied) {
    if ((*user_denied)->uuid == user.uuid()) return true;
    erase_denied(*user_denied);  // Stale user; remove
  }
  return false;
}

bool UserImpersonation::IsGranted(const User &user) const {
  const std::string_view username = user.username();
  // Check grant
  //  all -> return true
  //  check if username is in the list
  //    check if uuid is the same
  //      yes -> return true
  //      no -> remove user from the list and proceed
  if (grants_all()) return true;
  auto user_granted = find_granted(username);
  if (user_granted) {
    if ((*user_granted)->uuid == user.uuid()) return true;
    erase_granted(*user_granted);  // Stale user; remove
  }
  return false;
}

void UserImpersonation::grant_one(const User &user) {
  // Check if user is denied
  //  update denied_
  auto denied_user = find_denied(user.username());
  if (denied_user) {
    erase_denied(*denied_user);  // Stale user; remove
  }

  // Check if all are granted
  //  yes -> return
  //  no
  //    check if current username is in the list
  //      no -> add
  //      yes -> remove old one and add the current one
  if (grants_all()) return;
  auto granted_user = find_granted(user.username());
  if (granted_user) {
    if ((*granted_user)->uuid == user.uuid()) return;
    erase_granted(*granted_user);  // Stale user; remove
  }
  emplace_granted(user.username(), user.uuid());
}

void UserImpersonation::deny_one(const User &user) {
  // Check granted
  //  all -> skip
  //  check if user is granted
  //    remove irelevent of the uuid
  if (!grants_all()) {
    auto granted_user = find_granted(user.username());
    if (granted_user) {
      // remove user irrelevant of the uuid
      erase_granted(*granted_user);  // Stale user; remove
    }
  }

  // Check denied
  //  check if user is in the list
  //    no -> add
  //    yes
  //      check if uuids match
  //        no -> remove and add
  //        yes -> return
  auto denied_user = find_denied(user.username());
  if (denied_user) {
    if ((*denied_user)->uuid == user.uuid()) return;
    erase_denied(*denied_user);  // Stale user; remove
  }
  denied_.emplace(user.username(), user.uuid());
}

void to_json(nlohmann::json &data, const UserImpersonation::UserId &uid) {
  data = nlohmann::json::object({{kUserImpId, uid.uuid}, {kUserImpName, uid.name}});
}

void from_json(const nlohmann::json &data, UserImpersonation::UserId &uid) {
  uid = {data.at(kUserImpName), data.at(kUserImpId)};
}

void to_json(nlohmann::json &data, const UserImpersonation::GrantAllUsers & /* unused */) {
  data = nlohmann::json::object();
}

void from_json(const nlohmann::json &data, UserImpersonation::GrantAllUsers & /* unused */) {
  // Empty struct
}

// Empty object = all users
// Empty array = no users
// Array of objects = specific users
void to_json(nlohmann::json &data, const UserImpersonation &usr_imp) {
  data = nlohmann::json::object();

  std::visit(utils::Overloaded{[&](std::set<UserImpersonation::UserId> granted) {
                                 auto res = nlohmann::json::array();
                                 for (const auto &uid : granted) {
                                   res.push_back({{kUserImpId, uid.uuid}, {kUserImpName, uid.name}});
                                 }
                                 // Empty array if no granted users
                                 data[kUserImpGranted] = std::move(res);
                               },
                               [&](UserImpersonation::GrantAllUsers obj) { data[kUserImpGranted] = obj; }},
             usr_imp.granted_);

  auto res = nlohmann::json::array();
  for (const auto &uid : usr_imp.denied_) {
    res.emplace_back(uid);
  }
  data[kUserImpDenied] = std::move(res);
}

void from_json(const nlohmann::json &data, UserImpersonation &usr_imp) {
  if (!data.is_object()) {
    throw AuthException("Couldn't load user impersonation data!");
  }
  auto grants_it = data.find(kUserImpGranted);
  auto denied_it = data.find(kUserImpDenied);
  if (grants_it == data.end() || denied_it == data.end()) {
    throw AuthException("Couldn't load user impersonation data!");
  }

  UserImpersonation::GrantedUsers granted;
  if (grants_it->is_array()) {
    try {
      granted = {grants_it->get<std::set<UserImpersonation::UserId>>()};
    } catch (nlohmann::detail::exception & /* unused */) {
      throw AuthException("Couldn't load user impersonation data!");
    }
  } else if (grants_it->is_object()) {
    granted = UserImpersonation::GrantAllUsers{};
  } else {
    throw AuthException("Couldn't load user impersonation data!");
  }

  UserImpersonation::DeniedUsers denied{};
  if (!denied_it->is_array()) {
    throw AuthException("Couldn't load user impersonation data!");
  }
  try {
    denied = *denied_it;
  } catch (nlohmann::detail::exception & /* unused */) {
    throw AuthException("Couldn't load user impersonation data!");
  }

  usr_imp = {std::move(granted), std::move(denied)};
}

const FineGrainedAccessPermissions &Roles::GetFineGrainedAccessLabelPermissions(
    std::optional<std::string_view> db_name) const {
  if (roles_.empty()) return empty_permissions;

  FineGrainedAccessPermissions combined_permissions;
  for (const auto &role : roles_) {
    if (!db_name || role.HasAccess(*db_name)) {
      combined_permissions = Merge(combined_permissions, role.fine_grained_access_handler().label_permissions());
    }
  }
  static FineGrainedAccessPermissions result;
  result = combined_permissions;
  return result;
}

const FineGrainedAccessPermissions &Roles::GetFineGrainedAccessEdgeTypePermissions(
    std::optional<std::string_view> db_name) const {
  if (roles_.empty()) return empty_permissions;

  FineGrainedAccessPermissions combined_permissions;
  for (const auto &role : roles_) {
    if (!db_name || role.HasAccess(*db_name)) {
      combined_permissions = Merge(combined_permissions, role.fine_grained_access_handler().edge_type_permissions());
    }
  }
  static FineGrainedAccessPermissions result;
  result = combined_permissions;
  return result;
}
#endif  // MG_ENTERPRISE

}  // namespace memgraph::auth
