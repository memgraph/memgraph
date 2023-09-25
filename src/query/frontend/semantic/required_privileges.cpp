// Copyright 2023 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include "query/frontend/ast/ast.hpp"
#include "query/frontend/ast/ast_visitor.hpp"
#include "query/procedure/module.hpp"
#include "utils/memory.hpp"

namespace memgraph::query {

class PrivilegeExtractor : public QueryVisitor<void>, public HierarchicalTreeVisitor {
 public:
  using HierarchicalTreeVisitor::PostVisit;
  using HierarchicalTreeVisitor::PreVisit;
  using HierarchicalTreeVisitor::Visit;
  using QueryVisitor<void>::Visit;

  std::vector<AuthQuery::Privilege> privileges() { return privileges_; }

  void Visit(IndexQuery & /*unused*/) override { AddPrivilege(AuthQuery::Privilege::INDEX); }

  void Visit(AnalyzeGraphQuery & /*unused*/) override { AddPrivilege(AuthQuery::Privilege::INDEX); }

  void Visit(AuthQuery & /*unused*/) override { AddPrivilege(AuthQuery::Privilege::AUTH); }

  void Visit(ExplainQuery &query) override { query.cypher_query_->Accept(dynamic_cast<QueryVisitor &>(*this)); }

  void Visit(ProfileQuery &query) override { query.cypher_query_->Accept(dynamic_cast<QueryVisitor &>(*this)); }

  void Visit(DatabaseInfoQuery &info_query) override {
    switch (info_query.info_type_) {
      case DatabaseInfoQuery::InfoType::INDEX:
        // TODO: This should be INDEX | STATS, but we don't have support for
        // *or* with privileges.
        AddPrivilege(AuthQuery::Privilege::INDEX);
        break;
      case DatabaseInfoQuery::InfoType::STORAGE:
        AddPrivilege(AuthQuery::Privilege::STATS);
        break;
      case DatabaseInfoQuery::InfoType::CONSTRAINT:
        // TODO: This should be CONSTRAINT | STATS, but we don't have support
        // for *or* with privileges.
        AddPrivilege(AuthQuery::Privilege::CONSTRAINT);
        break;
    }
  }

  void Visit(SystemInfoQuery &info_query) override {
    switch (info_query.info_type_) {
      case SystemInfoQuery::InfoType::BUILD:
        AddPrivilege(AuthQuery::Privilege::STATS);
        break;
    }
  }

  void Visit(ConstraintQuery &constraint_query) override { AddPrivilege(AuthQuery::Privilege::CONSTRAINT); }

  void Visit(CypherQuery &query) override {
    query.single_query_->Accept(*this);
    for (auto *cypher_union : query.cypher_unions_) {
      cypher_union->Accept(*this);
    }
  }

  void Visit(DumpQuery &dump_query) override { AddPrivilege(AuthQuery::Privilege::DUMP); }

  void Visit(LockPathQuery &lock_path_query) override { AddPrivilege(AuthQuery::Privilege::DURABILITY); }

  void Visit(FreeMemoryQuery &free_memory_query) override { AddPrivilege(AuthQuery::Privilege::FREE_MEMORY); }

  void Visit(ShowConfigQuery & /*show_config_query*/) override { AddPrivilege(AuthQuery::Privilege::CONFIG); }

  void Visit(TriggerQuery &trigger_query) override { AddPrivilege(AuthQuery::Privilege::TRIGGER); }

  void Visit(StreamQuery &stream_query) override { AddPrivilege(AuthQuery::Privilege::STREAM); }

  void Visit(ReplicationQuery &replication_query) override { AddPrivilege(AuthQuery::Privilege::REPLICATION); }

  void Visit(IsolationLevelQuery &isolation_level_query) override { AddPrivilege(AuthQuery::Privilege::CONFIG); }

  void Visit(StorageModeQuery & /*storage_mode_query*/) override { AddPrivilege(AuthQuery::Privilege::STORAGE_MODE); }

  void Visit(CreateSnapshotQuery &create_snapshot_query) override { AddPrivilege(AuthQuery::Privilege::DURABILITY); }

  void Visit(SettingQuery & /*setting_query*/) override { AddPrivilege(AuthQuery::Privilege::CONFIG); }

  void Visit(TransactionQueueQuery & /*transaction_queue_query*/) override {}

  void Visit(EdgeImportModeQuery & /*edge_import_mode_query*/) override {}

  void Visit(VersionQuery & /*version_query*/) override { AddPrivilege(AuthQuery::Privilege::STATS); }

  void Visit(MultiDatabaseQuery &query) override {
    switch (query.action_) {
      case MultiDatabaseQuery::Action::CREATE:
      case MultiDatabaseQuery::Action::DROP:
        AddPrivilege(AuthQuery::Privilege::MULTI_DATABASE_EDIT);
        break;
      case MultiDatabaseQuery::Action::USE:
        AddPrivilege(AuthQuery::Privilege::MULTI_DATABASE_USE);
        break;
    }
  }

  void Visit(ShowDatabasesQuery & /*unused*/) override {
    AddPrivilege(AuthQuery::Privilege::MULTI_DATABASE_USE); /* OR EDIT */
  }

  bool PreVisit(Create & /*unused*/) override {
    AddPrivilege(AuthQuery::Privilege::CREATE);
    return false;
  }
  bool PreVisit(CallProcedure &procedure) override {
    const auto maybe_proc =
        procedure::FindProcedure(procedure::gModuleRegistry, procedure.procedure_name_, utils::NewDeleteResource());
    if (maybe_proc && maybe_proc->second->info.required_privilege) {
      AddPrivilege(*maybe_proc->second->info.required_privilege);
    }
    return false;
  }
  bool PreVisit(Delete & /*unused*/) override {
    AddPrivilege(AuthQuery::Privilege::DELETE);
    return false;
  }
  bool PreVisit(Match & /*unused*/) override {
    AddPrivilege(AuthQuery::Privilege::MATCH);
    return false;
  }
  bool PreVisit(Merge & /*unused*/) override {
    AddPrivilege(AuthQuery::Privilege::MERGE);
    return false;
  }
  bool PreVisit(SetProperty & /*unused*/) override {
    AddPrivilege(AuthQuery::Privilege::SET);
    return false;
  }
  bool PreVisit(SetProperties & /*unused*/) override {
    AddPrivilege(AuthQuery::Privilege::SET);
    return false;
  }
  bool PreVisit(SetLabels & /*unused*/) override {
    AddPrivilege(AuthQuery::Privilege::SET);
    return false;
  }
  bool PreVisit(RemoveProperty & /*unused*/) override {
    AddPrivilege(AuthQuery::Privilege::REMOVE);
    return false;
  }
  bool PreVisit(RemoveLabels & /*unused*/) override {
    AddPrivilege(AuthQuery::Privilege::REMOVE);
    return false;
  }
  bool PreVisit(LoadCsv & /*unused*/) override {
    AddPrivilege(AuthQuery::Privilege::READ_FILE);
    return false;
  }

  bool Visit(Identifier & /*unused*/) override { return true; }
  bool Visit(PrimitiveLiteral & /*unused*/) override { return true; }
  bool Visit(ParameterLookup & /*unused*/) override { return true; }

 private:
  void AddPrivilege(AuthQuery::Privilege privilege) {
    if (!utils::Contains(privileges_, privilege)) {
      privileges_.push_back(privilege);
    }
  }

  std::vector<AuthQuery::Privilege> privileges_;
};

std::vector<AuthQuery::Privilege> GetRequiredPrivileges(Query *query) {
  PrivilegeExtractor extractor;
  query->Accept(extractor);
  return extractor.privileges();
}

}  // namespace memgraph::query
