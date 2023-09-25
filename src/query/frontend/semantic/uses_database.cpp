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

class BLA : public QueryVisitor<void>, public HierarchicalTreeVisitor {
 public:
  using HierarchicalTreeVisitor::PostVisit;
  using HierarchicalTreeVisitor::PreVisit;
  using HierarchicalTreeVisitor::Visit;
  using QueryVisitor<void>::Visit;

  bool state() { return state_; }

  bool PreVisit(Create & /*unused*/) override {
    setState(usage_kind::REQ_DB_REPLICATION_W);
    return false;
  }
  bool PreVisit(CallProcedure & /*unused*/) override {
    // we have no idea what the procedure will do,
    // even if not needed, we will grant the default/current db
    setState(usage_kind::REQ_DB_REPLICATION_W);
    return false;
  }
  bool PreVisit(Delete & /*unused*/) override {
    setState(usage_kind::REQ_DB_REPLICATION_W);
    return false;
  }

  bool PreVisit(Match & /*unused*/) override {
    setState(usage_kind::REQ_DB_NON_REPLICATION_R);
    return true;
  }
  bool PreVisit(Merge & /*unused*/) override {
    setState(usage_kind::REQ_DB_REPLICATION_W);
    return false;
  }
  bool PreVisit(SetProperty & /*unused*/) override {
    setState(usage_kind::REQ_DB_REPLICATION_W);
    return false;
  }
  bool PreVisit(SetProperties & /*unused*/) override {
    setState(usage_kind::REQ_DB_REPLICATION_W);
    return false;
  }
  bool PreVisit(SetLabels & /*unused*/) override {
    setState(usage_kind::REQ_DB_REPLICATION_W);
    return false;
  }
  bool PreVisit(RemoveProperty & /*unused*/) override {
    setState(usage_kind::REQ_DB_REPLICATION_W);
    return false;
  }
  bool PreVisit(RemoveLabels & /*unused*/) override {
    setState(usage_kind::REQ_DB_REPLICATION_W);
    return false;
  }

  void Visit(IndexQuery & /*unused*/) override {
    // Performance observability, implies observability
    // hence transaction required for replication sequencing
    setState(usage_kind::REQ_DB_REPLICATION_W);
  }

  void Visit(AnalyzeGraphQuery & /*unused*/) override {
    // creates stats objects which can influence perf of planning
    setState(usage_kind::REQ_DB_REPLICATION_W);
  }

  void Visit(AuthQuery & /*unused*/) override { setState(usage_kind::NONE); }

  void Visit(ExplainQuery &query) override { query.cypher_query_->Accept(dynamic_cast<QueryVisitor &>(*this)); }

  void Visit(ProfileQuery &query) override { query.cypher_query_->Accept(dynamic_cast<QueryVisitor &>(*this)); }

  void Visit(DatabaseInfoQuery &info_query) override { setState(usage_kind::REQ_DB_NON_REPLICATION_R); }

  void Visit(SystemInfoQuery &info_query) override { setState(usage_kind::NONE); }

  void Visit(ConstraintQuery & /*unused*/) override { setState(usage_kind::REQ_DB_REPLICATION_W); }

  void Visit(CypherQuery &query) override {
    query.single_query_->Accept(*this);
    for (auto *cypher_union : query.cypher_unions_) {
      cypher_union->Accept(*this);
    }
  }

  void Visit(DumpQuery & /*unused*/) override { setState(usage_kind::REQ_DB_NON_REPLICATION_R); }

  void Visit(LockPathQuery & /*unused*/) override {
    // not transactional... but requires
    setState(usage_kind::REQ_DB_NON_REPLICATION_NO_DATA);
  }

  void Visit(FreeMemoryQuery & /*unused*/) override { setState(usage_kind::REQ_DB_NON_REPLICATION_NO_DATA); }

  void Visit(ShowConfigQuery & /*unused*/) override { setState(usage_kind::NONE); }

  void Visit(TriggerQuery &trigger_query) override {
    switch (trigger_query.action_) {
      using enum TriggerQuery::Action;
      case CREATE_TRIGGER:
      case DROP_TRIGGER:
        setState(usage_kind::REQ_DB_REPLICATION_W);
        break;
      case SHOW_TRIGGERS:
        setState(usage_kind::REQ_DB_NON_REPLICATION_R);
        break;
    }
  }

  void Visit(StreamQuery &stream_query) override {}

  void Visit(ReplicationQuery &replication_query) override {}

  void Visit(IsolationLevelQuery &isolation_level_query) override {}

  void Visit(StorageModeQuery & /*storage_mode_query*/) override {}

  void Visit(CreateSnapshotQuery &create_snapshot_query) override {}

  void Visit(SettingQuery & /*setting_query*/) override {}

  void Visit(TransactionQueueQuery & /*transaction_queue_query*/) override {
    // ATM SHOW/TERMINATE is with respect to the current database
    // TODO: in future when database-less queries, change to NONE
    setState(usage_kind::REQ_DB_NON_REPLICATION_NO_DATA);
  }

  void Visit(EdgeImportModeQuery & /*edge_import_mode_query*/) override {}

  void Visit(VersionQuery & /*version_query*/) override {}

  void Visit(MultiDatabaseQuery &query) override {}

  void Visit(ShowDatabasesQuery & /*unused*/) override { /* OR EDIT */
  }

  bool Visit(Identifier & /*unused*/) override { return true; }
  bool Visit(PrimitiveLiteral & /*unused*/) override { return true; }
  bool Visit(ParameterLookup & /*unused*/) override { return true; }

  enum usage_kind {
    NONE = 0,
    REQ_DB_NON_REPLICATION_NO_DATA = 1,
    REQ_DB_NON_REPLICATION_R = 2,
    REQ_DB_REPLICATION_W = 3,
  };

 private:
  void setState(usage_kind k) { state_ = std::max(k, state_); }

  usage_kind state_ = NONE;
};

bool UsesDatabases(Query *query) {
  BLA extractor;
  query->Accept(extractor);
  return extractor.state() != BLA::usage_kind::NONE;
}

}  // namespace memgraph::query
