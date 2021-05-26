#include "query/frontend/ast/ast.hpp"
#include "query/frontend/ast/ast_visitor.hpp"

namespace query {

class PrivilegeExtractor : public QueryVisitor<void>, public HierarchicalTreeVisitor {
 public:
  using HierarchicalTreeVisitor::PostVisit;
  using HierarchicalTreeVisitor::PreVisit;
  using HierarchicalTreeVisitor::Visit;
  using QueryVisitor<void>::Visit;

  std::vector<AuthQuery::Privilege> privileges() { return privileges_; }

  void Visit(IndexQuery &) override { AddPrivilege(AuthQuery::Privilege::INDEX); }

  void Visit(AuthQuery &) override { AddPrivilege(AuthQuery::Privilege::AUTH); }

  void Visit(ExplainQuery &query) override { query.cypher_query_->Accept(*this); }

  void Visit(ProfileQuery &query) override { query.cypher_query_->Accept(*this); }

  void Visit(InfoQuery &info_query) override {
    switch (info_query.info_type_) {
      case InfoQuery::InfoType::INDEX:
        // TODO: This should be INDEX | STATS, but we don't have support for
        // *or* with privileges.
        AddPrivilege(AuthQuery::Privilege::INDEX);
        break;
      case InfoQuery::InfoType::STORAGE:
        AddPrivilege(AuthQuery::Privilege::STATS);
        break;
      case InfoQuery::InfoType::CONSTRAINT:
        // TODO: This should be CONSTRAINT | STATS, but we don't have support
        // for *or* with privileges.
        AddPrivilege(AuthQuery::Privilege::CONSTRAINT);
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

  void Visit(LockPathQuery &lock_path_query) override { AddPrivilege(AuthQuery::Privilege::LOCK_PATH); }

  void Visit(FreeMemoryQuery &free_memory_query) override { AddPrivilege(AuthQuery::Privilege::FREE_MEMORY); }

  void Visit(ReplicationQuery &replication_query) override { AddPrivilege(AuthQuery::Privilege::REPLICATION); }

  bool PreVisit(Create & /*unused*/) override {
    AddPrivilege(AuthQuery::Privilege::CREATE);
    return false;
  }
  bool PreVisit(CallProcedure & /*unused*/) override {
    // TODO: Corresponding privilege
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

}  // namespace query
