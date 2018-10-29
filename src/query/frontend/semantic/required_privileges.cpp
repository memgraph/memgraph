#include "query/frontend/ast/ast.hpp"

namespace query {

class PrivilegeExtractor : public HierarchicalTreeVisitor {
 public:
  using HierarchicalTreeVisitor::PostVisit;
  using HierarchicalTreeVisitor::PreVisit;
  using HierarchicalTreeVisitor::Visit;

  std::vector<AuthQuery::Privilege> privileges() { return privileges_; }

  bool PreVisit(Create &) override {
    AddPrivilege(AuthQuery::Privilege::CREATE);
    return false;
  }
  bool PreVisit(Delete &) override {
    AddPrivilege(AuthQuery::Privilege::DELETE);
    return false;
  }
  bool PreVisit(Match &) override {
    AddPrivilege(AuthQuery::Privilege::MATCH);
    return false;
  }
  bool PreVisit(Merge &) override {
    AddPrivilege(AuthQuery::Privilege::MERGE);
    return false;
  }
  bool PreVisit(SetProperty &) override {
    AddPrivilege(AuthQuery::Privilege::SET);
    return false;
  }
  bool PreVisit(SetProperties &) override {
    AddPrivilege(AuthQuery::Privilege::SET);
    return false;
  }
  bool PreVisit(SetLabels &) override {
    AddPrivilege(AuthQuery::Privilege::SET);
    return false;
  }
  bool PreVisit(RemoveProperty &) override {
    AddPrivilege(AuthQuery::Privilege::REMOVE);
    return false;
  }
  bool PreVisit(RemoveLabels &) override {
    AddPrivilege(AuthQuery::Privilege::REMOVE);
    return false;
  }

  bool Visit(Identifier &) override { return true; }
  bool Visit(PrimitiveLiteral &) override { return true; }
  bool Visit(ParameterLookup &) override { return true; }

  bool Visit(IndexQuery &) override {
    AddPrivilege(AuthQuery::Privilege::INDEX);
    return true;
  }

  bool PreVisit(AuthQuery &) override {
    AddPrivilege(AuthQuery::Privilege::AUTH);
    return false;
  }

  bool PreVisit(StreamQuery &) override {
    AddPrivilege(AuthQuery::Privilege::STREAM);
    return false;
  }

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
