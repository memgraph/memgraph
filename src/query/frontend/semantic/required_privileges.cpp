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

  bool Visit(CreateIndex &) override {
    AddPrivilege(AuthQuery::Privilege::INDEX);
    return true;
  }

  bool Visit(CreateUniqueIndex &) override {
    AddPrivilege(AuthQuery::Privilege::INDEX);
    return true;
  }

  bool Visit(AuthQuery &) override {
    AddPrivilege(AuthQuery::Privilege::AUTH);
    return true;
  }
  bool Visit(CreateStream &) override {
    AddPrivilege(AuthQuery::Privilege::STREAM);
    return true;
  }
  bool Visit(DropStream &) override {
    AddPrivilege(AuthQuery::Privilege::STREAM);
    return true;
  }
  bool Visit(ShowStreams &) override {
    AddPrivilege(AuthQuery::Privilege::STREAM);
    return true;
  }
  bool Visit(StartStopStream &) override {
    AddPrivilege(AuthQuery::Privilege::STREAM);
    return true;
  }
  bool Visit(StartStopAllStreams &) override {
    AddPrivilege(AuthQuery::Privilege::STREAM);
    return true;
  }
  bool Visit(TestStream &) override {
    AddPrivilege(AuthQuery::Privilege::STREAM);
    return true;
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
