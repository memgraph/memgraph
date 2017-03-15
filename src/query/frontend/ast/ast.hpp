#pragma once

#include <memory>
#include <vector>

#include "database/graph_db.hpp"
#include "query/frontend/ast/ast_visitor.hpp"
#include "utils/visitor/visitable.hpp"

namespace query {

class Tree : public ::utils::Visitable<TreeVisitorBase> {
public:
  Tree(int uid) : uid_(uid) {}
  int uid() const { return uid_; }

 private:
  const int uid_;
};

class Expression : public Tree {
 public:
  Expression(int uid) : Tree(uid) {}
};

class Identifier : public Expression {
 public:
  Identifier(int uid, const std::string &name) : Expression(uid), name_(name) {}

  DEFVISITABLE(TreeVisitorBase)

  std::string name_;
};

class PropertyLookup : public Expression {
 public:
  PropertyLookup(int uid, std::shared_ptr<Expression> expression,
                 GraphDb::Property property)
      : Expression(uid), expression_(expression), property_(property) {}

  void Accept(TreeVisitorBase &visitor) override {
    visitor.Visit(*this);
    expression_->Accept(visitor);
    visitor.PostVisit(*this);
  }

  std::shared_ptr<Expression>
      expression_;  // vertex or edge, what if map literal???
  GraphDb::Property property_;
  // TODO potential problem: property lookups are allowed on both map literals
  // and records, but map literals have strings as keys and records have
  // GraphDb::Property
  //
  // possible solution: store both string and GraphDb::Property here and choose
  // between the two depending on Expression result
};

class NamedExpression : public Tree {
 public:
  NamedExpression(int uid) : Tree(uid) {}
  void Accept(TreeVisitorBase &visitor) override {
    visitor.Visit(*this);
    expression_->Accept(visitor);
    visitor.PostVisit(*this);
  }

  std::string name_;
  std::shared_ptr<Expression> expression_;
};

class PatternAtom : public Tree {
 public:
  PatternAtom(int uid) : Tree(uid) {}
};

class NodeAtom : public PatternAtom {
 public:
  NodeAtom(int uid) : PatternAtom(uid) {}
  void Accept(TreeVisitorBase &visitor) override {
    visitor.Visit(*this);
    identifier_->Accept(visitor);
    visitor.PostVisit(*this);
  }

  std::shared_ptr<Identifier> identifier_;
  std::vector<GraphDb::Label> labels_;
};

class EdgeAtom : public PatternAtom {
 public:
  enum class Direction { LEFT, RIGHT, BOTH };

  EdgeAtom(int uid) : PatternAtom(uid) {}
  void Accept(TreeVisitorBase &visitor) override {
    visitor.Visit(*this);
    identifier_->Accept(visitor);
    visitor.PostVisit(*this);
  }

  Direction direction = Direction::BOTH;
  std::shared_ptr<Identifier> identifier_;
};

class Clause : public Tree {
 public:
  Clause(int uid) : Tree(uid) {}
};

class Pattern : public Tree {
 public:
  Pattern(int uid) : Tree(uid) {}
  void Accept(TreeVisitorBase &visitor) override {
    visitor.Visit(*this);
    for (auto &part : atoms_) {
      part->Accept(visitor);
    }
    visitor.PostVisit(*this);
  }
  std::shared_ptr<Identifier> identifier_;
  std::vector<std::shared_ptr<PatternAtom>> atoms_;
};

class Query : public Tree {
 public:
  Query(int uid) : Tree(uid) {}
  void Accept(TreeVisitorBase &visitor) override {
    visitor.Visit(*this);
    for (auto &clause : clauses_) {
      clause->Accept(visitor);
    }
    visitor.PostVisit(*this);
  }
  std::vector<std::shared_ptr<Clause>> clauses_;
};

class Match : public Clause {
 public:
  Match(int uid) : Clause(uid) {}
  std::vector<std::shared_ptr<Pattern>> patterns_;
  void Accept(TreeVisitorBase &visitor) override {
    visitor.Visit(*this);
    for (auto &pattern : patterns_) {
      pattern->Accept(visitor);
    }
    visitor.PostVisit(*this);
  }
};

class Return : public Clause {
 public:
  Return(int uid) : Clause(uid) {}
  void Accept(TreeVisitorBase &visitor) override {
    visitor.Visit(*this);
    for (auto &expr : named_expressions_) {
      expr->Accept(visitor);
    }
    visitor.PostVisit(*this);
  }
  std::vector<std::shared_ptr<NamedExpression>> named_expressions_;
};
}
