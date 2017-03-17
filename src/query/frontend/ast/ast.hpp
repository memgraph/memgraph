#pragma once

#include <map>
#include <memory>
#include <vector>

#include "database/graph_db.hpp"
#include "query/frontend/ast/ast_visitor.hpp"
#include "utils/visitor/visitable.hpp"

namespace query {

class AstTreeStorage;

class Tree : public ::utils::Visitable<TreeVisitorBase> {
  friend class AstTreeStorage;

 public:
  int uid() const { return uid_; }

 protected:
  Tree(int uid) : uid_(uid) {}

 private:
  const int uid_;
};

class Expression : public Tree {
 protected:
  Expression(int uid) : Tree(uid) {}
};

class Identifier : public Expression {
  friend class AstTreeStorage;

 public:
  DEFVISITABLE(TreeVisitorBase);
  std::string name_;

 protected:
  Identifier(int uid, const std::string &name) : Expression(uid), name_(name) {}
};

class PropertyLookup : public Expression {
  friend class AstTreeStorage;

 public:
  void Accept(TreeVisitorBase &visitor) override {
    visitor.Visit(*this);
    expression_->Accept(visitor);
    visitor.PostVisit(*this);
  }

  Expression *expression_ = nullptr;
  GraphDb::Property property_ = nullptr;
  // TODO potential problem: property lookups are allowed on both map literals
  // and records, but map literals have strings as keys and records have
  // GraphDb::Property
  //
  // possible solution: store both string and GraphDb::Property here and choose
  // between the two depending on Expression result

 protected:
  PropertyLookup(int uid, Expression *expression, GraphDb::Property property)
      : Expression(uid), expression_(expression), property_(property) {}
};

class NamedExpression : public Tree {
  friend class AstTreeStorage;

 public:
  void Accept(TreeVisitorBase &visitor) override {
    visitor.Visit(*this);
    expression_->Accept(visitor);
    visitor.PostVisit(*this);
  }

  std::string name_;
  Expression *expression_ = nullptr;

 protected:
  NamedExpression(int uid) : Tree(uid) {}
  NamedExpression(int uid, std::string name, Expression *expression)
      : Tree(uid), name_(name), expression_(expression) {}
};

class PatternAtom : public Tree {
  friend class AstTreeStorage;

 public:
  Identifier *identifier_ = nullptr;

 protected:
  PatternAtom(int uid) : Tree(uid) {}
  PatternAtom(int uid, Identifier *identifier)
      : Tree(uid), identifier_(identifier) {}
};

class NodeAtom : public PatternAtom {
  friend class AstTreeStorage;

 public:
  void Accept(TreeVisitorBase &visitor) override {
    visitor.Visit(*this);
    identifier_->Accept(visitor);
    visitor.PostVisit(*this);
  }

  std::vector<GraphDb::Label> labels_;
  // TODO: change to unordered_map
  std::map<GraphDb::Property, Expression *> properties_;

 protected:
  using PatternAtom::PatternAtom;
};

class EdgeAtom : public PatternAtom {
  friend class AstTreeStorage;

 public:
  enum class Direction { LEFT, RIGHT, BOTH };

  void Accept(TreeVisitorBase &visitor) override {
    visitor.Visit(*this);
    identifier_->Accept(visitor);
    visitor.PostVisit(*this);
  }

  Direction direction_ = Direction::BOTH;
  std::vector<GraphDb::EdgeType> types_;
  // TODO: change to unordered_map
  std::map<GraphDb::Property, Expression *> properties_;

 protected:
  using PatternAtom::PatternAtom;
};

class Clause : public Tree {
  friend class AstTreeStorage;

 public:
  Clause(int uid) : Tree(uid) {}
};

class Pattern : public Tree {
  friend class AstTreeStorage;

 public:
  void Accept(TreeVisitorBase &visitor) override {
    visitor.Visit(*this);
    for (auto &part : atoms_) {
      part->Accept(visitor);
    }
    visitor.PostVisit(*this);
  }
  Identifier *identifier_ = nullptr;
  std::vector<PatternAtom *> atoms_;

 protected:
  Pattern(int uid) : Tree(uid) {}
};

class Query : public Tree {
  friend class AstTreeStorage;

 public:
  void Accept(TreeVisitorBase &visitor) override {
    visitor.Visit(*this);
    for (auto &clause : clauses_) {
      clause->Accept(visitor);
    }
    visitor.PostVisit(*this);
  }
  std::vector<Clause *> clauses_;

 protected:
  Query(int uid) : Tree(uid) {}
};

class Create : public Clause {
  friend class AstTreeStorage;

 public:
  Create(int uid) : Clause(uid) {}
  std::vector<Pattern *> patterns_;
  void Accept(TreeVisitorBase &visitor) override {
    visitor.Visit(*this);
    for (auto &pattern : patterns_) {
      pattern->Accept(visitor);
    }
    visitor.PostVisit(*this);
  }
};

class Match : public Clause {
  friend class AstTreeStorage;

 public:
  std::vector<Pattern *> patterns_;
  void Accept(TreeVisitorBase &visitor) override {
    visitor.Visit(*this);
    for (auto &pattern : patterns_) {
      pattern->Accept(visitor);
    }
    visitor.PostVisit(*this);
  }

 protected:
  Match(int uid) : Clause(uid) {}
};

class Return : public Clause {
  friend class AstTreeStorage;

 public:
  void Accept(TreeVisitorBase &visitor) override {
    visitor.Visit(*this);
    for (auto &expr : named_expressions_) {
      expr->Accept(visitor);
    }
    visitor.PostVisit(*this);
  }
  std::vector<NamedExpression *> named_expressions_;

 protected:
  Return(int uid) : Clause(uid) {}
};

// It would be better to call this AstTree, but we already have a class Tree,
// which could be renamed to Node or AstTreeNode, but we also have a class
// called NodeAtom...
class AstTreeStorage {
  friend class AstTreeStorage;

 public:
  AstTreeStorage() { storage_.emplace_back(new Query(next_uid_++)); }
  AstTreeStorage(const AstTreeStorage &) = delete;
  AstTreeStorage &operator=(const AstTreeStorage &) = delete;

  template <typename T, typename... Args>
  T *Create(Args &&... args) {
    // Never call create for a Query. Call query() instead.
    static_assert(!std::is_same<T, Query>::value, "Call query() instead");
    // TODO: use std::forward here
    T *p = new T(next_uid_++, args...);
    storage_.emplace_back(p);
    return p;
  }

  Query *query() { return dynamic_cast<Query *>(storage_[0].get()); }

 private:
  int next_uid_ = 0;
  std::vector<std::unique_ptr<Tree>> storage_;
};
}
