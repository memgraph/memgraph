#pragma once

#include <memory>
#include <vector>

#include "database/graph_db.hpp"
#include "query/backend/cpp/typed_value.hpp"

namespace query {

class Frame;
class SymbolTable;

// Forward declares for TreeVisitorBase
class Query;
class NamedExpr;
class Ident;
class Match;
class Return;
class Pattern;
class NodePart;
class EdgePart;

class TreeVisitorBase {
public:
  virtual ~TreeVisitorBase() {}
  // Start of the tree is a Query.
  virtual void PreVisit(Query &) {}
  virtual void Visit(Query &query) = 0;
  virtual void PostVisit(Query &) {}
  // Expressions
  virtual void PreVisit(NamedExpr &) {}
  virtual void Visit(NamedExpr &) = 0;
  virtual void PostVisit(NamedExpr &) {}
  virtual void PreVisit(Ident &) {}
  virtual void Visit(Ident &ident) = 0;
  virtual void PostVisit(Ident &) {}
  // Clauses
  virtual void PreVisit(Match &) {}
  virtual void Visit(Match &match) = 0;
  virtual void PostVisit(Match &) {}
  virtual void PreVisit(Return &) {}
  virtual void Visit(Return &ret) = 0;
  virtual void PostVisit(Return &) {}
  // Pattern and its subparts.
  virtual void PreVisit(Pattern &) {}
  virtual void Visit(Pattern &pattern) = 0;
  virtual void PostVisit(Pattern &) {}
  virtual void PreVisit(NodePart &) {}
  virtual void Visit(NodePart &node_part) = 0;
  virtual void PostVisit(NodePart &) {}
  virtual void PreVisit(EdgePart &) {}
  virtual void Visit(EdgePart &edge_part) = 0;
  virtual void PostVisit(EdgePart &) {}
};

class Tree {
public:
  Tree(int uid) : uid_(uid) {}
  int uid() const { return uid_; }
  virtual void Accept(TreeVisitorBase &visitor) = 0;

private:
  const int uid_;
};

class Expr : public Tree {
public:
  Expr(int uid) : Tree(uid) {}
  virtual TypedValue Evaluate(Frame &, SymbolTable &) = 0;
};

class Ident : public Expr {
public:
  Ident(int uid, const std::string &identifier)
      : Expr(uid), identifier_(identifier) {}

  TypedValue Evaluate(Frame &frame, SymbolTable &symbol_table) override;
  void Accept(TreeVisitorBase &visitor) override {
    visitor.PreVisit(*this);
    visitor.Visit(*this);
    visitor.PostVisit(*this);
  }

  std::string identifier_;
};

class NamedExpr : public Tree {
public:
  NamedExpr(int uid) : Tree(uid) {}
  void Evaluate(Frame &frame, SymbolTable &symbol_table);
  void Accept(TreeVisitorBase &visitor) override {
    visitor.PreVisit(*this);
    ident_->Accept(visitor);
    expr_->Accept(visitor);
    visitor.Visit(*this);
    visitor.PostVisit(*this);
  }

  std::shared_ptr<Ident> ident_;
  std::shared_ptr<Expr> expr_;
};

class Part : public Tree {
public:
  Part(int uid) : Tree(uid) {}
};

class NodePart : public Part {
public:
  NodePart(int uid) : Part(uid) {}
  void Accept(TreeVisitorBase &visitor) override {
    visitor.PreVisit(*this);
    identifier_->Accept(visitor);
    visitor.Visit(*this);
    visitor.PostVisit(*this);
  }

  std::shared_ptr<Ident> identifier_;
  std::vector<GraphDb::Label> labels_;
};

class EdgePart : public Part {
public:
  enum class Direction { LEFT, RIGHT, BOTH };

  EdgePart(int uid) : Part(uid) {}
  void Accept(TreeVisitorBase &visitor) override {
    visitor.PreVisit(*this);
    identifier_->Accept(visitor);
    visitor.Visit(*this);
    visitor.PostVisit(*this);
  }

  Direction direction = Direction::BOTH;
  std::shared_ptr<Ident> identifier_;
};

class Clause : public Tree {
public:
  Clause(int uid) : Tree(uid) {}
};

class Pattern : public Tree {
public:
  Pattern(int uid) : Tree(uid) {}
  void Accept(TreeVisitorBase &visitor) override {
    visitor.PreVisit(*this);
    for (auto &part : parts_) {
      part->Accept(visitor);
    }
    visitor.Visit(*this);
    visitor.PostVisit(*this);
  }
  std::shared_ptr<Ident> identifier_;
  std::vector<std::shared_ptr<Part>> parts_;
};

class Query : public Tree {
public:
  Query(int uid) : Tree(uid) {}
  void Accept(TreeVisitorBase &visitor) override {
    visitor.PreVisit(*this);
    for (auto &clause : clauses_) {
      clause->Accept(visitor);
    }
    visitor.Visit(*this);
    visitor.PostVisit(*this);
  }
  std::vector<std::shared_ptr<Clause>> clauses_;
};

class Match : public Clause {
public:
  Match(int uid) : Clause(uid) {}
  std::vector<std::shared_ptr<Pattern>> patterns_;
  void Accept(TreeVisitorBase &visitor) override {
    visitor.PreVisit(*this);
    for (auto &pattern : patterns_) {
      pattern->Accept(visitor);
    }
    visitor.Visit(*this);
    visitor.PostVisit(*this);
  }
};

class Return : public Clause {
public:
  Return(int uid) : Clause(uid) {}
  void Accept(TreeVisitorBase &visitor) override {
    visitor.PreVisit(*this);
    for (auto &expr : named_exprs_) {
      expr->Accept(visitor);
    }
    visitor.Visit(*this);
    visitor.PostVisit(*this);
  }

  std::shared_ptr<Ident> identifier_;
  std::vector<std::shared_ptr<NamedExpr>> named_exprs_;
};
}
