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
class Ident;
class Match;
class Return;
class Pattern;
class NodePart;
class EdgePart;

class TreeVisitorBase {
 public:
  // Start of the tree is a Query.
  virtual void PreVisit(Query& query) {}
  virtual void Visit(Query& query) = 0;
  virtual void PostVisit(Query& query) {}
  // Expressions
  virtual void PreVisit(Ident& ident) {}
  virtual void Visit(Ident& ident) = 0;
  virtual void PostVisit(Ident& ident) {}
  // Clauses
  virtual void PreVisit(Match& match) {}
  virtual void Visit(Match& match) = 0;
  virtual void PostVisit(Match& match) {}
  virtual void PreVisit(Return& ret) {}
  virtual void Visit(Return& ret) = 0;
  virtual void PostVisit(Return& ret) {}
  // Pattern and its subparts.
  virtual void PreVisit(Pattern& pattern) {}
  virtual void Visit(Pattern& pattern) = 0;
  virtual void PostVisit(Pattern& pattern) {}
  virtual void PreVisit(NodePart& node_part) {}
  virtual void Visit(NodePart& node_part) = 0;
  virtual void PostVisit(NodePart& node_part) {}
  virtual void PreVisit(EdgePart& edge_part) {}
  virtual void Visit(EdgePart& edge_part) = 0;
  virtual void PostVisit(EdgePart& edge_part) {}
};

class Tree {
public:
  Tree(const int uid) : uid_(uid) {}
  int uid() const { return uid_; }
  virtual void Accept(TreeVisitorBase& visitor) = 0;

private:
  const int uid_;
};

class Expr : public Tree {
public:
  virtual TypedValue Evaluate(Frame &, SymbolTable &) = 0;
};

class Ident : public Expr {
public:
  std::string identifier_;
  TypedValue Evaluate(Frame &frame, SymbolTable &symbol_table) override;
  void Accept(TreeVisitorBase& visitor) override {
    visitor.PreVisit(*this);
    visitor.Visit(*this);
    visitor.PostVisit(*this);
  }
};

class Part : public Tree {};

class NodePart : public Part {
public:
  Ident identifier_;
  // TODO: Mislav call GraphDb::label(label_name) to populate labels_!
  std::vector<GraphDb::Label> labels_;
  // TODO: properties
  void Accept(TreeVisitorBase& visitor) override {
    visitor.PreVisit(*this);
    identifier_.Accept(visitor);
    visitor.Visit(*this);
    visitor.PostVisit(*this);
  }
};

class EdgePart : public Part {
public:
  Ident identifier_;
  // TODO: finish this: properties, types...
  void Accept(TreeVisitorBase& visitor) override {
    visitor.PreVisit(*this);
    identifier_.Accept(visitor);
    visitor.Visit(*this);
    visitor.PostVisit(*this);
  }
};

class Clause : public Tree {};

class Pattern : public Tree {
public:
  std::vector<std::unique_ptr<Part>> node_parts_;
  void Accept(TreeVisitorBase& visitor) override {
    visitor.PreVisit(*this);
    for (auto& node_part : node_parts_) {
      node_part->Accept(visitor);
    }
    visitor.Visit(*this);
    visitor.PostVisit(*this);
  }
};

class Query : public Tree {
public:
  std::vector<std::unique_ptr<Clause>> clauses_;
  void Accept(TreeVisitorBase& visitor) override {
    visitor.PreVisit(*this);
    for (auto& clause : clauses_) {
      clause->Accept(visitor);
    }
    visitor.Visit(*this);
    visitor.PostVisit(*this);
  }
};

class Match : public Clause {
public:
  std::vector<std::unique_ptr<Pattern>> patterns_;
  void Accept(TreeVisitorBase& visitor) override {
    visitor.PreVisit(*this);
    for (auto& pattern : patterns_) {
      pattern->Accept(visitor);
    }
    visitor.Visit(*this);
    visitor.PostVisit(*this);
  }
};

class Return : public Clause {
public:
  std::vector<std::unique_ptr<Expr>> exprs_;
  void Accept(TreeVisitorBase& visitor) override {
    visitor.PreVisit(*this);
    for (auto& expr : exprs_) {
      expr->Accept(visitor);
    }
    visitor.Visit(*this);
    visitor.PostVisit(*this);
  }
};
}
