#pragma once

#include <memory>
#include <vector>

#include "database/graph_db_accessor.hpp"
#include "query/frontend/ast/ast.hpp"
#include "query/frontend/interpret/interpret.hpp"
#include "query/frontend/typecheck/symbol_table.hpp"

namespace query {
class Cursor {
 public:
  virtual bool pull(Frame&, SymbolTable&) = 0;
  virtual ~Cursor() {}
};

class LogicalOperator {
 public:
  auto children() { return children_; };
  virtual uptr<Cursor> MakeCursor(GraphDbAccessor db) = 0;
  virtual ~LogicalOperator() {}

 protected:
  std::vector<std::shared_ptr<LogicalOperator>> children_;
};

class ScanAll : public LogicalOperator {
 public:
  ScanAll(sptr<NodePart> node_part) : node_part_(node_part) {}

 private:
  class ScanAllCursor : public Cursor {
   public:
    ScanAllCursor(ScanAll& parent, GraphDbAccessor db)
        : parent_(parent), db_(db), vertices_(db.vertices()) {}
    bool pull(Frame& frame, SymbolTable& symbol_table) override {
      while (vertices_ != vertices_.end()) {
        auto& vertex = *vertices_++;
        if (evaluate(frame, symbol_table, vertex)) {
          return true;
        }
      }
      return false;
    }

   private:
    ScanAll& parent_;
    GraphDbAccessor db_;
    decltype(db_.vertices()) vertices_;

    bool evaluate(Frame& frame, SymbolTable& symbol_table,
                  VertexAccessor& vertex) {
      auto node_part = parent_.node_part_;
      for (auto label : node_part->labels_) {
        if (!vertex.has_label(label)) return false;
      }
      frame[symbol_table[parent_.node_part_->identifier_].position_] = vertex;
      return true;
    }
  };

 public:
  uptr<Cursor> MakeCursor(GraphDbAccessor db) override {
    return new ScanAllCursor(*this, db);
  }

  friend class ScanAll::ScanAllCursor;
  sptr<NodePart> node_part_;
};

class Produce : public LogicalOperator {
 public:
  Produce(sptr<LogicalOperator> op, std::vector<sptr<Expr>> exprs)
      : exprs_(exprs) {
    children_.emplace_back(op);
  }

 private:
  std::vector<sptr<Expr>> exprs_;
};
}
