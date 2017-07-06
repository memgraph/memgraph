#pragma once

#include <map>
#include <memory>
#include <unordered_map>
#include <vector>

#include "database/graph_db.hpp"
#include "database/graph_db_datatypes.hpp"
#include "query/frontend/ast/ast_visitor.hpp"
#include "query/parameters.hpp"
#include "query/typed_value.hpp"
#include "utils/assert.hpp"

namespace query {

#define CLONE_BINARY_EXPRESSION                                              \
  auto Clone(AstTreeStorage &storage) const->std::remove_const<              \
      std::remove_pointer<decltype(this)>::type>::type * override {          \
    return storage.Create<                                                   \
        std::remove_cv<std::remove_reference<decltype(*this)>::type>::type>( \
        expression1_->Clone(storage), expression2_->Clone(storage));         \
  }
#define CLONE_UNARY_EXPRESSION                                               \
  auto Clone(AstTreeStorage &storage) const->std::remove_const<              \
      std::remove_pointer<decltype(this)>::type>::type * override {          \
    return storage.Create<                                                   \
        std::remove_cv<std::remove_reference<decltype(*this)>::type>::type>( \
        expression_->Clone(storage));                                        \
  }

class Tree;

// It would be better to call this AstTree, but we already have a class Tree,
// which could be renamed to Node or AstTreeNode, but we also have a class
// called NodeAtom...
class AstTreeStorage {
 public:
  AstTreeStorage();
  AstTreeStorage(const AstTreeStorage &) = delete;
  AstTreeStorage &operator=(const AstTreeStorage &) = delete;
  AstTreeStorage(AstTreeStorage &&) = default;
  AstTreeStorage &operator=(AstTreeStorage &&) = default;

  template <typename T, typename... Args>
  T *Create(Args &&... args) {
    // Never call create for a Query. Call query() instead.
    static_assert(!std::is_same<T, Query>::value, "Call query() instead");
    T *p = new T(next_uid_++, std::forward<Args>(args)...);
    storage_.emplace_back(p);
    return p;
  }

  Query *query() const;

 private:
  int next_uid_ = 0;
  std::vector<std::unique_ptr<Tree>> storage_;
};

class Tree : public ::utils::Visitable<HierarchicalTreeVisitor>,
             ::utils::Visitable<TreeVisitor<TypedValue>> {
  friend class AstTreeStorage;

 public:
  using ::utils::Visitable<HierarchicalTreeVisitor>::Accept;
  using ::utils::Visitable<TreeVisitor<TypedValue>>::Accept;

  int uid() const { return uid_; }

  virtual Tree *Clone(AstTreeStorage &storage) const = 0;

 protected:
  Tree(int uid) : uid_(uid) {}

 private:
  const int uid_;
};

class Expression : public Tree {
  friend class AstTreeStorage;

 public:
  Expression *Clone(AstTreeStorage &storage) const override = 0;

 protected:
  Expression(int uid) : Tree(uid) {}
};

class BinaryOperator : public Expression {
  friend class AstTreeStorage;

 public:
  Expression *expression1_ = nullptr;
  Expression *expression2_ = nullptr;

  BinaryOperator *Clone(AstTreeStorage &storage) const override = 0;

 protected:
  BinaryOperator(int uid) : Expression(uid) {}
  BinaryOperator(int uid, Expression *expression1, Expression *expression2)
      : Expression(uid), expression1_(expression1), expression2_(expression2) {}
};

class UnaryOperator : public Expression {
  friend class AstTreeStorage;

 public:
  Expression *expression_ = nullptr;

  UnaryOperator *Clone(AstTreeStorage &storage) const override = 0;

 protected:
  UnaryOperator(int uid) : Expression(uid) {}
  UnaryOperator(int uid, Expression *expression)
      : Expression(uid), expression_(expression) {}
};

class OrOperator : public BinaryOperator {
  friend class AstTreeStorage;

 public:
  DEFVISITABLE(TreeVisitor<TypedValue>);
  bool Accept(HierarchicalTreeVisitor &visitor) override {
    if (visitor.PreVisit(*this)) {
      expression1_->Accept(visitor) && expression2_->Accept(visitor);
    }
    return visitor.PostVisit(*this);
  }
  CLONE_BINARY_EXPRESSION;

 protected:
  using BinaryOperator::BinaryOperator;
};

class XorOperator : public BinaryOperator {
  friend class AstTreeStorage;

 public:
  DEFVISITABLE(TreeVisitor<TypedValue>);
  bool Accept(HierarchicalTreeVisitor &visitor) override {
    if (visitor.PreVisit(*this)) {
      expression1_->Accept(visitor) && expression2_->Accept(visitor);
    }
    return visitor.PostVisit(*this);
  }
  CLONE_BINARY_EXPRESSION;

 protected:
  using BinaryOperator::BinaryOperator;
};

class AndOperator : public BinaryOperator {
  friend class AstTreeStorage;

 public:
  DEFVISITABLE(TreeVisitor<TypedValue>);
  bool Accept(HierarchicalTreeVisitor &visitor) override {
    if (visitor.PreVisit(*this)) {
      expression1_->Accept(visitor) && expression2_->Accept(visitor);
    }
    return visitor.PostVisit(*this);
  }
  CLONE_BINARY_EXPRESSION;

 protected:
  using BinaryOperator::BinaryOperator;
};

// This is separate operator so that we can implement different short-circuiting
// semantics than regular AndOperator. At this point CypherMainVisitor shouldn't
// concern itself with this, and should constructor only AndOperator-s. This is
// used in query planner at the moment.
class FilterAndOperator : public BinaryOperator {
  friend class AstTreeStorage;

 public:
  DEFVISITABLE(TreeVisitor<TypedValue>);
  bool Accept(HierarchicalTreeVisitor &visitor) override {
    if (visitor.PreVisit(*this)) {
      expression1_->Accept(visitor) && expression2_->Accept(visitor);
    }
    return visitor.PostVisit(*this);
  }
  CLONE_BINARY_EXPRESSION;

 protected:
  using BinaryOperator::BinaryOperator;
};

class AdditionOperator : public BinaryOperator {
  friend class AstTreeStorage;

 public:
  DEFVISITABLE(TreeVisitor<TypedValue>);
  bool Accept(HierarchicalTreeVisitor &visitor) override {
    if (visitor.PreVisit(*this)) {
      expression1_->Accept(visitor) && expression2_->Accept(visitor);
    }
    return visitor.PostVisit(*this);
  }
  CLONE_BINARY_EXPRESSION;

 protected:
  using BinaryOperator::BinaryOperator;
};

class SubtractionOperator : public BinaryOperator {
  friend class AstTreeStorage;

 public:
  DEFVISITABLE(TreeVisitor<TypedValue>);
  bool Accept(HierarchicalTreeVisitor &visitor) override {
    if (visitor.PreVisit(*this)) {
      expression1_->Accept(visitor) && expression2_->Accept(visitor);
    }
    return visitor.PostVisit(*this);
  }
  CLONE_BINARY_EXPRESSION;

 protected:
  using BinaryOperator::BinaryOperator;
};

class MultiplicationOperator : public BinaryOperator {
  friend class AstTreeStorage;

 public:
  DEFVISITABLE(TreeVisitor<TypedValue>);
  bool Accept(HierarchicalTreeVisitor &visitor) override {
    if (visitor.PreVisit(*this)) {
      expression1_->Accept(visitor) && expression2_->Accept(visitor);
    }
    return visitor.PostVisit(*this);
  }
  CLONE_BINARY_EXPRESSION;

 protected:
  using BinaryOperator::BinaryOperator;
};

class DivisionOperator : public BinaryOperator {
  friend class AstTreeStorage;

 public:
  DEFVISITABLE(TreeVisitor<TypedValue>);
  bool Accept(HierarchicalTreeVisitor &visitor) override {
    if (visitor.PreVisit(*this)) {
      expression1_->Accept(visitor) && expression2_->Accept(visitor);
    }
    return visitor.PostVisit(*this);
  }
  CLONE_BINARY_EXPRESSION;

 protected:
  using BinaryOperator::BinaryOperator;
};

class ModOperator : public BinaryOperator {
  friend class AstTreeStorage;

 public:
  DEFVISITABLE(TreeVisitor<TypedValue>);
  bool Accept(HierarchicalTreeVisitor &visitor) override {
    if (visitor.PreVisit(*this)) {
      expression1_->Accept(visitor) && expression2_->Accept(visitor);
    }
    return visitor.PostVisit(*this);
  }
  CLONE_BINARY_EXPRESSION;

 protected:
  using BinaryOperator::BinaryOperator;
};

class NotEqualOperator : public BinaryOperator {
  friend class AstTreeStorage;

 public:
  DEFVISITABLE(TreeVisitor<TypedValue>);
  bool Accept(HierarchicalTreeVisitor &visitor) override {
    if (visitor.PreVisit(*this)) {
      expression1_->Accept(visitor) && expression2_->Accept(visitor);
    }
    return visitor.PostVisit(*this);
  }
  CLONE_BINARY_EXPRESSION;

 protected:
  using BinaryOperator::BinaryOperator;
};

class EqualOperator : public BinaryOperator {
  friend class AstTreeStorage;

 public:
  DEFVISITABLE(TreeVisitor<TypedValue>);
  bool Accept(HierarchicalTreeVisitor &visitor) override {
    if (visitor.PreVisit(*this)) {
      expression1_->Accept(visitor) && expression2_->Accept(visitor);
    }
    return visitor.PostVisit(*this);
  }
  CLONE_BINARY_EXPRESSION;

 protected:
  using BinaryOperator::BinaryOperator;
};

class LessOperator : public BinaryOperator {
  friend class AstTreeStorage;

 public:
  DEFVISITABLE(TreeVisitor<TypedValue>);
  bool Accept(HierarchicalTreeVisitor &visitor) override {
    if (visitor.PreVisit(*this)) {
      expression1_->Accept(visitor) && expression2_->Accept(visitor);
    }
    return visitor.PostVisit(*this);
  }
  CLONE_BINARY_EXPRESSION;

 protected:
  using BinaryOperator::BinaryOperator;
};

class GreaterOperator : public BinaryOperator {
  friend class AstTreeStorage;

 public:
  DEFVISITABLE(TreeVisitor<TypedValue>);
  bool Accept(HierarchicalTreeVisitor &visitor) override {
    if (visitor.PreVisit(*this)) {
      expression1_->Accept(visitor) && expression2_->Accept(visitor);
    }
    return visitor.PostVisit(*this);
  }
  CLONE_BINARY_EXPRESSION;

 protected:
  using BinaryOperator::BinaryOperator;
};

class LessEqualOperator : public BinaryOperator {
  friend class AstTreeStorage;

 public:
  DEFVISITABLE(TreeVisitor<TypedValue>);
  bool Accept(HierarchicalTreeVisitor &visitor) override {
    if (visitor.PreVisit(*this)) {
      expression1_->Accept(visitor) && expression2_->Accept(visitor);
    }
    return visitor.PostVisit(*this);
  }
  CLONE_BINARY_EXPRESSION;

 protected:
  using BinaryOperator::BinaryOperator;
};

class GreaterEqualOperator : public BinaryOperator {
  friend class AstTreeStorage;

 public:
  DEFVISITABLE(TreeVisitor<TypedValue>);
  bool Accept(HierarchicalTreeVisitor &visitor) override {
    if (visitor.PreVisit(*this)) {
      expression1_->Accept(visitor) && expression2_->Accept(visitor);
    }
    return visitor.PostVisit(*this);
  }
  CLONE_BINARY_EXPRESSION;

 protected:
  using BinaryOperator::BinaryOperator;
};

class InListOperator : public BinaryOperator {
  friend class AstTreeStorage;

 public:
  DEFVISITABLE(TreeVisitor<TypedValue>);
  bool Accept(HierarchicalTreeVisitor &visitor) override {
    if (visitor.PreVisit(*this)) {
      expression1_->Accept(visitor) && expression2_->Accept(visitor);
    }
    return visitor.PostVisit(*this);
  }
  CLONE_BINARY_EXPRESSION;

 protected:
  using BinaryOperator::BinaryOperator;
};

class ListIndexingOperator : public BinaryOperator {
  friend class AstTreeStorage;

 public:
  DEFVISITABLE(TreeVisitor<TypedValue>);
  bool Accept(HierarchicalTreeVisitor &visitor) override {
    if (visitor.PreVisit(*this)) {
      expression1_->Accept(visitor) && expression2_->Accept(visitor);
    }
    return visitor.PostVisit(*this);
  }
  CLONE_BINARY_EXPRESSION;

 protected:
  using BinaryOperator::BinaryOperator;
};

class ListSlicingOperator : public Expression {
  friend class AstTreeStorage;

 public:
  DEFVISITABLE(TreeVisitor<TypedValue>);
  bool Accept(HierarchicalTreeVisitor &visitor) override {
    if (visitor.PreVisit(*this)) {
      bool cont = list_->Accept(visitor);
      if (cont && lower_bound_) {
        cont = lower_bound_->Accept(visitor);
      }
      if (cont && upper_bound_) {
        upper_bound_->Accept(visitor);
      }
    }
    return visitor.PostVisit(*this);
  }

  ListSlicingOperator *Clone(AstTreeStorage &storage) const override {
    return storage.Create<ListSlicingOperator>(
        list_->Clone(storage),
        lower_bound_ ? lower_bound_->Clone(storage) : nullptr,
        upper_bound_ ? upper_bound_->Clone(storage) : nullptr);
  }

  Expression *list_;
  Expression *lower_bound_;
  Expression *upper_bound_;

 protected:
  ListSlicingOperator(int uid, Expression *list, Expression *lower_bound,
                      Expression *upper_bound)
      : Expression(uid),
        list_(list),
        lower_bound_(lower_bound),
        upper_bound_(upper_bound) {}
};

class NotOperator : public UnaryOperator {
  friend class AstTreeStorage;

 public:
  DEFVISITABLE(TreeVisitor<TypedValue>);
  bool Accept(HierarchicalTreeVisitor &visitor) override {
    if (visitor.PreVisit(*this)) {
      expression_->Accept(visitor);
    }
    return visitor.PostVisit(*this);
  }
  CLONE_UNARY_EXPRESSION;

 protected:
  using UnaryOperator::UnaryOperator;
};

class UnaryPlusOperator : public UnaryOperator {
  friend class AstTreeStorage;

 public:
  DEFVISITABLE(TreeVisitor<TypedValue>);
  bool Accept(HierarchicalTreeVisitor &visitor) override {
    if (visitor.PreVisit(*this)) {
      expression_->Accept(visitor);
    }
    return visitor.PostVisit(*this);
  }
  CLONE_UNARY_EXPRESSION;

 protected:
  using UnaryOperator::UnaryOperator;
};

class UnaryMinusOperator : public UnaryOperator {
  friend class AstTreeStorage;

 public:
  DEFVISITABLE(TreeVisitor<TypedValue>);
  bool Accept(HierarchicalTreeVisitor &visitor) override {
    if (visitor.PreVisit(*this)) {
      expression_->Accept(visitor);
    }
    return visitor.PostVisit(*this);
  }
  CLONE_UNARY_EXPRESSION;

 protected:
  using UnaryOperator::UnaryOperator;
};

class IsNullOperator : public UnaryOperator {
  friend class AstTreeStorage;

 public:
  DEFVISITABLE(TreeVisitor<TypedValue>);
  bool Accept(HierarchicalTreeVisitor &visitor) override {
    if (visitor.PreVisit(*this)) {
      expression_->Accept(visitor);
    }
    return visitor.PostVisit(*this);
  }
  CLONE_UNARY_EXPRESSION;

 protected:
  using UnaryOperator::UnaryOperator;
};

class BaseLiteral : public Expression {
  friend class AstTreeStorage;

 public:
  BaseLiteral *Clone(AstTreeStorage &storage) const override = 0;

 protected:
  BaseLiteral(int uid) : Expression(uid) {}
};

class PrimitiveLiteral : public BaseLiteral {
  friend class AstTreeStorage;

 public:
  DEFVISITABLE(TreeVisitor<TypedValue>);
  DEFVISITABLE(HierarchicalTreeVisitor);

  PrimitiveLiteral *Clone(AstTreeStorage &storage) const override {
    return storage.Create<PrimitiveLiteral>(value_, token_position_);
  }

  TypedValue value_;
  // This field contains token position of literal used to create
  // PrimitiveLiteral object. If PrimitiveLiteral object is not created from
  // query leave its value at -1.
  int token_position_ = -1;

 protected:
  PrimitiveLiteral(int uid) : BaseLiteral(uid) {}
  template <typename T>
  PrimitiveLiteral(int uid, T value) : BaseLiteral(uid), value_(value) {}
  template <typename T>
  PrimitiveLiteral(int uid, T value, int token_position)
      : BaseLiteral(uid), value_(value), token_position_(token_position) {}
};

class ListLiteral : public BaseLiteral {
  friend class AstTreeStorage;

 public:
  DEFVISITABLE(TreeVisitor<TypedValue>);
  bool Accept(HierarchicalTreeVisitor &visitor) override {
    if (visitor.PreVisit(*this)) {
      for (auto expr_ptr : elements_)
        if (!expr_ptr->Accept(visitor)) break;
    }
    return visitor.PostVisit(*this);
  }

  ListLiteral *Clone(AstTreeStorage &storage) const override {
    auto *list = storage.Create<ListLiteral>();
    for (auto *element : elements_) {
      list->elements_.push_back(element->Clone(storage));
    }
    return list;
  }

  std::vector<Expression *> elements_;

 protected:
  ListLiteral(int uid) : BaseLiteral(uid) {}
  ListLiteral(int uid, const std::vector<Expression *> &elements)
      : BaseLiteral(uid), elements_(elements) {}
};

class Identifier : public Expression {
  friend class AstTreeStorage;

 public:
  DEFVISITABLE(TreeVisitor<TypedValue>);
  DEFVISITABLE(HierarchicalTreeVisitor);

  Identifier *Clone(AstTreeStorage &storage) const override {
    return storage.Create<Identifier>(name_, user_declared_);
  }

  std::string name_;
  bool user_declared_ = true;

 protected:
  Identifier(int uid, const std::string &name) : Expression(uid), name_(name) {}
  Identifier(int uid, const std::string &name, bool user_declared)
      : Expression(uid), name_(name), user_declared_(user_declared) {}
};

class PropertyLookup : public Expression {
  friend class AstTreeStorage;

 public:
  DEFVISITABLE(TreeVisitor<TypedValue>);
  bool Accept(HierarchicalTreeVisitor &visitor) override {
    if (visitor.PreVisit(*this)) {
      expression_->Accept(visitor);
    }
    return visitor.PostVisit(*this);
  }

  PropertyLookup *Clone(AstTreeStorage &storage) const override {
    return storage.Create<PropertyLookup>(expression_->Clone(storage),
                                          property_);
  }

  Expression *expression_ = nullptr;
  GraphDbTypes::Property property_ = nullptr;
  // TODO potential problem: property lookups are allowed on both map literals
  // and records, but map literals have strings as keys and records have
  // GraphDbTypes::Property
  //
  // possible solution: store both string and GraphDbTypes::Property here and
  // choose
  // between the two depending on Expression result

 protected:
  PropertyLookup(int uid, Expression *expression,
                 GraphDbTypes::Property property)
      : Expression(uid), expression_(expression), property_(property) {}
};

class LabelsTest : public Expression {
  friend class AstTreeStorage;

 public:
  DEFVISITABLE(TreeVisitor<TypedValue>);
  bool Accept(HierarchicalTreeVisitor &visitor) override {
    if (visitor.PreVisit(*this)) {
      expression_->Accept(visitor);
    }
    return visitor.PostVisit(*this);
  }

  LabelsTest *Clone(AstTreeStorage &storage) const override {
    return storage.Create<LabelsTest>(expression_->Clone(storage), labels_);
  }

  Expression *expression_ = nullptr;
  std::vector<GraphDbTypes::Label> labels_;

 protected:
  LabelsTest(int uid, Expression *expression,
             std::vector<GraphDbTypes::Label> labels)
      : Expression(uid), expression_(expression), labels_(labels) {}
};

class EdgeTypeTest : public Expression {
  friend class AstTreeStorage;

 public:
  DEFVISITABLE(TreeVisitor<TypedValue>);
  bool Accept(HierarchicalTreeVisitor &visitor) override {
    if (visitor.PreVisit(*this)) {
      expression_->Accept(visitor);
    }
    return visitor.PostVisit(*this);
  }

  EdgeTypeTest *Clone(AstTreeStorage &storage) const override {
    return storage.Create<EdgeTypeTest>(expression_->Clone(storage),
                                        edge_types_);
  }

  Expression *expression_ = nullptr;
  std::vector<GraphDbTypes::EdgeType> edge_types_;

 protected:
  EdgeTypeTest(int uid, Expression *expression,
               std::vector<GraphDbTypes::EdgeType> edge_types)
      : Expression(uid), expression_(expression), edge_types_(edge_types) {
    debug_assert(edge_types.size(),
                 "EdgeTypeTest must have at least one edge_type");
  }
};

class Function : public Expression {
  friend class AstTreeStorage;

 public:
  DEFVISITABLE(TreeVisitor<TypedValue>);
  bool Accept(HierarchicalTreeVisitor &visitor) override {
    if (visitor.PreVisit(*this)) {
      for (auto *argument : arguments_) {
        if (!argument->Accept(visitor)) break;
      }
    }
    return visitor.PostVisit(*this);
  }

  // TODO: Think if there are any problems if function_ is mutable.
  Function *Clone(AstTreeStorage &storage) const override {
    std::vector<Expression *> arguments;
    for (auto *argument : arguments_) {
      arguments.push_back(argument->Clone(storage));
    }
    return storage.Create<Function>(function_, arguments);
  }

  std::function<TypedValue(const std::vector<TypedValue> &, GraphDbAccessor &)>
      function_;
  std::vector<Expression *> arguments_;

 protected:
  Function(int uid, std::function<TypedValue(const std::vector<TypedValue> &,
                                             GraphDbAccessor &)>
                        function,
           const std::vector<Expression *> &arguments)
      : Expression(uid), function_(function), arguments_(arguments) {}
};

class Aggregation : public UnaryOperator {
  friend class AstTreeStorage;

 public:
  enum class Op { COUNT, MIN, MAX, SUM, AVG, COLLECT };
  static const constexpr char *const kCount = "COUNT";
  static const constexpr char *const kMin = "MIN";
  static const constexpr char *const kMax = "MAX";
  static const constexpr char *const kSum = "SUM";
  static const constexpr char *const kAvg = "AVG";
  static const constexpr char *const kCollect = "COLLECT";

  DEFVISITABLE(TreeVisitor<TypedValue>);
  bool Accept(HierarchicalTreeVisitor &visitor) override {
    if (visitor.PreVisit(*this)) {
      if (expression_) {
        expression_->Accept(visitor);
      }
    }
    return visitor.PostVisit(*this);
  }

  Aggregation *Clone(AstTreeStorage &storage) const override {
    return storage.Create<Aggregation>(
        expression_ ? expression_->Clone(storage) : nullptr, op_);
  }

  Op op_;

 protected:
  Aggregation(int uid, Expression *expression, Op op)
      : UnaryOperator(uid, expression), op_(op) {
    // COUNT without expression denotes COUNT(*) in cypher.
    debug_assert(expression || op == Aggregation::Op::COUNT,
                 "All aggregations, except COUNT require expression");
  }
};

class NamedExpression : public Tree {
  friend class AstTreeStorage;

 public:
  DEFVISITABLE(TreeVisitor<TypedValue>);
  bool Accept(HierarchicalTreeVisitor &visitor) override {
    if (visitor.PreVisit(*this)) {
      expression_->Accept(visitor);
    }
    return visitor.PostVisit(*this);
  }

  NamedExpression *Clone(AstTreeStorage &storage) const override {
    return storage.Create<NamedExpression>(name_, expression_->Clone(storage),
                                           token_position_);
  }

  std::string name_;
  Expression *expression_ = nullptr;
  // This field contains token position of first token in named expression
  // used to create name_. If NamedExpression object is not created from
  // query or it is aliased leave this value at -1.
  int token_position_ = -1;

 protected:
  NamedExpression(int uid) : Tree(uid) {}
  NamedExpression(int uid, const std::string &name) : Tree(uid), name_(name) {}
  NamedExpression(int uid, const std::string &name, Expression *expression)
      : Tree(uid), name_(name), expression_(expression) {}
  NamedExpression(int uid, const std::string &name, Expression *expression,
                  int token_position)
      : Tree(uid),
        name_(name),
        expression_(expression),
        token_position_(token_position) {}
};

class PatternAtom : public Tree {
  friend class AstTreeStorage;

 public:
  Identifier *identifier_ = nullptr;

  PatternAtom *Clone(AstTreeStorage &storage) const override = 0;

 protected:
  PatternAtom(int uid) : Tree(uid) {}
  PatternAtom(int uid, Identifier *identifier)
      : Tree(uid), identifier_(identifier) {}
};

class NodeAtom : public PatternAtom {
  friend class AstTreeStorage;

 public:
  DEFVISITABLE(TreeVisitor<TypedValue>);
  bool Accept(HierarchicalTreeVisitor &visitor) override {
    if (visitor.PreVisit(*this)) {
      bool cont = identifier_->Accept(visitor);
      for (auto &property : properties_) {
        if (cont) {
          cont = property.second->Accept(visitor);
        }
      }
    }
    return visitor.PostVisit(*this);
  }

  NodeAtom *Clone(AstTreeStorage &storage) const override {
    auto *node_atom = storage.Create<NodeAtom>(identifier_->Clone(storage));
    node_atom->labels_ = labels_;
    for (auto property : properties_) {
      node_atom->properties_[property.first] = property.second->Clone(storage);
    }
    return node_atom;
  }

  std::vector<GraphDbTypes::Label> labels_;
  // TODO: change to unordered_map
  std::map<GraphDbTypes::Property, Expression *> properties_;

 protected:
  using PatternAtom::PatternAtom;
};

class EdgeAtom : public PatternAtom {
  friend class AstTreeStorage;

 public:
  enum class Direction { IN, OUT, BOTH };

  DEFVISITABLE(TreeVisitor<TypedValue>);
  bool Accept(HierarchicalTreeVisitor &visitor) override {
    if (visitor.PreVisit(*this)) {
      bool cont = identifier_->Accept(visitor);
      for (auto &property : properties_) {
        if (cont) {
          cont = property.second->Accept(visitor);
        }
      }
    }
    return visitor.PostVisit(*this);
  }

  EdgeAtom *Clone(AstTreeStorage &storage) const override {
    auto *edge_atom = storage.Create<EdgeAtom>(identifier_->Clone(storage));
    edge_atom->direction_ = direction_;
    edge_atom->edge_types_ = edge_types_;
    for (auto property : properties_) {
      edge_atom->properties_[property.first] = property.second->Clone(storage);
    }
    return edge_atom;
  }

  Direction direction_ = Direction::BOTH;
  std::vector<GraphDbTypes::EdgeType> edge_types_;
  // TODO: change to unordered_map
  std::map<GraphDbTypes::Property, Expression *> properties_;

 protected:
  using PatternAtom::PatternAtom;
  EdgeAtom(int uid, Identifier *identifier, Direction direction)
      : PatternAtom(uid, identifier), direction_(direction) {}
};

class Clause : public Tree {
  friend class AstTreeStorage;

 public:
  Clause(int uid) : Tree(uid) {}

  Clause *Clone(AstTreeStorage &storage) const override = 0;
};

class Pattern : public Tree {
  friend class AstTreeStorage;

 public:
  DEFVISITABLE(TreeVisitor<TypedValue>);
  bool Accept(HierarchicalTreeVisitor &visitor) override {
    if (visitor.PreVisit(*this)) {
      for (auto &part : atoms_) {
        if (!part->Accept(visitor)) break;
      }
    }
    return visitor.PostVisit(*this);
  }

  Pattern *Clone(AstTreeStorage &storage) const override {
    auto *pattern = storage.Create<Pattern>();
    pattern->identifier_ = identifier_->Clone(storage);
    for (auto *atom : atoms_) {
      pattern->atoms_.push_back(atom->Clone(storage));
    }
    return pattern;
  }

  Identifier *identifier_ = nullptr;
  std::vector<PatternAtom *> atoms_;

 protected:
  Pattern(int uid) : Tree(uid) {}
};

class Query : public Tree {
  friend class AstTreeStorage;

 public:
  DEFVISITABLE(TreeVisitor<TypedValue>);
  bool Accept(HierarchicalTreeVisitor &visitor) override {
    if (visitor.PreVisit(*this)) {
      for (auto &clause : clauses_) {
        if (!clause->Accept(visitor)) break;
      }
    }
    return visitor.PostVisit(*this);
  }

  // Creates deep copy of whole ast.
  Query *Clone(AstTreeStorage &storage) const override {
    auto *query = storage.query();
    for (auto *clause : clauses_) {
      query->clauses_.push_back(clause->Clone(storage));
    }
    return query;
  }

  std::vector<Clause *> clauses_;

 protected:
  Query(int uid) : Tree(uid) {}
};

class Create : public Clause {
  friend class AstTreeStorage;

 public:
  Create(int uid) : Clause(uid) {}
  DEFVISITABLE(TreeVisitor<TypedValue>);
  bool Accept(HierarchicalTreeVisitor &visitor) override {
    if (visitor.PreVisit(*this)) {
      for (auto &pattern : patterns_) {
        if (!pattern->Accept(visitor)) break;
      }
    }
    return visitor.PostVisit(*this);
  }

  Create *Clone(AstTreeStorage &storage) const override {
    auto *create = storage.Create<Create>();
    for (auto *pattern : patterns_) {
      create->patterns_.push_back(pattern->Clone(storage));
    }
    return create;
  }

  std::vector<Pattern *> patterns_;
};

class Where : public Tree {
  friend class AstTreeStorage;

 public:
  DEFVISITABLE(TreeVisitor<TypedValue>);
  bool Accept(HierarchicalTreeVisitor &visitor) override {
    if (visitor.PreVisit(*this)) {
      expression_->Accept(visitor);
    }
    return visitor.PostVisit(*this);
  }

  Where *Clone(AstTreeStorage &storage) const override {
    return storage.Create<Where>(expression_->Clone(storage));
  }

  Expression *expression_ = nullptr;

 protected:
  Where(int uid) : Tree(uid) {}
  Where(int uid, Expression *expression) : Tree(uid), expression_(expression) {}
};

class Match : public Clause {
  friend class AstTreeStorage;

 public:
  DEFVISITABLE(TreeVisitor<TypedValue>);
  bool Accept(HierarchicalTreeVisitor &visitor) override {
    if (visitor.PreVisit(*this)) {
      bool cont = true;
      for (auto &pattern : patterns_) {
        if (!pattern->Accept(visitor)) {
          cont = false;
          break;
        }
      }
      if (cont && where_) {
        where_->Accept(visitor);
      }
    }
    return visitor.PostVisit(*this);
  }

  Match *Clone(AstTreeStorage &storage) const override {
    auto *match = storage.Create<Match>(optional_);
    for (auto *pattern : patterns_) {
      match->patterns_.push_back(pattern->Clone(storage));
    }
    match->where_ = where_ ? where_->Clone(storage) : nullptr;
    return match;
  }

  std::vector<Pattern *> patterns_;
  Where *where_ = nullptr;
  bool optional_ = false;

 protected:
  Match(int uid) : Clause(uid) {}
  Match(int uid, bool optional) : Clause(uid), optional_(optional) {}
};

/** @brief Defines the order for sorting values (ascending or descending). */
enum class Ordering { ASC, DESC };

/**
 * @brief Contents common to @c Return and @c With clauses.
 */
struct ReturnBody {
  /** @brief True if distinct results should be produced. */
  bool distinct = false;
  /** @brief True if asterisk was found in return body */
  bool all_identifiers = false;
  /** @brief Expressions which are used to produce results. */
  std::vector<NamedExpression *> named_expressions;
  /** @brief Expressions used for ordering the results. */
  std::vector<std::pair<Ordering, Expression *>> order_by;
  /** @brief Optional expression on how many results to skip. */
  Expression *skip = nullptr;
  /** @brief Optional expression on how much to limit the results. */
  Expression *limit = nullptr;
};

// Deep copy ReturnBody.
// TODO: Think about turning ReturnBody to class and making this
// function class member.
ReturnBody CloneReturnBody(AstTreeStorage &storage, const ReturnBody &body);

class Return : public Clause {
  friend class AstTreeStorage;

 public:
  DEFVISITABLE(TreeVisitor<TypedValue>);
  bool Accept(HierarchicalTreeVisitor &visitor) override {
    if (visitor.PreVisit(*this)) {
      bool cont = true;
      for (auto &expr : body_.named_expressions) {
        if (!expr->Accept(visitor)) {
          cont = false;
          break;
        }
      }
      if (cont) {
        for (auto &order_by : body_.order_by) {
          if (!order_by.second->Accept(visitor)) {
            cont = false;
            break;
          }
        }
      }
      if (cont && body_.skip) cont = body_.skip->Accept(visitor);
      if (cont && body_.limit) cont = body_.limit->Accept(visitor);
    }
    return visitor.PostVisit(*this);
  }

  Return *Clone(AstTreeStorage &storage) const override {
    auto *ret = storage.Create<Return>();
    ret->body_ = CloneReturnBody(storage, body_);
    return ret;
  }

  ReturnBody body_;

 protected:
  Return(int uid) : Clause(uid) {}
};

class With : public Clause {
  friend class AstTreeStorage;

 public:
  DEFVISITABLE(TreeVisitor<TypedValue>);
  bool Accept(HierarchicalTreeVisitor &visitor) override {
    if (visitor.PreVisit(*this)) {
      bool cont = true;
      for (auto &expr : body_.named_expressions) {
        if (!expr->Accept(visitor)) {
          cont = false;
          break;
        }
      }
      if (cont) {
        for (auto &order_by : body_.order_by) {
          if (!order_by.second->Accept(visitor)) {
            cont = false;
            break;
          }
        }
      }
      if (cont && where_) cont = where_->Accept(visitor);
      if (cont && body_.skip) cont = body_.skip->Accept(visitor);
      if (cont && body_.limit) cont = body_.limit->Accept(visitor);
    }
    return visitor.PostVisit(*this);
  }

  With *Clone(AstTreeStorage &storage) const override {
    auto *with = storage.Create<With>();
    with->body_ = CloneReturnBody(storage, body_);
    with->where_ = where_ ? where_->Clone(storage) : nullptr;
    return with;
  }

  ReturnBody body_;
  Where *where_ = nullptr;

 protected:
  With(int uid) : Clause(uid) {}
};

class Delete : public Clause {
  friend class AstTreeStorage;

 public:
  DEFVISITABLE(TreeVisitor<TypedValue>);
  bool Accept(HierarchicalTreeVisitor &visitor) override {
    if (visitor.PreVisit(*this)) {
      for (auto &expr : expressions_) {
        if (!expr->Accept(visitor)) break;
      }
    }
    return visitor.PostVisit(*this);
  }

  Delete *Clone(AstTreeStorage &storage) const override {
    auto *del = storage.Create<Delete>();
    for (auto *expression : expressions_) {
      del->expressions_.push_back(expression->Clone(storage));
    }
    del->detach_ = detach_;
    return del;
  }

  std::vector<Expression *> expressions_;
  bool detach_ = false;

 protected:
  Delete(int uid) : Clause(uid) {}
};

class SetProperty : public Clause {
  friend class AstTreeStorage;

 public:
  DEFVISITABLE(TreeVisitor<TypedValue>);
  bool Accept(HierarchicalTreeVisitor &visitor) override {
    if (visitor.PreVisit(*this)) {
      property_lookup_->Accept(visitor) && expression_->Accept(visitor);
    }
    return visitor.PostVisit(*this);
  }

  SetProperty *Clone(AstTreeStorage &storage) const override {
    return storage.Create<SetProperty>(property_lookup_->Clone(storage),
                                       expression_->Clone(storage));
  }

  PropertyLookup *property_lookup_ = nullptr;
  Expression *expression_ = nullptr;

 protected:
  SetProperty(int uid) : Clause(uid) {}
  SetProperty(int uid, PropertyLookup *property_lookup, Expression *expression)
      : Clause(uid),
        property_lookup_(property_lookup),
        expression_(expression) {}
};

class SetProperties : public Clause {
  friend class AstTreeStorage;

 public:
  DEFVISITABLE(TreeVisitor<TypedValue>);
  bool Accept(HierarchicalTreeVisitor &visitor) override {
    if (visitor.PreVisit(*this)) {
      identifier_->Accept(visitor) && expression_->Accept(visitor);
    }
    return visitor.PostVisit(*this);
  }

  SetProperties *Clone(AstTreeStorage &storage) const override {
    return storage.Create<SetProperties>(identifier_->Clone(storage),
                                         expression_->Clone(storage), update_);
  }

  Identifier *identifier_ = nullptr;
  Expression *expression_ = nullptr;
  bool update_ = false;

 protected:
  SetProperties(int uid) : Clause(uid) {}
  SetProperties(int uid, Identifier *identifier, Expression *expression,
                bool update = false)
      : Clause(uid),
        identifier_(identifier),
        expression_(expression),
        update_(update) {}
};

class SetLabels : public Clause {
  friend class AstTreeStorage;

 public:
  DEFVISITABLE(TreeVisitor<TypedValue>);
  bool Accept(HierarchicalTreeVisitor &visitor) override {
    if (visitor.PreVisit(*this)) {
      identifier_->Accept(visitor);
    }
    return visitor.PostVisit(*this);
  }

  SetLabels *Clone(AstTreeStorage &storage) const override {
    return storage.Create<SetLabels>(identifier_->Clone(storage), labels_);
  }

  Identifier *identifier_ = nullptr;
  std::vector<GraphDbTypes::Label> labels_;

 protected:
  SetLabels(int uid) : Clause(uid) {}
  SetLabels(int uid, Identifier *identifier,
            const std::vector<GraphDbTypes::Label> &labels)
      : Clause(uid), identifier_(identifier), labels_(labels) {}
};

class RemoveProperty : public Clause {
  friend class AstTreeStorage;

 public:
  DEFVISITABLE(TreeVisitor<TypedValue>);
  bool Accept(HierarchicalTreeVisitor &visitor) override {
    if (visitor.PreVisit(*this)) {
      property_lookup_->Accept(visitor);
    }
    return visitor.PostVisit(*this);
  }

  RemoveProperty *Clone(AstTreeStorage &storage) const override {
    return storage.Create<RemoveProperty>(property_lookup_->Clone(storage));
  }

  PropertyLookup *property_lookup_ = nullptr;

 protected:
  RemoveProperty(int uid) : Clause(uid) {}
  RemoveProperty(int uid, PropertyLookup *property_lookup)
      : Clause(uid), property_lookup_(property_lookup) {}
};

class RemoveLabels : public Clause {
  friend class AstTreeStorage;

 public:
  DEFVISITABLE(TreeVisitor<TypedValue>);
  bool Accept(HierarchicalTreeVisitor &visitor) override {
    if (visitor.PreVisit(*this)) {
      identifier_->Accept(visitor);
    }
    return visitor.PostVisit(*this);
  }

  RemoveLabels *Clone(AstTreeStorage &storage) const override {
    return storage.Create<RemoveLabels>(identifier_->Clone(storage), labels_);
  }

  Identifier *identifier_ = nullptr;
  std::vector<GraphDbTypes::Label> labels_;

 protected:
  RemoveLabels(int uid) : Clause(uid) {}
  RemoveLabels(int uid, Identifier *identifier,
               const std::vector<GraphDbTypes::Label> &labels)
      : Clause(uid), identifier_(identifier), labels_(labels) {}
};

class Merge : public Clause {
  friend class AstTreeStorage;

 public:
  DEFVISITABLE(TreeVisitor<TypedValue>);
  bool Accept(HierarchicalTreeVisitor &visitor) override {
    if (visitor.PreVisit(*this)) {
      bool cont = pattern_->Accept(visitor);
      if (cont) {
        for (auto &set : on_match_) {
          if (!set->Accept(visitor)) {
            cont = false;
            break;
          }
        }
      }
      if (cont) {
        for (auto &set : on_create_) {
          if (!set->Accept(visitor)) {
            cont = false;
            break;
          }
        }
      }
    }
    return visitor.PostVisit(*this);
  }

  Merge *Clone(AstTreeStorage &storage) const override {
    auto *merge = storage.Create<Merge>();
    merge->pattern_ = pattern_->Clone(storage);
    for (auto *on_match : on_match_) {
      merge->on_match_.push_back(on_match->Clone(storage));
    }
    for (auto *on_create : on_create_) {
      merge->on_create_.push_back(on_create->Clone(storage));
    }
    return merge;
  }

  Pattern *pattern_ = nullptr;
  std::vector<Clause *> on_match_;
  std::vector<Clause *> on_create_;

 protected:
  Merge(int uid) : Clause(uid) {}
};

class Unwind : public Clause {
  friend class AstTreeStorage;

 public:
  DEFVISITABLE(TreeVisitor<TypedValue>);
  bool Accept(HierarchicalTreeVisitor &visitor) override {
    if (visitor.PreVisit(*this)) {
      named_expression_->Accept(visitor);
    }
    return visitor.PostVisit(*this);
  }

  Unwind *Clone(AstTreeStorage &storage) const override {
    return storage.Create<Unwind>(named_expression_->Clone(storage));
  }

  NamedExpression *const named_expression_ = nullptr;

 protected:
  Unwind(int uid, NamedExpression *named_expression)
      : Clause(uid), named_expression_(named_expression) {
    debug_assert(named_expression,
                 "Unwind cannot take nullptr for named_expression")
  }
};

class CreateIndex : public Clause {
  friend class AstTreeStorage;

 public:
  DEFVISITABLE(TreeVisitor<TypedValue>);
  DEFVISITABLE(HierarchicalTreeVisitor);

  CreateIndex *Clone(AstTreeStorage &storage) const override {
    return storage.Create<CreateIndex>(label_, property_);
  }

  GraphDbTypes::Label label_;
  GraphDbTypes::Property property_;

 protected:
  CreateIndex(int uid, GraphDbTypes::Label label,
              GraphDbTypes::Property property)
      : Clause(uid), label_(label), property_(property) {}
};

/// CachedAst is used for storing high level asts.
///
/// After query is stripped, parsed and converted to high level ast it can be
/// stored in this class and new trees can be created by plugging different
/// literals.
class CachedAst {
 public:
  CachedAst(AstTreeStorage storage) : storage_(std::move(storage)) {}

  /// Create new storage by plugging literals and named expessions on theirs
  /// positions.
  AstTreeStorage Plug(const Parameters &literals,
                      const std::unordered_map<int, std::string> &named_exprs) {
    AstTreeStorage new_ast;
    storage_.query()->Clone(new_ast);
    LiteralsPlugger plugger(literals, named_exprs);
    new_ast.query()->Accept(plugger);
    return new_ast;
  }

 private:
  class LiteralsPlugger : public HierarchicalTreeVisitor {
   public:
    using HierarchicalTreeVisitor::PreVisit;
    using typename HierarchicalTreeVisitor::ReturnType;
    using HierarchicalTreeVisitor::Visit;
    using HierarchicalTreeVisitor::PostVisit;

    LiteralsPlugger(const Parameters &literals,
                    const std::unordered_map<int, std::string> &named_exprs)
        : literals_(literals), named_exprs_(named_exprs) {}

    bool Visit(PrimitiveLiteral &literal) override {
      if (!literal.value_.IsNull()) {
        permanent_assert(literal.token_position_ != -1,
                         "Use AstPlugLiteralsVisitor only on ast created by "
                         "parsing queries");
        literal.value_ = literals_.AtTokenPosition(literal.token_position_);
      }
      return true;
    }

    bool PreVisit(NamedExpression &named_expr) override {
      // We care only about aliased named expressions in return.
      if (!in_return_ || named_expr.token_position_ == -1) return true;
      permanent_assert(
          named_exprs_.count(named_expr.token_position_),
          "There is no named expression string for needed position");
      named_expr.name_ = named_exprs_.at(named_expr.token_position_);
      return true;
    }

    bool Visit(Identifier &) override { return true; }
    bool Visit(CreateIndex &) override { return true; }

    bool PreVisit(Return &) override {
      in_return_ = true;
      return true;
    }

    bool PostVisit(Return &) override {
      in_return_ = false;
      return true;
    }

   private:
    const Parameters &literals_;
    const std::unordered_map<int, std::string> &named_exprs_;
    bool in_return_ = false;
  };
  AstTreeStorage storage_;
};

#undef CLONE_BINARY_EXPRESSION
#undef CLONE_UNARY_EXPRESSION
}
