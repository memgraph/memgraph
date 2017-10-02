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

// Hash function for the key in pattern atom property maps.
namespace std {
template <>
struct hash<std::pair<std::string, GraphDbTypes::Property>> {
  size_t operator()(
      const std::pair<std::string, GraphDbTypes::Property> &pair) const {
    return string_hash(pair.first) ^ property_hash(pair.second);
  };

 private:
  std::hash<std::string> string_hash{};
  std::hash<GraphDbTypes::Property> property_hash{};
};
}

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

// Expressions

class Expression : public Tree {
  friend class AstTreeStorage;

 public:
  Expression *Clone(AstTreeStorage &storage) const override = 0;

 protected:
  Expression(int uid) : Tree(uid) {}
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

class ListMapIndexingOperator : public BinaryOperator {
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

class IfOperator : public Expression {
  friend class AstTreeStorage;

 public:
  DEFVISITABLE(TreeVisitor<TypedValue>);
  bool Accept(HierarchicalTreeVisitor &visitor) override {
    if (visitor.PreVisit(*this)) {
      condition_->Accept(visitor) && then_expression_->Accept(visitor) &&
          else_expression_->Accept(visitor);
    }
    return visitor.PostVisit(*this);
  }

  IfOperator *Clone(AstTreeStorage &storage) const override {
    return storage.Create<IfOperator>(condition_->Clone(storage),
                                      then_expression_->Clone(storage),
                                      else_expression_->Clone(storage));
  }

  // None of the expressions should be nullptrs. If there is no else_expression
  // you probably want to make it NULL PrimitiveLiteral.
  Expression *condition_;
  Expression *then_expression_;
  Expression *else_expression_;

 protected:
  IfOperator(int uid, Expression *condition, Expression *then_expression,
             Expression *else_expression)
      : Expression(uid),
        condition_(condition),
        then_expression_(then_expression),
        else_expression_(else_expression) {
    debug_assert(
        condition_ != nullptr && then_expression_ != nullptr &&
            else_expression_ != nullptr,
        "clause_, then_expression_ and else_expression_ can't be nullptr");
  }
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

class MapLiteral : public BaseLiteral {
  friend class AstTreeStorage;

 public:
  DEFVISITABLE(TreeVisitor<TypedValue>);
  bool Accept(HierarchicalTreeVisitor &visitor) override {
    if (visitor.PreVisit(*this)) {
      for (auto pair : elements_)
        if (!pair.second->Accept(visitor)) break;
    }
    return visitor.PostVisit(*this);
  }

  MapLiteral *Clone(AstTreeStorage &storage) const override {
    auto *map = storage.Create<MapLiteral>();
    for (auto pair : elements_)
      map->elements_.emplace(pair.first, pair.second->Clone(storage));
    return map;
  }

  // maps (property_name, property) to expressions
  std::unordered_map<std::pair<std::string, GraphDbTypes::Property>,
                     Expression *>
      elements_;

 protected:
  MapLiteral(int uid) : BaseLiteral(uid) {}
  MapLiteral(
      int uid,
      const std::unordered_map<std::pair<std::string, GraphDbTypes::Property>,
                               Expression *> &elements)
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
                                          property_name_, property_);
  }

  Expression *expression_ = nullptr;
  std::string property_name_;
  GraphDbTypes::Property property_;

 protected:
  PropertyLookup(int uid, Expression *expression,
                 const std::string &property_name,
                 GraphDbTypes::Property property)
      : Expression(uid),
        expression_(expression),
        property_name_(property_name),
        property_(property) {}
  PropertyLookup(int uid, Expression *expression,
                 std::pair<std::string, GraphDbTypes::Property> property)
      : Expression(uid),
        expression_(expression),
        property_name_(property.first),
        property_(property.second) {}
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

class Aggregation : public BinaryOperator {
  friend class AstTreeStorage;

 public:
  enum class Op { COUNT, MIN, MAX, SUM, AVG, COLLECT_LIST, COLLECT_MAP };
  static const constexpr char *const kCount = "COUNT";
  static const constexpr char *const kMin = "MIN";
  static const constexpr char *const kMax = "MAX";
  static const constexpr char *const kSum = "SUM";
  static const constexpr char *const kAvg = "AVG";
  static const constexpr char *const kCollect = "COLLECT";

  DEFVISITABLE(TreeVisitor<TypedValue>);
  bool Accept(HierarchicalTreeVisitor &visitor) override {
    if (visitor.PreVisit(*this)) {
      if (expression1_) expression1_->Accept(visitor);
      if (expression2_) expression2_->Accept(visitor);
    }
    return visitor.PostVisit(*this);
  }

  Aggregation *Clone(AstTreeStorage &storage) const override {
    return storage.Create<Aggregation>(
        expression1_ ? expression1_->Clone(storage) : nullptr,
        expression2_ ? expression2_->Clone(storage) : nullptr, op_);
  }

  Op op_;

 protected:
  /** Aggregation's first expression is the value being aggregated. The second
   * expression is the key used only in COLLECT_MAP. */
  Aggregation(int uid, Expression *expression1, Expression *expression2, Op op)
      : BinaryOperator(uid, expression1, expression2), op_(op) {
    // COUNT without expression denotes COUNT(*) in cypher.
    debug_assert(expression1 || op == Aggregation::Op::COUNT,
                 "All aggregations, except COUNT require expression");
    debug_assert(expression2 == nullptr ^ op == Aggregation::Op::COLLECT_MAP,
                 "The second expression is obligatory in COLLECT_MAP and "
                 "invalid otherwise");
  }
};

class All : public Expression {
  friend class AstTreeStorage;

 public:
  DEFVISITABLE(TreeVisitor<TypedValue>);
  bool Accept(HierarchicalTreeVisitor &visitor) override {
    if (visitor.PreVisit(*this)) {
      identifier_->Accept(visitor) && list_expression_->Accept(visitor) &&
          where_->Accept(visitor);
    }
    return visitor.PostVisit(*this);
  }

  All *Clone(AstTreeStorage &storage) const override {
    return storage.Create<All>(identifier_->Clone(storage),
                               list_expression_->Clone(storage),
                               where_->Clone(storage));
  }

  Identifier *identifier_ = nullptr;
  Expression *list_expression_ = nullptr;
  Where *where_ = nullptr;

 protected:
  All(int uid, Identifier *identifier, Expression *list_expression,
      Where *where)
      : Expression(uid),
        identifier_(identifier),
        list_expression_(list_expression),
        where_(where) {
    debug_assert(identifier, "identifier must not be nullptr");
    debug_assert(list_expression, "list_expression must not be nullptr");
    debug_assert(where, "where must not be nullptr");
  }
};

class ParameterLookup : public Expression {
  friend class AstTreeStorage;

 public:
  DEFVISITABLE(TreeVisitor<TypedValue>);
  DEFVISITABLE(HierarchicalTreeVisitor);

  ParameterLookup *Clone(AstTreeStorage &storage) const override {
    return storage.Create<ParameterLookup>(token_position_);
  }

  // This field contains token position of *literal* used to create
  // ParameterLookup object. If ParameterLookup object is not created from
  // a literal leave this value at -1.
  int token_position_ = -1;

 protected:
  ParameterLookup(int uid) : Expression(uid) {}
  ParameterLookup(int uid, int token_position)
      : Expression(uid), token_position_(token_position) {}
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

// Pattern atoms

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
  // maps (property_name, property) to an expression
  std::unordered_map<std::pair<std::string, GraphDbTypes::Property>,
                     Expression *>
      properties_;

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
      if (cont && lower_bound_) {
        cont = lower_bound_->Accept(visitor);
      }
      if (cont && upper_bound_) {
        cont = upper_bound_->Accept(visitor);
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
    edge_atom->has_range_ = has_range_;
    edge_atom->lower_bound_ =
        lower_bound_ ? lower_bound_->Clone(storage) : nullptr;
    edge_atom->upper_bound_ =
        upper_bound_ ? upper_bound_->Clone(storage) : nullptr;
    return edge_atom;
  }

  Direction direction_ = Direction::BOTH;
  std::vector<GraphDbTypes::EdgeType> edge_types_;
  // maps (property_name, property) to an expression
  std::unordered_map<std::pair<std::string, GraphDbTypes::Property>,
                     Expression *>
      properties_;
  bool has_range_ = false;
  Expression *lower_bound_ = nullptr;
  Expression *upper_bound_ = nullptr;

 protected:
  using PatternAtom::PatternAtom;
  EdgeAtom(int uid, Identifier *identifier, Direction direction)
      : PatternAtom(uid, identifier), direction_(direction) {}
  EdgeAtom(int uid, Identifier *identifier, Direction direction,
           const std::vector<GraphDbTypes::EdgeType> &edge_types)
      : PatternAtom(uid, identifier),
        direction_(direction),
        edge_types_(edge_types) {}
};

class BreadthFirstAtom : public EdgeAtom {
  friend class AstTreeStorage;

  template <typename TPtr>
  static TPtr *CloneOpt(TPtr *ptr, AstTreeStorage &storage) {
    return ptr ? ptr->Clone(storage) : nullptr;
  }

 public:
  DEFVISITABLE(TreeVisitor<TypedValue>);
  bool Accept(HierarchicalTreeVisitor &visitor) override {
    if (visitor.PreVisit(*this)) {
      bool cont = identifier_->Accept(visitor);
      if (cont && traversed_edge_identifier_)
        cont = traversed_edge_identifier_->Accept(visitor);
      if (cont && next_node_identifier_)
        cont = next_node_identifier_->Accept(visitor);
      if (cont && filter_expression_)
        cont = filter_expression_->Accept(visitor);
    }
    return visitor.PostVisit(*this);
  }

  BreadthFirstAtom *Clone(AstTreeStorage &storage) const override {
    auto bfs_atom = storage.Create<BreadthFirstAtom>(
        identifier_->Clone(storage), direction_, edge_types_,
        CloneOpt(traversed_edge_identifier_, storage),
        CloneOpt(next_node_identifier_, storage),
        CloneOpt(filter_expression_, storage));
    bfs_atom->has_range_ = has_range_;
    bfs_atom->lower_bound_ = CloneOpt(lower_bound_, storage);
    bfs_atom->upper_bound_ = CloneOpt(upper_bound_, storage);
    return bfs_atom;
  }

  Identifier *traversed_edge_identifier_ = nullptr;
  Identifier *next_node_identifier_ = nullptr;
  // Expression which evaluates to true in order to continue the BFS.
  Expression *filter_expression_ = nullptr;

 protected:
  using EdgeAtom::EdgeAtom;
  BreadthFirstAtom(int uid, Identifier *identifier, Direction direction,
                   const std::vector<GraphDbTypes::EdgeType> &edge_types,
                   Identifier *traversed_edge_identifier,
                   Identifier *next_node_identifier,
                   Expression *filter_expression)
      : EdgeAtom(uid, identifier, direction, edge_types),
        traversed_edge_identifier_(traversed_edge_identifier),
        next_node_identifier_(next_node_identifier),
        filter_expression_(filter_expression) {
    has_range_ = true;
  }
};

class Pattern : public Tree {
  friend class AstTreeStorage;

 public:
  DEFVISITABLE(TreeVisitor<TypedValue>);
  bool Accept(HierarchicalTreeVisitor &visitor) override {
    if (visitor.PreVisit(*this)) {
      identifier_->Accept(visitor);
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

// Clauses

class Clause : public Tree {
  friend class AstTreeStorage;

 public:
  Clause(int uid) : Tree(uid) {}

  Clause *Clone(AstTreeStorage &storage) const override = 0;
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

#undef CLONE_BINARY_EXPRESSION
#undef CLONE_UNARY_EXPRESSION
}  // namespace query
