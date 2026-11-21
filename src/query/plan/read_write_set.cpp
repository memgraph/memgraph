// Copyright 2026 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include "query/plan/read_write_set.hpp"

#include <algorithm>
#include <initializer_list>
#include <string_view>
#include <unordered_map>
#include <variant>
#include <vector>

#include "query/frontend/ast/ast.hpp"
#include "query/frontend/ast/ast_visitor.hpp"
#include "query/frontend/semantic/symbol_table.hpp"
#include "query/interpret/awesome_memgraph_functions.hpp"
#include "query/plan/preprocess.hpp"

namespace memgraph::query::plan {

namespace {

bool Intersects(const std::set<std::string> &lhs, const std::set<std::string> &rhs) {
  return std::ranges::any_of(lhs, [&rhs](const auto &key) { return rhs.contains(key); });
}

bool KeysConflict(const std::set<std::string> &read_keys, bool read_any, const std::set<std::string> &write_keys,
                  bool write_any) {
  if (read_any) return write_any || !write_keys.empty();
  if (write_any) return !read_keys.empty();
  return Intersects(read_keys, write_keys);
}

/// Whether a variable may be, or hold, a node, an edge, a path or an edge list.
bool SymbolMayHoldEntity(const Symbol &symbol, const std::unordered_set<Symbol> &non_graph) {
  return !non_graph.contains(symbol) && symbol.type() != Symbol::Type::NUMBER;
}

class ReadWriteCollector final : public HierarchicalTreeVisitor {
 public:
  ReadWriteCollector(const SymbolTable &symbol_table, const std::unordered_set<Symbol> &bound,
                     std::unordered_set<Symbol> non_graph,
                     const std::unordered_map<Symbol, EdgeAtom *> *edge_binders = nullptr)
      : symbol_table_(symbol_table), bound_(bound), non_graph_(std::move(non_graph)), edge_binders_(edge_binders) {}

  using HierarchicalTreeVisitor::PostVisit;
  using HierarchicalTreeVisitor::PreVisit;
  using HierarchicalTreeVisitor::Visit;

  bool Visit(Identifier &ident) override {
    // An identifier the symbol generator left unmapped names no variable.
    if (ident.symbol_pos_ < 0) return true;
    auto const &symbol = symbol_table_.at(ident);
    if (MayHoldEntity(symbol)) result.reads.referenced.insert(symbol);
    return true;
  }

  bool Visit(PrimitiveLiteral & /*literal*/) override { return true; }

  bool Visit(ParameterLookup & /*lookup*/) override { return true; }

  bool Visit(EnumValueAccess & /*access*/) override { return true; }

  // ---------------------------------------------------------------- reads

  bool PreVisit(PropertyLookup &lookup) override {
    if (ReadsGraph(lookup.expression_)) {
      result.reads.NoteProperty(lookup.property_path_.front().name, KeySymbolOf(lookup.expression_));
      ReadEdgePropertiesThrough(lookup.expression_);
      // A node's property permission depends on its labels.
      ReadNodeLabelsForAccessCheck(lookup.expression_);
    }
    return true;
  }

  bool PreVisit(AllPropertiesLookup &lookup) override {
    if (ReadsGraph(lookup.expression_)) {
      ReadNodeLabelsForAccessCheck(lookup.expression_);
      ReadEdgePropertiesThrough(lookup.expression_);
      result.reads.NoteAnyProperty(KeySymbolOf(lookup.expression_));
    }
    return true;
  }

  bool PreVisit(SubscriptOperator &op) override {
    if (!ReadsGraph(op.expression1_) || utils::Downcast<ListLiteral>(op.expression1_)) return true;
    // A subscript on a node is a property read, whose permission depends on the node's labels.
    ReadNodeLabelsForAccessCheck(op.expression1_);
    ReadEdgePropertiesThrough(op.expression1_);
    if (auto *key = utils::Downcast<PrimitiveLiteral>(op.expression2_)) {
      if (key->value_.IsString()) {
        std::string name{key->value_.ValueString()};
        result.reads.NoteProperty(name, KeySymbolOf(op.expression1_));
      }
      return true;
    }
    result.reads.NoteAnyProperty(KeySymbolOf(op.expression1_));
    return true;
  }

  bool PreVisit(LabelsTest &test) override {
    if (const auto *cnf = test.Cnf()) {
      for (const auto &label : cnf->labels) result.reads.labels.insert(label.name);
      for (const auto &group : cnf->or_labels) {
        for (const auto &label : group) result.reads.labels.insert(label.name);
      }
    } else if (const auto *term = test.Term()) {
      ReadLabelTerm(*term);
    }
    return true;
  }

  bool PreVisit(EdgeTypesTest &test) override {
    for (const auto &type : test.valid_edgetypes_) result.reads.edge_types.insert(type.name);
    return true;
  }

  bool PreVisit(Function &function) override {
    // It reads what the planner cannot name: every label, key or edge of an entity. A built-in says whether it reads
    // one; a module function reaches the graph only through its arguments.
    auto const reads_graph =
        function.is_user_defined_
            ? std::ranges::any_of(function.arguments_, [this](Expression *arg) { return MayHoldEntity(arg); })
            : FunctionReadsGraph(function.function_name_);
    if (reads_graph) result.reads.everything = true;
    return true;
  }

  bool PreVisit(NodeAtom &node) override {
    auto const &symbol = symbol_table_.at(*node.identifier_);
    result.reads.symbols.insert(symbol);
    // The fine-grained access check of every node a pattern reads looks at its labels.
    ReadLabelsForAccessCheck(std::nullopt);
    // Only a node the pattern has to find reads which nodes exist, and only when an unlabelled node can match.
    if (node.label_term_) {
      ReadLabelTerm(*node.label_term_);
      if (!bound_.contains(symbol) && !IsPositive(*node.label_term_)) result.reads.nodes = true;
    } else if (!bound_.contains(symbol)) {
      result.reads.nodes = true;
    }
    ReadPropertyKeys(node.properties_, node.identifier_);
    return true;
  }

  bool PreVisit(EdgeAtom &edge) override {
    result.reads.symbols.insert(symbol_table_.at(*edge.identifier_));
    NoteEdgeTypes(edge, result.reads);
    ReadPropertyKeys(edge.properties_, edge.identifier_);
    // EdgeAtom::Accept does not walk the lambdas.
    for (auto *lambda : {&edge.filter_lambda_, &edge.weight_lambda_}) {
      if (lambda->expression) lambda->expression->Accept(*this);
    }
    return true;
  }

  bool PreVisit(CallProcedure &call) override {
    result.reads.everything = true;
    if (call.graph_access_ == GraphAccess::Write) result.writes.everything = true;
    return true;
  }

  bool PreVisit(LoadCsv &load) override { return ReadsAFileRow(*load.row_var_); }

  bool PreVisit(LoadParquet &load) override { return ReadsAFileRow(*load.row_var_); }

  bool PreVisit(LoadJsonl &load) override { return ReadsAFileRow(*load.row_var_); }

  // ---------------------------------------------------------------- writes

  bool PreVisit(Create &create) override {
    for (auto *pattern : create.patterns_) WritePattern(*pattern);
    return false;
  }

  bool PreVisit(Merge &merge) override {
    // MERGE first matches its pattern, then may create it.
    merge.pattern_->Accept(*this);
    WritePattern(*merge.pattern_);
    for (auto *clause : merge.on_match_) clause->Accept(*this);
    for (auto *clause : merge.on_create_) clause->Accept(*this);
    return false;
  }

  bool PreVisit(Delete &del) override {
    for (auto *expression : del.expressions_) {
      if (!WriteDeletedEdge(expression)) result.writes.everything = true;
      // A DELETE checks the labels of its node, or of both end nodes of an edge.
      if (MayHoldEntity(expression)) {
        auto *ident = utils::Downcast<Identifier>(expression);
        bool const node = ident && symbol_table_.at(*ident).type() == Symbol::Type::VERTEX;
        ReadLabelsForAccessCheck(node ? KeySymbolOf(expression) : std::nullopt);
      }
      expression->Accept(*this);
    }
    return false;
  }

  bool PreVisit(SetProperty &set) override {
    WriteProperty(*set.property_lookup_);
    set.expression_->Accept(*this);
    return false;
  }

  bool PreVisit(SetProperties &set) override {
    result.writes.NoteAnyProperty(KeySymbolOf(set.identifier_));
    ReadNodeLabelsForAccessCheck(set.identifier_);
    // `SET n = m` copies every property of an entity; any value but a map literal or a parameter may be one.
    if (!utils::Downcast<MapLiteral>(set.expression_) && !utils::Downcast<ParameterLookup>(set.expression_) &&
        ReadsGraph(set.expression_)) {
      result.reads.NoteAnyProperty(std::nullopt);
      ReadEdgePropertiesThrough(set.expression_);
      // Its properties are masked by its labels.
      ReadNodeLabelsForAccessCheck(set.expression_);
    }
    set.expression_->Accept(*this);
    return false;
  }

  bool PreVisit(SetLabels &set) override {
    Relabel(set.identifier_, set.labels_);
    return false;
  }

  bool PreVisit(RemoveProperty &remove) override {
    WriteProperty(*remove.property_lookup_);
    return false;
  }

  bool PreVisit(RemoveLabels &remove) override {
    Relabel(remove.identifier_, remove.labels_);
    return false;
  }

  ReadsAndWrites result;

 private:
  /// Whether a value may be, or hold, a node, an edge or a path. `ReadsGraph` asks the narrower question of a lookup's
  /// object: whether the object itself may be a node or an edge.
  bool MayHoldEntity(Expression *value) const {
    return memgraph::query::plan::MayHoldEntity(value, symbol_table_, non_graph_);
  }

  /// A SET or REMOVE of a property: the target is an expression too (`SET (CASE WHEN a.f THEN b ELSE a END).x`).
  void WriteProperty(PropertyLookup &target) {
    result.writes.NoteProperty(target.property_path_.front().name, KeySymbolOf(target.expression_));
    // A node's property permission depends on its labels.
    ReadNodeLabelsForAccessCheck(target.expression_);
    target.expression_->Accept(*this);
  }

  void Relabel(Identifier *node, const std::vector<QueryLabelType> &labels) {
    WriteLabels(labels);
    result.writes.relabel.Note(KeySymbolOf(node));
    ReadLabelsForAccessCheck(KeySymbolOf(node));
  }

  /// Whether a lookup on @p object reads the graph: it may be a node or an edge, not a map value.
  bool ReadsGraph(Expression *object) const {
    if (utils::Downcast<MapLiteral>(object) || !MayHoldEntity(object)) return false;
    auto *ident = utils::Downcast<Identifier>(object);
    if (!ident) return true;
    auto const type = symbol_table_.at(*ident).type();
    return type == Symbol::Type::ANY || type == Symbol::Type::VERTEX || type == Symbol::Type::EDGE;
  }

  static void NoteEdgeTypes(const EdgeAtom &edge, ReadWriteSet &set) {
    if (edge.edge_types_.empty()) set.any_edge_type = true;
    for (const auto &type : edge.edge_types_) {
      if (const auto *name = std::get_if<EdgeTypeIx>(&type)) {
        set.edge_types.insert(name->name);
      } else {
        set.any_edge_type = true;
      }
    }
  }

  /// A DELETE of a variable an edge atom binds deletes only edges of that atom's types, and their properties.
  bool WriteDeletedEdge(Expression *expression) {
    auto *ident = utils::Downcast<Identifier>(expression);
    if (!ident || ident->symbol_pos_ < 0 || !edge_binders_) return false;
    auto const it = edge_binders_->find(symbol_table_.at(*ident));
    if (it == edge_binders_->end()) return false;
    NoteEdgeTypes(*it->second, result.writes);
    result.writes.edge_properties = true;
    return true;
  }

  /// A property read through any value but a node variable may read a deleted edge's properties.
  void ReadEdgePropertiesThrough(Expression *object) {
    auto *ident = utils::Downcast<Identifier>(object);
    if (!ident || symbol_table_.at(*ident).type() != Symbol::Type::VERTEX) result.reads.edge_properties = true;
  }

  /// Whether a lookup's object is an edge variable, whose access check reads its type, not labels.
  bool IsEdge(Expression *object) const {
    auto *ident = utils::Downcast<Identifier>(object);
    return ident && symbol_table_.at(*ident).type() == Symbol::Type::EDGE;
  }

  /// Whether a variable may be, or hold, a node, an edge, a path or an edge list.
  bool MayHoldEntity(const Symbol &symbol) const { return SymbolMayHoldEntity(symbol, non_graph_); }

  /// A LOAD clause's row variable holds a file row, never a graph entity.
  bool ReadsAFileRow(Identifier &row_var) {
    non_graph_.insert(symbol_table_.at(row_var));
    return true;
  }

  /// Whether every node the term matches has a label, so a created node can match it only through a label write.
  static bool IsPositive(const LabelTerm &term) {
    return std::visit(
        [](const auto &node) {
          using T = std::decay_t<decltype(node)>;
          if constexpr (std::is_same_v<T, LabelTerm::Not>) {
            return false;
          } else if constexpr (std::is_same_v<T, LabelTerm::And>) {
            return std::ranges::any_of(node.operands, IsPositive);
          } else if constexpr (std::is_same_v<T, LabelTerm::Or>) {
            return std::ranges::all_of(node.operands, IsPositive);
          } else {
            return true;
          }
        },
        term.node);
  }

  void ReadLabelTerm(const LabelTerm &term) {
    std::visit(
        [this](const auto &node) {
          using T = std::decay_t<decltype(node)>;
          if constexpr (std::is_same_v<T, LabelTerm::Label>) {
            result.reads.labels.insert(node.label.name);
          } else if constexpr (std::is_same_v<T, LabelTerm::And> || std::is_same_v<T, LabelTerm::Or>) {
            for (const auto &operand : node.operands) ReadLabelTerm(operand);
          } else {
            result.reads.any_label = true;
            if constexpr (std::is_same_v<T, LabelTerm::Dynamic>) {
              if (node.expression) node.expression->Accept(*this);
            }
          }
        },
        term.node);
  }

  template <class TProperties>
  void ReadPropertyKeys(const TProperties &properties, Identifier *atom) {
    if (const auto *map = std::get_if<std::unordered_map<PropertyIx, Expression *>>(&properties)) {
      for (const auto &[key, value] : *map) {
        result.reads.NoteProperty(key.name, KeySymbolOf(atom));
      }
    } else {
      result.reads.NoteAnyProperty(KeySymbolOf(atom));
    }
  }

  /// The variable a property key is reached through, or nullopt for an object that is no variable.
  std::optional<Symbol> KeySymbolOf(Expression *object) const {
    auto *ident = utils::Downcast<Identifier>(object);
    if (!ident || ident->symbol_pos_ < 0) return std::nullopt;
    return symbol_table_.at(*ident);
  }

  /// A created entity's properties are reachable only through its labels or edge type, or its own symbol, which the
  /// other conflicts cover. Counting them as property writes would make `(b:B {id})` wait for `(a:A {id})`.
  template <class TProperties>
  void ReadCreatedPropertyValues(const TProperties &properties) {
    if (const auto *map = std::get_if<std::unordered_map<PropertyIx, Expression *>>(&properties)) {
      for (const auto &[key, value] : *map) value->Accept(*this);
    }
  }

  void WriteLabels(const std::vector<QueryLabelType> &labels) {
    for (const auto &label : labels) {
      if (const auto *name = std::get_if<LabelIx>(&label)) {
        result.writes.labels.insert(name->name);
      } else {
        result.writes.any_label = true;
        std::get<Expression *>(label)->Accept(*this);
      }
    }
  }

  /// A fine-grained access check reads the labels of the node @p through holds; nullopt: of nodes no one variable
  /// holds.
  void ReadLabelsForAccessCheck(const std::optional<Symbol> &through) {
#ifdef MG_ENTERPRISE
    result.reads.access_check.Note(through);
#endif
  }

  /// The same for the node @p object may be; an edge's check reads its type, not labels.
  void ReadNodeLabelsForAccessCheck(Expression *object) {
    if (!IsEdge(object)) ReadLabelsForAccessCheck(KeySymbolOf(object));
  }

  void WritePattern(Pattern &pattern) {
    for (auto *atom : pattern.atoms_) {
      auto const &symbol = symbol_table_.at(*atom->identifier_);
      if (bound_.contains(symbol)) {
        // Creating an edge checks the labels of an existing end node.
        if (pattern.atoms_.size() > 1 && utils::Downcast<NodeAtom>(atom)) ReadLabelsForAccessCheck(symbol);
        continue;
      }
      result.writes.symbols.insert(symbol);
      if (auto *node = utils::Downcast<NodeAtom>(atom)) {
        result.writes.nodes = true;
        if (auto labels = node->LabelConjunction()) WriteLabels(*labels);
        ReadCreatedPropertyValues(node->properties_);
      } else if (auto *edge = utils::Downcast<EdgeAtom>(atom)) {
        for (const auto &type : edge->edge_types_) {
          if (const auto *name = std::get_if<EdgeTypeIx>(&type)) {
            result.writes.edge_types.insert(name->name);
          } else {
            result.writes.any_edge_type = true;
            std::get<Expression *>(type)->Accept(*this);
          }
        }
        ReadCreatedPropertyValues(edge->properties_);
      }
    }
  }

  const SymbolTable &symbol_table_;
  const std::unordered_set<Symbol> &bound_;
  std::unordered_set<Symbol> non_graph_;
  const std::unordered_map<Symbol, EdgeAtom *> *edge_binders_;
};

}  // namespace

bool ReadWriteSet::Empty() const {
  return properties.empty() && !any_property && labels.empty() && !any_label && edge_types.empty() && !any_edge_type &&
         !nodes && !edge_properties && !everything && symbols.empty();
}

void ReadWriteSet::KeyReach::Note(const std::optional<Symbol> &through) {
  if (!through || (symbol && *symbol != *through)) mixed = true;
  if (!symbol) symbol = through;
}

void ReadWriteSet::KeyReach::Merge(const KeyReach &other) {
  mixed |= other.mixed;
  if (other.symbol) Note(other.symbol);
}

void ReadWriteSet::NoteProperty(const std::string &key, const std::optional<Symbol> &through) {
  properties.insert(key);
  property_reach[key].Note(through);
}

void ReadWriteSet::NoteAnyProperty(const std::optional<Symbol> &through) {
  any_property = true;
  any_property_reach.Note(through);
}

void ReadWriteSet::Merge(const ReadWriteSet &other) {
  properties.insert(other.properties.begin(), other.properties.end());
  any_property |= other.any_property;
  labels.insert(other.labels.begin(), other.labels.end());
  any_label |= other.any_label;
  edge_types.insert(other.edge_types.begin(), other.edge_types.end());
  any_edge_type |= other.any_edge_type;
  nodes |= other.nodes;
  edge_properties |= other.edge_properties;
  everything |= other.everything;
  access_check.Merge(other.access_check);
  relabel.Merge(other.relabel);
  for (const auto &[key, reach] : other.property_reach) property_reach[key].Merge(reach);
  any_property_reach.Merge(other.any_property_reach);
  symbols.insert(other.symbols.begin(), other.symbols.end());
  referenced.insert(other.referenced.begin(), other.referenced.end());
}

bool Conflicts(const ReadWriteSet &reads, const ReadWriteSet &writes) {
  if (reads.access_check.Any() && writes.relabel.Any()) return true;
  if (reads.Empty() || writes.Empty()) return false;
  if (reads.everything || writes.everything) return true;
  // A pattern through a node this command created: View::OLD cannot see it at all.
  if (std::ranges::any_of(reads.symbols, [&writes](const auto &sym) { return writes.symbols.contains(sym); })) {
    return true;
  }
  return KeysConflict(reads.properties, reads.any_property, writes.properties, writes.any_property) ||
         KeysConflict(reads.labels, reads.any_label, writes.labels, writes.any_label) ||
         KeysConflict(reads.edge_types, reads.any_edge_type, writes.edge_types, writes.any_edge_type) ||
         (reads.nodes && writes.nodes) || (reads.edge_properties && writes.edge_properties);
}

namespace {

/// How @p set reaches @p key (nullptr: any key), counting its any-key entries.
ReadWriteSet::KeyReach ReachOf(const ReadWriteSet &set, const std::string *key) {
  ReadWriteSet::KeyReach reach;
  if (key) {
    if (auto it = set.property_reach.find(*key); it != set.property_reach.end()) reach.Merge(it->second);
  }
  if (set.any_property) reach.Merge(set.any_property_reach);
  return reach;
}

}  // namespace

ReadWriteSet::KeyReach SharedPropertyReach(const ReadWriteSet &a, const ReadWriteSet &b) {
  ReadWriteSet::KeyReach shared;
  auto const note = [&](const std::string *key) {
    shared.Note(ReachOf(a, key).One());
    shared.Note(ReachOf(b, key).One());
  };
  for (const auto &key : a.properties) {
    if (b.any_property || b.properties.contains(key)) note(&key);
  }
  if (a.any_property) {
    for (const auto &key : b.properties) note(&key);
    if (b.any_property) note(nullptr);
  }
  return shared;
}

std::optional<Symbol> ConflictsOnlyOnKeysOf(const ReadWriteSet &reads, const ReadWriteSet &writes) {
  // A procedure or a graph-reading function reads everything, through no one variable.
  if (reads.everything || writes.everything) return std::nullopt;
  auto other_reads = reads;
  auto other_writes = writes;
  for (auto *set : {&other_reads, &other_writes}) {
    set->properties.clear();
    set->any_property = false;
    set->access_check = {};
    set->relabel = {};
  }
  if (Conflicts(other_reads, other_writes)) return std::nullopt;
  // Every key both sides touch, and every access check a label write reaches, goes through one and the same variable.
  auto shared = SharedPropertyReach(reads, writes);
  if (reads.access_check.Any() && writes.relabel.Any()) {
    shared.Note(reads.access_check.One());
    shared.Note(writes.relabel.One());
  }
  return shared.One();
}

bool MayHoldEntity(Expression *value, const SymbolTable &symbol_table, const std::unordered_set<Symbol> &non_graph) {
  // An allowlist: a kind not named here may hold a node, an edge or a path. Every named kind builds its value from its
  // operands only, so it holds one only when an operand does.
  auto const may = [&](Expression *operand) { return operand && MayHoldEntity(operand, symbol_table, non_graph); };
  // A comprehension's variables hold elements of its list (and a reduce's start value): data when those are.
  auto const may_with = [&](Expression *result, std::initializer_list<Identifier *> data_vars) {
    auto seeded = non_graph;
    for (auto *var : data_vars) seeded.insert(symbol_table.at(*var));
    return MayHoldEntity(result, symbol_table, seeded);
  };
  auto const is_entity = [&](Expression *object) {
    auto *ident = utils::Downcast<Identifier>(object);
    if (!ident) return false;
    auto const type = symbol_table.at(*ident).type();
    return type == Symbol::Type::VERTEX || type == Symbol::Type::EDGE;
  };
  if (utils::Downcast<PrimitiveLiteral>(value) || utils::Downcast<ParameterLookup>(value) ||
      utils::Downcast<EnumValueAccess>(value)) {
    return false;
  }
  if (auto *ident = utils::Downcast<Identifier>(value)) return SymbolMayHoldEntity(symbol_table.at(*ident), non_graph);
  if (auto *list = utils::Downcast<ListLiteral>(value)) return std::ranges::any_of(list->elements_, may);
  if (auto *map = utils::Downcast<MapLiteral>(value)) {
    return std::ranges::any_of(map->elements_, [&](const auto &entry) { return may(entry.second); });
  }
  if (auto *projection = utils::Downcast<MapProjectionLiteral>(value)) {
    return std::ranges::any_of(projection->elements_,
                               [&](const auto &entry) { return !entry.second || may(entry.second); });
  }
  // A property of a node or an edge is a property value; a map's or a list's element follows the map or list.
  if (auto *lookup = utils::Downcast<PropertyLookup>(value))
    return !is_entity(lookup->expression_) && may(lookup->expression_);
  if (auto *lookup = utils::Downcast<AllPropertiesLookup>(value)) {
    return !is_entity(lookup->expression_) && may(lookup->expression_);
  }
  if (auto *subscript = utils::Downcast<SubscriptOperator>(value)) {
    return !is_entity(subscript->expression1_) && may(subscript->expression1_);
  }
  if (auto *aggregation = utils::Downcast<Aggregation>(value)) {
    switch (aggregation->op_) {
      case Aggregation::Op::PROJECT_PATH:
      case Aggregation::Op::PROJECT_LISTS:
      case Aggregation::Op::DERIVE:
        return true;
      default:
        return may(aggregation->expression1_) || may(aggregation->expression2_);
    }
  }
  if (utils::Downcast<LabelsTest>(value) || utils::Downcast<EdgeTypesTest>(value) ||
      utils::Downcast<RegexMatch>(value) || utils::Downcast<All>(value) || utils::Downcast<Any>(value) ||
      utils::Downcast<None>(value) || utils::Downcast<Single>(value)) {
    return false;
  }
  if (auto *binary = utils::Downcast<BinaryOperator>(value))
    return may(binary->expression1_) || may(binary->expression2_);
  if (auto *unary = utils::Downcast<UnaryOperator>(value)) return may(unary->expression_);
  if (auto *slice = utils::Downcast<ListSlicingOperator>(value)) return may(slice->list_);
  if (auto *if_op = utils::Downcast<IfOperator>(value))
    return may(if_op->then_expression_) || may(if_op->else_expression_);
  if (auto *coalesce = utils::Downcast<Coalesce>(value)) return std::ranges::any_of(coalesce->expressions_, may);
  if (auto *comprehension = utils::Downcast<ListComprehension>(value)) {
    if (may(comprehension->list_)) return true;
    return comprehension->expression_ && may_with(comprehension->expression_, {comprehension->identifier_});
  }
  if (auto *extract = utils::Downcast<Extract>(value)) {
    return may(extract->list_) || may_with(extract->expression_, {extract->identifier_});
  }
  if (auto *reduce = utils::Downcast<Reduce>(value)) {
    return may(reduce->initializer_) || may(reduce->list_) ||
           may_with(reduce->expression_, {reduce->accumulator_, reduce->identifier_});
  }
  // A built-in makes no entity out of values that hold none, and these return only property values, names or ids; a
  // module function may look one up.
  if (auto *function = utils::Downcast<Function>(value); function && !function->is_user_defined_) {
    static const std::unordered_set<std::string_view> kValueOnly{"PROPERTIES", "KEYS", "LABELS", "TYPE", "ID"};
    if (kValueOnly.contains(function->function_name_)) return false;
    return std::ranges::any_of(function->arguments_, may);
  }
  return true;
}

bool ConflictsOnlyInAccessCheck(const ReadWriteSet &reads, const ReadWriteSet &writes) {
  if (!reads.access_check.Any() || !Conflicts(reads, writes)) return false;
  auto without_check = reads;
  without_check.access_check = {};
  return !Conflicts(without_check, writes);
}

namespace {

/// The RETURN clauses that end @p query and its UNION parts, through a final WHEN.
void CollectReturns(CypherQuery &query, std::vector<Return *> &returns) {
  auto const collect = [&](SingleQuery *single) {
    if (!single || single->clauses_.empty()) return;
    auto *last = single->clauses_.back();
    if (auto *ret = utils::Downcast<Return>(last)) {
      returns.push_back(ret);
    } else if (auto *branches = utils::Downcast<ConditionalBranches>(last)) {
      for (auto &branch : branches->branches_) CollectReturns(*branch.body, returns);
    }
  };
  collect(query.single_query_);
  for (auto *cypher_union : query.cypher_unions_) collect(cypher_union->single_query_);
}

}  // namespace

bool CallResultMayBeFresh(CallSubquery &call, const Symbol &result, const SymbolTable &symbol_table,
                          const std::unordered_set<Symbol> &fresh, const std::unordered_set<Symbol> &outer_bound,
                          const std::unordered_set<Symbol> &non_graph) {
  std::vector<Return *> returns;
  CollectReturns(*call.cypher_query_, returns);
  bool named = false;
  for (auto *ret : returns) {
    for (auto *named_expression : ret->body_.named_expressions) {
      if (symbol_table.at(*named_expression) != result) continue;
      named = true;
      if (!MayHoldEntity(named_expression->expression_, symbol_table, non_graph)) continue;
      UsedSymbolsCollector used(symbol_table);
      named_expression->expression_->Accept(used);
      if (std::ranges::any_of(used.symbols_, [&](const Symbol &symbol) {
            return fresh.contains(symbol) || !outer_bound.contains(symbol);
          })) {
        return true;
      }
    }
  }
  return !named;
}

ReadsAndWrites CollectReadsAndWrites(utils::Visitable<HierarchicalTreeVisitor> &tree, const SymbolTable &symbol_table,
                                     const std::unordered_set<Symbol> &bound,
                                     const std::unordered_set<Symbol> &non_graph,
                                     const std::unordered_map<Symbol, EdgeAtom *> *edge_binders) {
  ReadWriteCollector collector(symbol_table, bound, non_graph, edge_binders);
  tree.Accept(collector);
  return std::move(collector.result);
}

ReadWriteSet CollectReads(const Matching &matching, const SymbolTable &symbol_table,
                          const std::unordered_set<Symbol> &bound, const std::unordered_set<Symbol> &non_graph) {
  ReadWriteCollector collector(symbol_table, bound, non_graph);
  for (const auto &expansion : matching.expansions) {
    if (expansion.node1) expansion.node1->Accept(collector);
    if (expansion.edge) expansion.edge->Accept(collector);
    if (expansion.node2) expansion.node2->Accept(collector);
  }
  for (const auto &filter : matching.filters) {
    if (filter.expression) filter.expression->Accept(collector);
  }
  return std::move(collector.result.reads);
}

}  // namespace memgraph::query::plan
