#pragma once

#include "query/common.hpp"
#include "query/context.hpp"
#include "query/frontend/semantic/symbol.capnp.h"
#include "query/frontend/semantic/symbol.hpp"
#include "query/frontend/semantic/symbol_table.hpp"
#include "query/serialization.capnp.h"
#include "query/typed_value.hpp"
#include "storage/distributed/serialization.hpp"

namespace distributed {
class DataManager;
}

namespace query {

void SaveCapnpTypedValue(const query::TypedValue &value,
                         capnp::TypedValue::Builder *builder,
                         storage::SendVersions versions, int worker_id);

void LoadCapnpTypedValue(const capnp::TypedValue::Reader &reader,
                         query::TypedValue *value,
                         database::GraphDbAccessor *dba,
                         distributed::DataManager *data_manager);

void SaveEvaluationContext(const EvaluationContext &ctx,
                           capnp::EvaluationContext::Builder *builder);

void LoadEvaluationContext(const capnp::EvaluationContext::Reader &reader,
                           EvaluationContext *ctx);

void Save(const TypedValueVectorCompare &comparator,
          capnp::TypedValueVectorCompare::Builder *builder);

void Load(TypedValueVectorCompare *comparator,
          const capnp::TypedValueVectorCompare::Reader &reader);

inline void Save(const Symbol &symbol, capnp::Symbol::Builder *builder) {
  builder->setName(symbol.name());
  builder->setPosition(symbol.position());
  builder->setUserDeclared(symbol.user_declared());
  builder->setTokenPosition(symbol.token_position());
  switch (symbol.type()) {
    case Symbol::Type::Any:
      builder->setType(capnp::Symbol::Type::ANY);
      break;
    case Symbol::Type::Edge:
      builder->setType(capnp::Symbol::Type::EDGE);
      break;
    case Symbol::Type::EdgeList:
      builder->setType(capnp::Symbol::Type::EDGE_LIST);
      break;
    case Symbol::Type::Number:
      builder->setType(capnp::Symbol::Type::NUMBER);
      break;
    case Symbol::Type::Path:
      builder->setType(capnp::Symbol::Type::PATH);
      break;
    case Symbol::Type::Vertex:
      builder->setType(capnp::Symbol::Type::VERTEX);
      break;
  }
}

inline void Load(Symbol *symbol, const capnp::Symbol::Reader &reader) {
  symbol->name_ = reader.getName();
  symbol->position_ = reader.getPosition();
  symbol->user_declared_ = reader.getUserDeclared();
  symbol->token_position_ = reader.getTokenPosition();
  switch (reader.getType()) {
    case capnp::Symbol::Type::ANY:
      symbol->type_ = Symbol::Type::Any;
      break;
    case capnp::Symbol::Type::EDGE:
      symbol->type_ = Symbol::Type::Edge;
      break;
    case capnp::Symbol::Type::EDGE_LIST:
      symbol->type_ = Symbol::Type::EdgeList;
      break;
    case capnp::Symbol::Type::NUMBER:
      symbol->type_ = Symbol::Type::Number;
      break;
    case capnp::Symbol::Type::PATH:
      symbol->type_ = Symbol::Type::Path;
      break;
    case capnp::Symbol::Type::VERTEX:
      symbol->type_ = Symbol::Type::Vertex;
      break;
  }
}

inline void Save(const SymbolTable &symbol_table,
                 capnp::SymbolTable::Builder *builder) {
  builder->setPosition(symbol_table.max_position());
  auto list_builder = builder->initTable(symbol_table.table().size());
  size_t i = 0;
  for (const auto &entry : symbol_table.table()) {
    auto entry_builder = list_builder[i++];
    entry_builder.setKey(entry.first);
    auto sym_builder = entry_builder.initVal();
    Save(entry.second, &sym_builder);
  }
}

inline void Load(SymbolTable *symbol_table,
                 const capnp::SymbolTable::Reader &reader) {
  symbol_table->position_ = reader.getPosition();
  symbol_table->table_.clear();
  for (const auto &entry_reader : reader.getTable()) {
    int key = entry_reader.getKey();
    Symbol val;
    Load(&val, entry_reader.getVal());
    symbol_table->table_[key] = val;
  }
}

}  // namespace query
