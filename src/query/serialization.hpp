#pragma once

#include "query/common.hpp"
#include "query/context.hpp"
#include "query/frontend/semantic/symbol_serialization.hpp"
#include "query/frontend/semantic/symbol_table.hpp"
#include "query/serialization.capnp.h"
#include "query/typed_value.hpp"
#include "storage/distributed/rpc/serialization.hpp"

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

namespace slk {

inline void Save(const query::SymbolTable &symbol_table,
                 slk::Builder *builder) {
  slk::Save(symbol_table.position_, builder);
  slk::Save(symbol_table.table_, builder);
}

inline void Load(query::SymbolTable *symbol_table, slk::Reader *reader) {
  slk::Load(&symbol_table->position_, reader);
  slk::Load(&symbol_table->table_, reader);
}

void Save(const query::EvaluationContext &ctx, slk::Builder *builder);

void Load(query::EvaluationContext *ctx, slk::Reader *reader);

void Save(const query::TypedValue &value, slk::Builder *builder,
          storage::SendVersions versions, int16_t worker_id);

void Load(query::TypedValue *value, slk::Reader *reader,
          database::GraphDbAccessor *dba,
          distributed::DataManager *data_manager);

void Save(const query::GraphView &graph_view, slk::Builder *builder);

void Load(query::GraphView *graph_view, slk::Reader *reader);

void Save(const query::TypedValueVectorCompare &comparator,
          slk::Builder *builder);

void Load(query::TypedValueVectorCompare *comparator, slk::Reader *reader);

}  // namespace slk
