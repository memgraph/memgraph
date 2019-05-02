#pragma once

#include "query/common.hpp"
#include "query/context.hpp"
#include "query/distributed/frontend/semantic/symbol_serialization.hpp"
#include "query/frontend/semantic/symbol_table.hpp"
#include "query/typed_value.hpp"
#include "storage/distributed/rpc/serialization.hpp"

namespace distributed {
class DataManager;
}

namespace slk {

inline void Save(const query::SymbolTable &symbol_table,
                 slk::Builder *builder) {
  slk::Save(symbol_table.table_, builder);
}

inline void Load(query::SymbolTable *symbol_table, slk::Reader *reader) {
  slk::Load(&symbol_table->table_, reader);
}

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

void Save(const query::Parameters &parameters, slk::Builder *builder);

void Load(query::Parameters *parameters, slk::Reader *reader);

}  // namespace slk
