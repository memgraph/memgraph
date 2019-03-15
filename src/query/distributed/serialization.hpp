#pragma once

#include "query/common.hpp"
#include "query/context.hpp"
#include "query/distributed/frontend/semantic/symbol_serialization.hpp"
#include "query/frontend/semantic/symbol_table.hpp"
#include "query/distributed/serialization.capnp.h"
#include "query/typed_value.hpp"
#include "rpc/serialization.hpp"
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

void Save(const TypedValueVectorCompare &comparator,
          capnp::TypedValueVectorCompare::Builder *builder);

void Load(TypedValueVectorCompare *comparator,
          const capnp::TypedValueVectorCompare::Reader &reader);

inline void Save(const SymbolTable &symbol_table,
                 capnp::SymbolTable::Builder *builder) {
  auto table_builder = builder->initTable();
  utils::SaveMap<utils::capnp::BoxInt32, capnp::Symbol,
                 std::map<int32_t, Symbol>>(
      symbol_table.table(), &table_builder,
      [](auto *builder, const auto &entry) {
        auto key_builder = builder->initKey();
        key_builder.setValue(entry.first);
        auto value_builder = builder->initValue();
        Save(entry.second, &value_builder);
      });
}

inline void Load(SymbolTable *symbol_table,
                 const capnp::SymbolTable::Reader &reader) {
  utils::LoadMap<utils::capnp::BoxInt32, capnp::Symbol,
                 std::map<int32_t, Symbol>>(
      &symbol_table->table_, reader.getTable(), [](const auto &reader) {
        std::pair<int32_t, Symbol> entry;
        entry.first = reader.getKey().getValue();
        Load(&entry.second, reader.getValue());
        return entry;
      });
}

void Save(const Parameters &parameters,
          utils::capnp::Map<utils::capnp::BoxInt64,
                            storage::capnp::PropertyValue>::Builder *builder);

void Load(
    Parameters *parameters,
    const utils::capnp::Map<utils::capnp::BoxInt64,
                            storage::capnp::PropertyValue>::Reader &reader);

}  // namespace query

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
