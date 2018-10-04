#pragma once

#include <map>
#include <string>

#include "query/frontend/ast/ast.hpp"
#include "query/frontend/semantic/symbol.capnp.h"
#include "query/frontend/semantic/symbol.hpp"

namespace query {

class SymbolTable final {
 public:
  SymbolTable() {}
  Symbol CreateSymbol(const std::string &name, bool user_declared,
                      Symbol::Type type = Symbol::Type::Any,
                      int token_position = -1) {
    int position = position_++;
    return Symbol(name, position, user_declared, type, token_position);
  }

  auto &operator[](const Tree &tree) { return table_[tree.uid_]; }

  Symbol &at(const Tree &tree) { return table_.at(tree.uid_); }
  const Symbol &at(const Tree &tree) const { return table_.at(tree.uid_); }

  // TODO: Remove these since members are public
  int max_position() const { return position_; }

  const auto &table() const { return table_; }

  int position_{0};
  std::map<int, Symbol> table_;
};

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
