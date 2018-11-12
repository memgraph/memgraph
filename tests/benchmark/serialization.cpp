#include <sstream>
#include <string>

#include <benchmark/benchmark.h>

#include <capnp/serialize.h>
#include <kj/std/iostream.h>

#include "query/frontend/semantic/symbol.capnp.h"
#include "query/frontend/semantic/symbol.hpp"

#include "communication/rpc/serialization.hpp"

class SymbolVectorFixture : public benchmark::Fixture {
 protected:
  std::vector<query::Symbol> symbols_;

  void SetUp(const benchmark::State &state) override {
    using Type = ::query::Symbol::Type;
    std::vector<Type> types{Type::Any,  Type::Vertex, Type::Edge,
                            Type::Path, Type::Number, Type::EdgeList};
    symbols_.reserve(state.range(0));
    for (int i = 0; i < state.range(0); ++i) {
      std::string name = "Symbol " + std::to_string(i);
      bool user_declared = i % 2;
      auto type = types[i % types.size()];
      symbols_.emplace_back(name, i, user_declared, type, i);
    }
  }

  void TearDown(const benchmark::State &) override { symbols_.clear(); }
};

void SymbolVectorToCapnpMessage(const std::vector<query::Symbol> &symbols,
                                capnp::MessageBuilder &message) {
  auto symbols_builder =
      message.initRoot<capnp::List<query::capnp::Symbol>>(symbols.size());
  for (int i = 0; i < symbols.size(); ++i) {
    const auto &sym = symbols[i];
    query::capnp::Symbol::Builder sym_builder = symbols_builder[i];
    sym_builder.setName(sym.name());
    sym_builder.setPosition(sym.position());
    sym_builder.setType(query::capnp::Symbol::Type::ANY);
    sym_builder.setUserDeclared(sym.user_declared());
    sym_builder.setTokenPosition(sym.token_position());
  }
}

std::stringstream SerializeCapnpSymbolVector(
    const std::vector<query::Symbol> &symbols) {
  std::stringstream stream(std::ios_base::in | std::ios_base::out |
                           std::ios_base::binary);
  {
    capnp::MallocMessageBuilder message;
    SymbolVectorToCapnpMessage(symbols, message);
    kj::std::StdOutputStream std_stream(stream);
    kj::BufferedOutputStreamWrapper buffered_stream(std_stream);
    writeMessage(buffered_stream, message);
  }
  return stream;
}

BENCHMARK_DEFINE_F(SymbolVectorFixture, CapnpSerial)(benchmark::State &state) {
  while (state.KeepRunning()) {
    SerializeCapnpSymbolVector(symbols_);
  }
  state.SetItemsProcessed(state.iterations());
}

BENCHMARK_DEFINE_F(SymbolVectorFixture, CapnpDeserial)
(benchmark::State &state) {
  while (state.KeepRunning()) {
    state.PauseTiming();
    auto stream = SerializeCapnpSymbolVector(symbols_);
    state.ResumeTiming();
    kj::std::StdInputStream std_stream(stream);
    capnp::InputStreamMessageReader message(std_stream);
    auto symbols_reader = message.getRoot<capnp::List<query::capnp::Symbol>>();
    std::vector<query::Symbol> symbols;
    symbols.reserve(symbols_reader.size());
    for (const auto &sym : symbols_reader) {
      symbols.emplace_back(sym.getName().cStr(), sym.getPosition(),
                           sym.getUserDeclared(), query::Symbol::Type::Any,
                           sym.getTokenPosition());
    }
  }
  state.SetItemsProcessed(state.iterations());
}

BENCHMARK_REGISTER_F(SymbolVectorFixture, CapnpSerial)
    ->RangeMultiplier(4)
    ->Range(4, 1 << 12)
    ->Unit(benchmark::kNanosecond);

BENCHMARK_REGISTER_F(SymbolVectorFixture, CapnpDeserial)
    ->RangeMultiplier(4)
    ->Range(4, 1 << 12)
    ->Unit(benchmark::kNanosecond);

uint8_t Type2Int(query::Symbol::Type type) {
  switch (type) {
    case query::Symbol::Type::Any:
      return 1;
    case query::Symbol::Type::Vertex:
      return 2;
    case query::Symbol::Type::Edge:
      return 3;
    case query::Symbol::Type::Path:
      return 4;
    case query::Symbol::Type::Number:
      return 5;
    case query::Symbol::Type::EdgeList:
      return 6;
  }
}

query::Symbol::Type Int2Type(uint8_t value) {
  switch (value) {
    case 1:
      return query::Symbol::Type::Any;
    case 2:
      return query::Symbol::Type::Vertex;
    case 3:
      return query::Symbol::Type::Edge;
    case 4:
      return query::Symbol::Type::Path;
    case 5:
      return query::Symbol::Type::Number;
    case 6:
      return query::Symbol::Type::EdgeList;
  }
  CHECK(false);
}

namespace slk {
void Save(const query::Symbol &obj, slk::Builder *builder) {
  Save(obj.name(), builder);
  Save(obj.position(), builder);
  Save(Type2Int(obj.type()), builder);
  Save(obj.user_declared(), builder);
  Save(obj.token_position(), builder);
}

void Load(query::Symbol *obj, slk::Reader *reader) {
  std::string name;
  Load(&name, reader);
  int position = 0;
  Load(&position, reader);
  uint8_t type = 0;
  Load(&type, reader);
  bool user_declared = false;
  Load(&user_declared, reader);
  int token_position = 0;
  Load(&token_position, reader);
  *obj = query::Symbol(std::move(name), position, user_declared, Int2Type(type),
                       token_position);
}
}  // namespace slk

void SymbolVectorToCustom(const std::vector<query::Symbol> &symbols,
                          slk::Builder *builder) {
  slk::Save(symbols.size(), builder);
  for (int i = 0; i < symbols.size(); ++i) {
    slk::Save(symbols[i], builder);
  }
}

void CustomToSymbolVector(std::vector<query::Symbol> *symbols,
                          slk::Reader *reader) {
  uint64_t size = 0;
  slk::Load(&size, reader);
  symbols->resize(size);
  for (uint64_t i = 0; i < size; ++i) {
    slk::Load(&(*symbols)[i], reader);
  }
}

BENCHMARK_DEFINE_F(SymbolVectorFixture, CustomSerial)(benchmark::State &state) {
  while (state.KeepRunning()) {
    slk::Builder builder;
    SymbolVectorToCustom(symbols_, &builder);
  }
  state.SetItemsProcessed(state.iterations());
}

BENCHMARK_DEFINE_F(SymbolVectorFixture, CustomDeserial)
(benchmark::State &state) {
  slk::Builder builder;
  SymbolVectorToCustom(symbols_, &builder);
  while (state.KeepRunning()) {
    slk::Reader reader(builder.data(), builder.size());
    std::vector<query::Symbol> symbols;
    CustomToSymbolVector(&symbols, &reader);
  }
  state.SetItemsProcessed(state.iterations());
}

BENCHMARK_REGISTER_F(SymbolVectorFixture, CustomSerial)
    ->RangeMultiplier(4)
    ->Range(4, 1 << 12)
    ->Unit(benchmark::kNanosecond);

BENCHMARK_REGISTER_F(SymbolVectorFixture, CustomDeserial)
    ->RangeMultiplier(4)
    ->Range(4, 1 << 12)
    ->Unit(benchmark::kNanosecond);

BENCHMARK_MAIN();
