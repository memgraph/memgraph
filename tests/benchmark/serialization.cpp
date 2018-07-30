#include <sstream>
#include <string>

#include <benchmark/benchmark.h>

#include <capnp/serialize.h>
#include <kj/std/iostream.h>

#include "query/frontend/semantic/symbol.capnp.h"
#include "query/frontend/semantic/symbol.hpp"

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
}

BENCHMARK_REGISTER_F(SymbolVectorFixture, CapnpSerial)
    ->RangeMultiplier(4)
    ->Range(4, 1 << 12)
    ->Unit(benchmark::kNanosecond);

BENCHMARK_REGISTER_F(SymbolVectorFixture, CapnpDeserial)
    ->RangeMultiplier(4)
    ->Range(4, 1 << 12)
    ->Unit(benchmark::kNanosecond);

BENCHMARK_MAIN();
