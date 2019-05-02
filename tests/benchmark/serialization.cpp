#include <sstream>
#include <string>

#include <benchmark/benchmark.h>

#include <capnp/serialize.h>
#include <kj/std/iostream.h>

#include "query/distributed/frontend/semantic/symbol_serialization.hpp"
#include "query/frontend/semantic/symbol.hpp"
#include "slk/serialization.hpp"

class SymbolVectorFixture : public benchmark::Fixture {
 protected:
  std::vector<query::Symbol> symbols_;

  void SetUp(const benchmark::State &state) override {
    using Type = ::query::Symbol::Type;
    std::vector<Type> types{Type::ANY,  Type::VERTEX, Type::EDGE,
                            Type::PATH, Type::NUMBER, Type::EDGE_LIST};
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
                           sym.getUserDeclared(), query::Symbol::Type::ANY,
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

void SymbolVectorToSlk(const std::vector<query::Symbol> &symbols,
                       slk::Builder *builder) {
  slk::Save(symbols.size(), builder);
  for (int i = 0; i < symbols.size(); ++i) {
    slk::Save(symbols[i], builder);
  }
}

void SlkToSymbolVector(std::vector<query::Symbol> *symbols,
                       slk::Reader *reader) {
  uint64_t size = 0;
  slk::Load(&size, reader);
  symbols->resize(size);
  for (uint64_t i = 0; i < size; ++i) {
    slk::Load(&(*symbols)[i], reader);
  }
}

BENCHMARK_DEFINE_F(SymbolVectorFixture, SlkSerial)(benchmark::State &state) {
  while (state.KeepRunning()) {
    slk::Builder builder([](const uint8_t *, size_t, bool) {});
    SymbolVectorToSlk(symbols_, &builder);
    builder.Finalize();
  }
  state.SetItemsProcessed(state.iterations());
}

BENCHMARK_DEFINE_F(SymbolVectorFixture, SlkDeserial)
(benchmark::State &state) {
  std::vector<uint8_t> encoded;
  slk::Builder builder(
      [&encoded](const uint8_t *data, size_t size, bool have_more) {
        for (size_t i = 0; i < size; ++i) encoded.push_back(data[i]);
      });
  SymbolVectorToSlk(symbols_, &builder);
  builder.Finalize();

  while (state.KeepRunning()) {
    slk::Reader reader(encoded.data(), encoded.size());
    std::vector<query::Symbol> symbols;
    SlkToSymbolVector(&symbols, &reader);
  }
  state.SetItemsProcessed(state.iterations());
}

BENCHMARK_REGISTER_F(SymbolVectorFixture, SlkSerial)
    ->RangeMultiplier(4)
    ->Range(4, 1 << 12)
    ->Unit(benchmark::kNanosecond);

BENCHMARK_REGISTER_F(SymbolVectorFixture, SlkDeserial)
    ->RangeMultiplier(4)
    ->Range(4, 1 << 12)
    ->Unit(benchmark::kNanosecond);

BENCHMARK_MAIN();
