#include <sstream>
#include <string>

#include <benchmark/benchmark.h>

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
