#include <benchmark/benchmark.h>
#include <benchmark/benchmark_api.h>
#include <glog/logging.h>

#include "mvcc/record.hpp"
#include "mvcc/version_list.hpp"

class Prop : public mvcc::Record<Prop> {};

// Benchmark multiple updates, and finds, focused on finds.
// This a rather weak test, but I'm not sure what's the better way to test this
// in the future.
// TODO(dgleich): Refresh this.
void MvccMix(benchmark::State &state) {
  while (state.KeepRunning()) {
    state.PauseTiming();
    tx::Engine engine;
    auto t1 = engine.Begin();
    mvcc::VersionList<Prop> version_list(*t1);

    t1->Commit();
    auto t2 = engine.Begin();

    state.ResumeTiming();
    version_list.update(*t2);
    state.PauseTiming();

    state.ResumeTiming();
    version_list.find(*t2);
    state.PauseTiming();

    t2->Abort();

    auto t3 = engine.Begin();
    state.ResumeTiming();
    version_list.update(*t3);
    state.PauseTiming();
    auto t4 = engine.Begin();

    // Repeat find state.range(0) number of times.
    state.ResumeTiming();
    for (int i = 0; i < state.range(0); ++i) {
      version_list.find(*t4);
    }
    state.PauseTiming();

    t3->Commit();
    t4->Commit();
    state.ResumeTiming();
  }
}

BENCHMARK(MvccMix)
    ->RangeMultiplier(2)       // Multiply next range testdata size by 2
    ->Range(1 << 14, 1 << 23)  // 1<<14, 1<<15, 1<<16, ...
    ->Unit(benchmark::kMillisecond);

DEFINE_string(hehehe, "bok", "ne");
int main(int argc, char **argv) {
  google::InitGoogleLogging(argv[0]);

  ::benchmark::Initialize(&argc, argv);
  ::benchmark::RunSpecifiedBenchmarks();
  return 0;
}
