// Copyright 2026 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Writeback pacing bounds the dirty page cache *per file*, and a snapshot writes one file per
// worker thread, so the dirty data a snapshot hands the device at once is the window times the
// thread count rather than the window. `--storage_snapshot_writeback_window_mib` reads as one
// instance-wide number but does not behave like one.
//
// Each benchmark writes a fixed total number of bytes, split evenly across `threads` files, and
// reports aggregate throughput. The three arms differ only in how the window is chosen:
//
//   PacingOff          no pacing, the kernel decides when to write back
//   PacingPerFile      a full window per file, which is what a snapshot does today
//   PacingSplitBudget  the window divided by the thread count, holding the burst constant
//
// `sync_file_range` is a silent no-op on overlayfs, which would make all three arms identical and
// the result meaningless, so the filesystem is checked before anything is measured.

#include <sys/vfs.h>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <string>
#include <thread>
#include <vector>

#include <benchmark/benchmark.h>

#include "utils/file.hpp"

namespace {

constexpr size_t kMiB = 1024UL * 1024UL;
// What `--storage_snapshot_writeback_window_mib` defaults to.
constexpr size_t kDefaultWindow = 32 * kMiB;
constexpr size_t kChunk = 256 * 1024;

// Pacing has nothing to prevent until the unpaced arm can reach `vm.dirty_background_ratio`,
// which is a fraction of RAM, so a run small enough to be polite reports that pacing is pure
// overhead. The default is the largest total that stays well inside a small disk; raise it with
// `MG_BENCH_TOTAL_MIB` on a machine with more memory than the writeback threshold needs.
size_t TotalBytes() {
  if (auto const *from_env = std::getenv("MG_BENCH_TOTAL_MIB")) {
    return static_cast<size_t>(std::strtoull(from_env, nullptr, 10)) * kMiB;
  }
  return 1024 * kMiB;
}

// A filesystem where sync_file_range does nothing would report a difference of zero between the
// arms and look like good news.
constexpr uint32_t kOverlayfsMagic = 0x794c7630;
constexpr uint32_t kTmpfsMagic = 0x01021994;

std::filesystem::path ScratchDir() {
  if (auto const *from_env = std::getenv("MG_BENCH_SCRATCH_DIR")) return from_env;
  return std::filesystem::temp_directory_path();
}

// Returns why the directory cannot carry this measurement, or an empty string if it can.
std::string ScratchDirUnusable(std::filesystem::path const &dir) {
  struct statfs fs{};
  if (::statfs(dir.c_str(), &fs) != 0) return "statfs failed on " + dir.string();
  auto const magic = static_cast<uint32_t>(fs.f_type);
  if (magic == kOverlayfsMagic) return dir.string() + " is overlayfs, where sync_file_range does nothing";
  if (magic == kTmpfsMagic) return dir.string() + " is tmpfs, which has no writeback at all";
  return {};
}

// Returns whether every byte asked for was written. A writer that stops early finishes sooner,
// so a silent failure would be reported as throughput.
bool WriteOneFile(std::filesystem::path const &path, size_t bytes, size_t window) {
  std::vector<char> block(kChunk, 'x');
  memgraph::utils::NonConcurrentOutputFile file;
  if (!file.Open(path, memgraph::utils::NonConcurrentOutputFile::Mode::OVERWRITE_EXISTING)) return false;
  if (window != 0) file.EnableWritebackPacing(window, memgraph::utils::PageCachePolicy::kKeep);
  bool complete = true;
  for (size_t written = 0; written < bytes; written += kChunk) {
    if (!file.Write(block.data(), kChunk)) {
      complete = false;
      break;
    }
  }
  if (!file.Sync()) complete = false;
  file.Close();
  std::filesystem::remove(path);
  return complete;
}

// `window_for` turns a thread count into the window each file should use.
void RunArm(benchmark::State &state, auto &&window_for) {
  auto const threads = static_cast<size_t>(state.range(0));
  auto const dir = ScratchDir();
  if (auto const reason = ScratchDirUnusable(dir); !reason.empty()) {
    state.SkipWithError(reason.c_str());
    return;
  }

  auto const total_bytes = TotalBytes();
  auto const bytes_per_thread = total_bytes / threads;
  auto const window = window_for(threads);

  for (auto _ : state) {
    std::atomic<bool> all_complete{true};
    std::vector<std::jthread> writers;
    writers.reserve(threads);
    auto const start = std::chrono::steady_clock::now();
    for (size_t i = 0; i < threads; ++i) {
      writers.emplace_back([&, i] {
        if (!WriteOneFile(dir / ("mg_writeback_bench_" + std::to_string(i)), bytes_per_thread, window)) {
          all_complete.store(false, std::memory_order_relaxed);
        }
      });
    }
    writers.clear();  // joins
    auto const elapsed = std::chrono::duration<double>(std::chrono::steady_clock::now() - start).count();
    if (!all_complete.load(std::memory_order_relaxed)) {
      state.SkipWithError("a writer stopped early, so this time is not for the work asked for");
      return;
    }
    state.SetIterationTime(elapsed);
  }

  state.SetBytesProcessed(static_cast<int64_t>(state.iterations()) * static_cast<int64_t>(total_bytes));
  state.counters["window_mib"] = static_cast<double>(window) / static_cast<double>(kMiB);
  // What the whole snapshot can leave dirty at once, which is the number the flag reads like.
  state.counters["dirty_burst_mib"] = static_cast<double>(window * threads) / static_cast<double>(kMiB);
}

void PacingOff(benchmark::State &state) {
  RunArm(state, [](size_t) { return size_t{0}; });
}

void PacingPerFile(benchmark::State &state) {
  RunArm(state, [](size_t) { return kDefaultWindow; });
}

void PacingSplitBudget(benchmark::State &state) {
  RunArm(state, [](size_t threads) { return kDefaultWindow / threads; });
}

constexpr int kThreadCounts[] = {1, 4, 8, 16};

void AddThreadCounts(benchmark::internal::Benchmark *bench) {
  for (auto const threads : kThreadCounts) bench->Arg(threads);
  bench->UseManualTime()->Unit(benchmark::kMillisecond)->Iterations(2);
}

BENCHMARK(PacingOff)->Apply(AddThreadCounts);
BENCHMARK(PacingPerFile)->Apply(AddThreadCounts);
BENCHMARK(PacingSplitBudget)->Apply(AddThreadCounts);

}  // namespace

BENCHMARK_MAIN();
