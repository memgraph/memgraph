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

#include <algorithm>
#include <atomic>
#include <bit>
#include <cstdint>
#include <functional>
#include <thread>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

#include "storage/v2/vertex_row_filter.hpp"
#include "versioning/branch_change_filter.hpp"

namespace ms = memgraph::storage;
using memgraph::versioning::BranchChangeFilters;

namespace {

ms::Gid G(uint64_t gid) { return ms::Gid::FromUint(gid); }

ms::PropertyId P(uint64_t pid) { return ms::PropertyId::FromUint(pid); }

// One bool per kind, so a record into one kind can be checked against every other.
std::vector<bool> Probe(const BranchChangeFilters &f, ms::Gid gid, ms::PropertyId pid) {
  return {f.MayHavePropertyChange(gid),
          f.MayHaveLabelChange(gid),
          f.MayHaveEdgeChange(gid, ms::EdgeDirection::OUT),
          f.MayHaveVertexDeleted(gid),
          f.MayHavePropertyFieldChange(gid, pid),
          f.MayHaveBaseEdgePropertyChange(gid),
          f.MayHaveBaseEdgePropertyFieldChange(gid, pid),
          f.MayHaveBaseEdgeDeleted(gid)};
}

}  // namespace

TEST(VersioningBranchChangeFilter, EachRecordHitsOnlyItsOwnKind) {
  using Record = void (*)(BranchChangeFilters &, ms::Gid, ms::PropertyId);
  const std::vector<std::pair<Record, std::vector<bool>>> cases = {
      {[](auto &f, auto g, auto) { f.RecordPropertyChange(g); }, {1, 0, 0, 0, 0, 0, 0, 0}},
      {[](auto &f, auto g, auto) { f.RecordLabelChange(g); }, {0, 1, 0, 0, 0, 0, 0, 0}},
      {[](auto &f, auto g, auto) { f.RecordEdgeChange(g, ms::EdgeDirection::OUT); }, {0, 0, 1, 0, 0, 0, 0, 0}},
      {[](auto &f, auto g, auto) { f.RecordVertexDeleted(g); }, {0, 0, 0, 1, 0, 0, 0, 0}},
      {[](auto &f, auto g, auto p) { f.RecordPropertyField(g, p); }, {0, 0, 0, 0, 1, 0, 0, 0}},
      {[](auto &f, auto g, auto p) { f.RecordBaseEdgeProperty(g, p); }, {0, 0, 0, 0, 0, 1, 1, 0}},
      {[](auto &f, auto g, auto) { f.RecordBaseEdgeDeleted(g); }, {0, 0, 0, 0, 0, 0, 0, 1}},
  };
  for (size_t i = 0; i < cases.size(); ++i) {
    BranchChangeFilters filters;
    EXPECT_EQ(Probe(filters, G(1), P(7)), std::vector<bool>(8, false)) << i;
    cases[i].first(filters, G(1), P(7));
    EXPECT_EQ(Probe(filters, G(1), P(7)), cases[i].second) << i;
    EXPECT_EQ(Probe(filters, G(1'000'000), P(8)), std::vector<bool>(8, false)) << "unrecorded gid and pid, case " << i;
  }
}

// The per-vertex probes a scan tests inline stand in for the kind filters: the change probe is fed by every record that
// can make a scan lose or move a vertex, the dropped probe only by those that can make a label scan lose it.
TEST(VersioningBranchChangeFilter, VertexProbesAreFedByTheRecordsTheyStandFor) {
  using Record = void (*)(BranchChangeFilters &, ms::Gid, ms::PropertyId);

  struct Case {
    Record record;
    bool change;
    bool dropped;
  };

  const std::vector<Case> cases = {
      {[](auto &f, auto g, auto) { f.RecordVertexDeleted(g); }, true, true},
      {[](auto &f, auto g, auto) { f.RecordLabelChange(g); }, true, true},
      {[](auto &f, auto g, auto p) { f.RecordPropertyField(g, p); }, true, false},
      {[](auto &f, auto g, auto) { f.RecordPropertyChange(g); }, false, false},
      {[](auto &f, auto g, auto) { f.RecordEdgeChange(g, ms::EdgeDirection::OUT); }, false, false},
      {[](auto &f, auto g, auto) { f.RecordBaseEdgeDeleted(g); }, false, false},
  };
  for (size_t i = 0; i < cases.size(); ++i) {
    BranchChangeFilters filters;
    EXPECT_FALSE(filters.MayHaveVertexChange(G(1))) << i;
    EXPECT_FALSE(filters.MayHaveVertexDropped(G(1))) << i;
    cases[i].record(filters, G(1), P(7));
    EXPECT_EQ(filters.MayHaveVertexChange(G(1)), cases[i].change) << i;
    EXPECT_EQ(filters.MayHaveVertexDropped(G(1)), cases[i].dropped) << i;
    EXPECT_EQ(filters.VertexChangeProbe().MayContain(G(1)), cases[i].change) << "the inline view agrees, case " << i;
    EXPECT_EQ(filters.VertexDroppedProbe().MayContain(G(1)), cases[i].dropped) << i;
    EXPECT_FALSE(filters.MayHaveVertexChange(G(1'000'000))) << "an unrecorded gid, case " << i;
  }
}

TEST(VersioningBranchChangeFilter, EdgeChangeIsPerDirection) {
  BranchChangeFilters filters;
  filters.RecordEdgeChange(G(1), ms::EdgeDirection::OUT);
  filters.RecordEdgeChange(G(2), ms::EdgeDirection::IN);
  EXPECT_TRUE(filters.MayHaveEdgeChange(G(1), ms::EdgeDirection::OUT));
  EXPECT_FALSE(filters.MayHaveEdgeChange(G(1), ms::EdgeDirection::IN));
  EXPECT_TRUE(filters.MayHaveEdgeChange(G(2), ms::EdgeDirection::IN));
  EXPECT_FALSE(filters.MayHaveEdgeChange(G(2), ms::EdgeDirection::OUT));
}

TEST(VersioningBranchChangeFilter, LabelGateNeverMissesARecordedLabel) {
  BranchChangeFilters filters;
  const auto l = [](uint64_t id) { return ms::LabelId::FromUint(id); };
  EXPECT_FALSE(filters.MayHaveLabelTouched(l(3)));
  filters.RecordLabelTouched(l(3));
  EXPECT_TRUE(filters.MayHaveLabelTouched(l(3)));
  EXPECT_FALSE(filters.MayHaveLabelTouched(l(4)));
}

TEST(VersioningBranchChangeFilter, PidGateNeverHitsForAnUnwrittenPid) {
  BranchChangeFilters filters;
  for (uint64_t gid = 0; gid < 100'000; ++gid) {
    filters.RecordPropertyField(G(gid), P(7));
    filters.RecordBaseEdgeProperty(G(gid), P(7));
  }
  EXPECT_TRUE(filters.MayHavePropertyTouched(P(7)));
  EXPECT_FALSE(filters.MayHavePropertyTouched(P(8)));
  for (uint64_t gid = 0; gid < 10'000; ++gid) {
    ASSERT_TRUE(filters.MayHavePropertyFieldChange(G(gid), P(7)));
    ASSERT_TRUE(filters.MayHaveBaseEdgePropertyFieldChange(G(gid), P(7)));
    ASSERT_FALSE(filters.MayHavePropertyFieldChange(G(gid), P(8)));
    ASSERT_FALSE(filters.MayHaveBaseEdgePropertyFieldChange(G(gid), P(8)));
  }
}

// Gids that agree modulo the filter size must not all share one word and bit pair.
TEST(VersioningBranchChangeFilter, MultiplesOfTheFilterSizeDoNotAllCollide) {
  BranchChangeFilters filters;
  constexpr uint64_t kStride = BranchChangeFilters::kFilterWords;
  for (uint64_t i = 1; i <= 64; ++i) filters.RecordLabelChange(G(i * kStride));
  int hits = 0;
  for (uint64_t i = 65; i <= 128; ++i) hits += filters.MayHaveLabelChange(G(i * kStride)) ? 1 : 0;
  EXPECT_LT(hits, 8);
}

TEST(VersioningBranchChangeFilter, ConcurrentRecordsAreNeverMissedByReaders) {
  constexpr int kWriters = 4;
  constexpr uint64_t kPerWriter = 20'000;  // loads the filters heavily: false positives allowed, negatives not
  BranchChangeFilters filters;
  std::vector<std::atomic<uint64_t>> published(kWriters);
  std::atomic<bool> done{false};
  std::atomic<int> missed{0};

  auto gid_of = [](int w, uint64_t i) { return G(i * kWriters + w); };
  auto expect_all = [&](ms::Gid gid) {
    const auto pid = P(gid.AsUint() % 300);
    const auto hits = Probe(filters, gid, pid);
    return std::ranges::all_of(hits, std::identity{}) && filters.MayHaveAnyVertexDeleted();
  };

  std::vector<std::thread> readers;
  for (int r = 0; r < 2; ++r) {
    readers.emplace_back([&] {
      while (!done.load(std::memory_order_acquire)) {
        for (int w = 0; w < kWriters; ++w) {
          const uint64_t n = published[w].load(std::memory_order_acquire);
          for (uint64_t i = n > 64 ? n - 64 : 0; i < n; ++i) {
            if (!expect_all(gid_of(w, i))) missed.fetch_add(1);
          }
        }
      }
    });
  }
  std::vector<std::thread> writers;
  for (int w = 0; w < kWriters; ++w) {
    writers.emplace_back([&, w] {
      for (uint64_t i = 0; i < kPerWriter; ++i) {
        const auto gid = gid_of(w, i);
        const auto pid = P(gid.AsUint() % 300);
        filters.RecordPropertyChange(gid);
        filters.RecordLabelChange(gid);
        filters.RecordPropertyField(gid, pid);
        filters.RecordEdgeChange(gid, ms::EdgeDirection::OUT);
        filters.RecordVertexDeleted(gid);
        filters.RecordBaseEdgeProperty(gid, pid);
        filters.RecordBaseEdgeDeleted(gid);
        published[w].store(i + 1, std::memory_order_release);
      }
    });
  }
  for (auto &t : writers) t.join();
  done.store(true, std::memory_order_release);
  for (auto &t : readers) t.join();
  EXPECT_EQ(missed.load(), 0);

  for (int w = 0; w < kWriters; ++w) {
    for (uint64_t i = 0; i < kPerWriter; ++i) ASSERT_TRUE(expect_all(gid_of(w, i))) << gid_of(w, i).AsUint();
  }
}

namespace {

// pids {1,2,3} over every gid, pid 4 on every fourth gid.
template <typename F>
void ForEachWrittenPair(uint64_t gids, F &&f) {
  for (uint64_t g = 0; g < gids; ++g) {
    for (uint64_t pid = 1; pid <= 3; ++pid) f(g, pid);
    if (g % 4 == 0) f(g, 4);
  }
}

}  // namespace

TEST(VersioningBranchChangeFilter, SizedPropertyFieldStaysSelectiveAtHighLoad) {
  constexpr uint64_t kGids = 68'000;
  BranchChangeFilters filters(100'000);
  uint64_t inserted = 0;
  ForEachWrittenPair(kGids, [&](uint64_t g, uint64_t pid) {
    filters.RecordPropertyField(G(g), P(pid));
    ++inserted;
  });
  EXPECT_GE(inserted, 200'000U);

  ForEachWrittenPair(kGids, [&](uint64_t g, uint64_t pid) {
    ASSERT_TRUE(filters.MayHavePropertyFieldChange(G(g), P(pid))) << g << " " << pid;
  });

  // Never-inserted pairs: gated pids on gids that did not write them, and gids beyond the written range.
  uint64_t probes = 0;
  uint64_t false_positives = 0;
  for (uint64_t g = 0; g < kGids; ++g) {
    if (g % 4 != 0) {
      ++probes;
      false_positives += filters.MayHavePropertyFieldChange(G(g), P(4));
    }
  }
  for (uint64_t g = kGids; g < kGids + 70'000; ++g) {
    for (uint64_t pid = 1; pid <= 3; ++pid) {
      ++probes;
      false_positives += filters.MayHavePropertyFieldChange(G(g), P(pid));
    }
  }
  EXPECT_GE(probes, 200'000U);
  EXPECT_LT(static_cast<double>(false_positives) / static_cast<double>(probes), 0.08);
}

TEST(VersioningBranchChangeFilter, LazyPropertyFieldConcurrentFirstRecord) {
  constexpr int kWriters = 4;
  constexpr uint64_t kPerWriter = 5'000;
  BranchChangeFilters filters(100'000);
  std::atomic<bool> go{false};
  std::vector<std::thread> writers;
  for (int w = 0; w < kWriters; ++w) {
    writers.emplace_back([&, w] {
      while (!go.load(std::memory_order_acquire)) std::this_thread::yield();
      for (uint64_t i = 0; i < kPerWriter; ++i) filters.RecordPropertyField(G(w * kPerWriter + i), P(5));
    });
  }
  go.store(true, std::memory_order_release);
  for (auto &t : writers) t.join();
  for (uint64_t g = 0; g < kWriters * kPerWriter; ++g) {
    ASSERT_TRUE(filters.MayHavePropertyFieldChange(G(g), P(5))) << g;
  }
}

// A filter nothing was recorded into holds no words; a probe taken then must still see what is recorded later, since a
// scan may start before the branch's first write.
TEST(VersioningBranchChangeFilter, ProbeTakenBeforeTheFirstRecordSeesLaterRecords) {
  BranchChangeFilters filters;
  auto const change = filters.VertexChangeProbe();
  auto const dropped = filters.VertexDroppedProbe();
  EXPECT_FALSE(change.MayContain(G(1)));
  EXPECT_FALSE(dropped.MayContain(G(1)));
  filters.RecordPropertyField(G(1), P(7));
  EXPECT_TRUE(change.MayContain(G(1)));
  EXPECT_FALSE(dropped.MayContain(G(1)));
  filters.RecordLabelChange(G(2));
  EXPECT_TRUE(change.MayContain(G(2)));
  EXPECT_TRUE(dropped.MayContain(G(2)));
  EXPECT_FALSE(change.MayContain(G(1'000'000)));
}

TEST(VersioningBranchChangeFilter, LazyFilterReadsAbsentBeforeAllocationAndNeverMissesAfter) {
  ms::LazyGidFilter filter(BranchChangeFilters::kFilterWords);
  for (uint64_t g = 0; g < 1'000; ++g) ASSERT_FALSE(filter.MayContain(G(g))) << g;
  EXPECT_EQ(filter.Probe().words, nullptr) << "nothing is allocated before the first insert";
  for (uint64_t g = 0; g < 1'000; ++g) {
    filter.Insert(G(g));
    for (uint64_t seen = 0; seen <= g; ++seen) ASSERT_TRUE(filter.MayContain(G(seen))) << g << " " << seen;
  }
  EXPECT_NE(filter.Probe().words, nullptr);
}

// Every kind allocates on its own first record; threads race on each kind's first insert, one allocation wins and no
// insert is lost.
TEST(VersioningBranchChangeFilter, ConcurrentFirstRecordsOfEveryKindAreAllVisible) {
  constexpr int kWriters = 8;
  constexpr uint64_t kPerWriter = 2'000;
  BranchChangeFilters filters;
  std::atomic<int> ready{0};
  std::atomic<bool> go{false};
  std::vector<std::thread> writers;
  for (int w = 0; w < kWriters; ++w) {
    writers.emplace_back([&, w] {
      ready.fetch_add(1, std::memory_order_acq_rel);
      while (!go.load(std::memory_order_acquire)) std::this_thread::yield();
      for (uint64_t i = 0; i < kPerWriter; ++i) {
        auto const g = G(w * kPerWriter + i);
        filters.RecordPropertyChange(g);
        filters.RecordLabelChange(g);
        filters.RecordEdgeChange(g, ms::EdgeDirection::IN);
        filters.RecordPropertyField(g, P(3));
        filters.RecordBaseEdgeProperty(g, P(3));
        filters.RecordBaseEdgeDeleted(g);
        if (i % 2 == 0) filters.RecordVertexDeleted(g);
      }
    });
  }
  while (ready.load(std::memory_order_acquire) != kWriters) std::this_thread::yield();
  go.store(true, std::memory_order_release);
  for (auto &t : writers) t.join();
  for (uint64_t g = 0; g < kWriters * kPerWriter; ++g) {
    ASSERT_TRUE(filters.MayHavePropertyChange(G(g))) << g;
    ASSERT_TRUE(filters.MayHaveLabelChange(G(g))) << g;
    ASSERT_TRUE(filters.MayHaveEdgeChange(G(g), ms::EdgeDirection::IN)) << g;
    ASSERT_TRUE(filters.MayHavePropertyFieldChange(G(g), P(3))) << g;
    ASSERT_TRUE(filters.MayHaveBaseEdgePropertyChange(G(g))) << g;
    ASSERT_TRUE(filters.MayHaveBaseEdgeDeleted(G(g))) << g;
    ASSERT_TRUE(filters.MayHaveVertexChange(G(g))) << g;
    ASSERT_TRUE(filters.MayHaveVertexDropped(G(g))) << g;
    if (g % kPerWriter % 2 == 0) ASSERT_TRUE(filters.MayHaveVertexDeleted(G(g))) << g;
  }
}

TEST(VersioningBranchChangeFilter, ZeroVertexFilterMatchesDefault) {
  BranchChangeFilters filters(0);
  EXPECT_FALSE(filters.MayHavePropertyFieldChange(G(1), P(1)));
  for (uint64_t g = 0; g < 2'000; ++g) filters.RecordPropertyField(G(g), P(g % 3));
  for (uint64_t g = 0; g < 2'000; ++g) ASSERT_TRUE(filters.MayHavePropertyFieldChange(G(g), P(g % 3))) << g;
}

namespace {

uint64_t SplitMix(uint64_t x) {
  x += 0x9E3779B97F4A7C15ULL;
  x = (x ^ (x >> 30U)) * 0xBF58476D1CE4E5B9ULL;
  x = (x ^ (x >> 27U)) * 0x94D049BB133111EBULL;
  return x ^ (x >> 31U);
}

// Scans `words * kHitsPerWord` gids; the bit patterns of the gids landing in one fixed word must stay varied, i.e. the
// bit positions must not be pinned by the word index.
template <typename GidOf>
void CheckBitsIndependentOfWord(size_t words, GidOf &&gid_of) {
  constexpr uint64_t kHitsPerWord = 128;
  unsigned const shift = ms::GidBloomProbe::IndexShift(words);
  size_t const target = words / 3;
  uint64_t const total = words * kHitsPerWord;
  std::vector<uint64_t> hits;
  uint64_t collapsed = 0;
  for (uint64_t i = 0; i < total; ++i) {
    auto const [idx, bits] = ms::GidBloomProbe::Locate(gid_of(i), shift);
    ASSERT_LT(idx, words);
    if (std::popcount(bits) == 1) ++collapsed;
    if (idx == target) hits.push_back(bits);
  }
  ASSERT_GE(hits.size(), 64U) << words;
  uint64_t seen = 0;
  for (auto b : hits) seen |= b;
  std::ranges::sort(hits);
  auto const distinct = static_cast<size_t>(std::ranges::unique(hits).begin() - hits.begin());
  EXPECT_GE(distinct * 10, hits.size() * 8) << "word " << words << ": bit pairs repeat inside one word";
  EXPECT_GE(std::popcount(seen), 56) << "word " << words << ": bit positions pinned by the word index";
  EXPECT_LE(collapsed * 1000, total * 25) << "word " << words << ": bit_a == bit_b too often";
}

}  // namespace

TEST(VersioningBranchChangeFilter, BloomBitPositionsAreIndependentOfTheWordIndex) {
  for (unsigned log2_words : {12U, 14U, 16U, 18U, 20U}) {
    size_t const words = size_t{1} << log2_words;
    CheckBitsIndependentOfWord(words, [](uint64_t i) { return i; });
    CheckBitsIndependentOfWord(words, [](uint64_t i) { return SplitMix(i); });
  }
}

TEST(VersioningBranchChangeFilter, BloomLayoutAtDefaultSizeIsUnchanged) {
  constexpr size_t kWords = 4096;
  unsigned const shift = ms::GidBloomProbe::IndexShift(kWords);
  ASSERT_EQ(shift, 52U);
  for (uint64_t gid : {0ULL, 1ULL, 2ULL, 12345ULL, 0xFFFFFFFFULL, 0xDEADBEEFCAFEF00DULL}) {
    uint64_t const h = gid * 0x9E3779B97F4A7C15ULL;
    auto const slot = ms::GidBloomProbe::Locate(gid, shift);
    EXPECT_EQ(slot.idx, h >> 52U);
    EXPECT_EQ(slot.bits, (1ULL << ((h >> 46U) & 63U)) | (1ULL << ((h >> 40U) & 63U)));
  }
}

namespace {

double BloomFalsePositiveRate(size_t words, uint64_t inserted, uint64_t probed) {
  std::vector<std::atomic<uint64_t>> table(words);
  unsigned const shift = ms::GidBloomProbe::IndexShift(words);
  for (uint64_t i = 0; i < inserted; ++i) {
    auto const [idx, bits] = ms::GidBloomProbe::Locate(SplitMix(i), shift);
    table[idx].fetch_or(bits, std::memory_order_relaxed);
  }
  ms::GidBloomProbe const probe{.words = table.data(), .index_shift = shift};
  for (uint64_t i = 0; i < inserted; ++i) {
    EXPECT_TRUE(probe.MayContain(G(SplitMix(i)))) << i;
  }
  uint64_t fp = 0;
  for (uint64_t i = 0; i < probed; ++i) {
    if (probe.MayContain(G(SplitMix((uint64_t{1} << 40U) + i)))) ++fp;
  }
  return static_cast<double>(fp) / static_cast<double>(probed);
}

}  // namespace

TEST(VersioningBranchChangeFilter, BloomFalsePositiveRateStaysLowAtLargeSize) {
  EXPECT_LT(BloomFalsePositiveRate(size_t{1} << 16U, 200'000, 1'000'000), 0.03);
}

TEST(VersioningBranchChangeFilter, BloomFalsePositiveRateStaysLowAtDefaultSize) {
  EXPECT_LT(BloomFalsePositiveRate(size_t{1} << 12U, 10'000, 1'000'000), 0.03);
}
