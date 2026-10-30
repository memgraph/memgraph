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

// Shared by the branch recovery tests: a fork base in main, the branch and main captured as plain data (by names, so it
// compares across restarts), and attaching to a branch the way a session does.
#pragma once

#include <gtest/gtest.h>

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <expected>
#include <filesystem>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "dbms/database.hpp"
#include "storage/v2/inmemory/storage.hpp"
#include "storage/v2/property_value.hpp"
#include "storage/v2/view.hpp"
#include "tests/test_commit_args_helper.hpp"
#include "utils/scheduler.hpp"
#include "versioning/branch_engine.hpp"
#include "versioning/version_store.hpp"
#include "versioning/vertex_overlay_store.hpp"

namespace memgraph::tests::versioning_recovery {

namespace ms = memgraph::storage;
namespace mv = memgraph::versioning;

inline ms::Config MakeConfig(const std::filesystem::path &storage_directory, bool recover_on_startup,
                             bool versioning = true) {
  return ms::Config{
      .durability = {.storage_directory = storage_directory,
                     .recover_on_startup = recover_on_startup,
                     .snapshot_wal_mode = ms::Config::Durability::SnapshotWalMode::PERIODIC_SNAPSHOT_WITH_WAL,
                     // Effectively disable periodic snapshotting so recovery is WAL-only.
                     .snapshot_interval = memgraph::utils::SchedulerInterval{std::chrono::hours(24)}},
      .experimental_versioning = versioning,
  };
}

inline ms::InMemoryStorage *Mem(memgraph::dbms::Database &db) {
  return static_cast<ms::InMemoryStorage *>(db.storage());
}

inline void Commit(ms::Storage::Accessor &acc) {
  ASSERT_TRUE(acc.PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
}

inline std::filesystem::path BranchWalDir(const std::filesystem::path &storage_directory, uint64_t number) {
  return storage_directory / "branches" / std::to_string(number) / "wal";
}

// The regular files of `dir` (a WAL directory), oldest first: WAL file names start with a zero-padded timestamp.
inline std::vector<std::filesystem::path> WalFiles(const std::filesystem::path &dir) {
  std::vector<std::filesystem::path> files;
  if (!std::filesystem::exists(dir)) return files;
  for (const auto &entry : std::filesystem::directory_iterator(dir)) {
    if (entry.is_regular_file()) files.push_back(entry.path());
  }
  std::ranges::sort(files);
  return files;
}

struct Names {
  ms::LabelId l, m, x, n;
  ms::PropertyId p, q, w, a;
  ms::EdgeTypeId t;
};

inline Names ResolveNames(memgraph::dbms::Database &db) {
  auto *st = db.storage();
  return {st->NameToLabel("L"),
          st->NameToLabel("M"),
          st->NameToLabel("X"),
          st->NameToLabel("N"),
          st->NameToProperty("p"),
          st->NameToProperty("q"),
          st->NameToProperty("w"),
          st->NameToProperty("a"),
          st->NameToEdgeType("T")};
}

// v1(:L {p:1, q:5}) -e1[:T {w:1}]-> v2(:M);  v3(:L) -e2[:T {w:2}]-> v1;  v5(:L) -e3[:T {w:3}]-> v4;
struct Base {
  ms::Gid v1, v2, v3, v4, v5, e1, e2, e3;
};

inline Base CreateBase(memgraph::dbms::Database &db) {
  const auto n = ResolveNames(db);
  auto acc = db.Access(ms::WRITE);
  auto v1 = acc->CreateVertex();
  auto v2 = acc->CreateVertex();
  auto v3 = acc->CreateVertex();
  auto v4 = acc->CreateVertex();
  auto v5 = acc->CreateVertex();
  EXPECT_TRUE(v1.AddLabel(n.l).has_value());
  EXPECT_TRUE(v1.SetProperty(n.p, ms::PropertyValue(1)).has_value());
  EXPECT_TRUE(v1.SetProperty(n.q, ms::PropertyValue(5)).has_value());
  EXPECT_TRUE(v2.AddLabel(n.m).has_value());
  EXPECT_TRUE(v3.AddLabel(n.l).has_value());
  EXPECT_TRUE(v5.AddLabel(n.l).has_value());
  auto e1 = acc->CreateEdge(&v1, &v2, n.t);
  auto e2 = acc->CreateEdge(&v3, &v1, n.t);
  auto e3 = acc->CreateEdge(&v5, &v4, n.t);
  EXPECT_TRUE(e1.has_value() && e2.has_value() && e3.has_value());
  EXPECT_TRUE(e1->SetProperty(n.w, ms::PropertyValue(1)).has_value());
  EXPECT_TRUE(e2->SetProperty(n.w, ms::PropertyValue(2)).has_value());
  EXPECT_TRUE(e3->SetProperty(n.w, ms::PropertyValue(3)).has_value());
  Base base{v1.Gid(), v2.Gid(), v3.Gid(), v4.Gid(), v5.Gid(), e1->Gid(), e2->Gid(), e3->Gid()};
  Commit(*acc);
  return base;
}

// The branch as a session reads it, by names: vertices in gid order with labels, properties and out edges.
using NamedProps = std::map<std::string, ms::PropertyValue>;

struct EdgeDump {
  uint64_t gid;
  std::string type;
  uint64_t to;
  NamedProps props;
  friend bool operator==(const EdgeDump &, const EdgeDump &) = default;
};

struct VertexDump {
  uint64_t gid;
  std::set<std::string> labels;
  NamedProps props;
  std::vector<EdgeDump> out;
  friend bool operator==(const VertexDump &, const VertexDump &) = default;
};

inline NamedProps Named(ms::Storage &st, const std::map<ms::PropertyId, ms::PropertyValue> &props) {
  NamedProps named;
  for (const auto &[pid, value] : props) named.emplace(st.PropertyToName(pid), value);
  return named;
}

inline void ApplyOverlayProps(ms::Storage &st, const mv::OverlayVersion &version, NamedProps &props) {
  for (const auto pid : version.removed_pids()) props.erase(st.PropertyToName(pid));
  for (const auto &[pid, value] : version.properties().Properties())
    props.insert_or_assign(st.PropertyToName(pid), value);
}

inline void Finish(std::vector<VertexDump> &dump) {
  for (auto &v : dump) {
    std::ranges::sort(v.out, {}, [](const EdgeDump &e) { return e.gid; });
  }
  std::ranges::sort(dump, {}, [](const VertexDump &v) { return v.gid; });
}

inline std::vector<VertexDump> DumpBranch(memgraph::dbms::Database &db, mv::BranchContext &ctx) {
  auto &st = *db.storage();
  std::vector<VertexDump> dump;
  auto diff_acc = ctx.diff_engine().Access(ms::READ);
  EXPECT_TRUE(ctx.BeginTransaction(diff_acc.get()).has_value());
  for (auto v : ctx.Vertices(ms::View::NEW)) {
    VertexDump d;
    d.gid = v.Gid().AsUint();
    const bool fork = ctx.IsForkVertexGid(v.Gid());
    const auto view = fork ? ms::View::OLD : ms::View::NEW;
    for (const auto label : *v.Labels(view)) d.labels.insert(st.LabelToName(label));
    d.props = Named(st, *v.Properties(view));
    if (fork) {
      if (const auto *version = ctx.ReadOverlayVersion(v.Gid(), ms::View::NEW)) {
        for (const auto label : version->removed_labels()) d.labels.erase(st.LabelToName(label));
        for (const auto label : version->added_labels()) d.labels.insert(st.LabelToName(label));
        ApplyOverlayProps(st, *version, d.props);
      }
    }
    for (const auto &e : ctx.ResolveEdges(v.Gid(), ms::EdgeDirection::OUT, ms::View::NEW)) {
      EdgeDump ed;
      ed.gid = e.Gid().AsUint();
      ed.type = st.EdgeTypeToName(e.EdgeType());
      ed.to = e.ToVertex().Gid().AsUint();
      const bool fork_edge = ctx.IsForkEdgeGid(e.Gid());
      ed.props = Named(st, *e.Properties(fork_edge ? ms::View::OLD : ms::View::NEW));
      if (fork_edge) {
        if (const auto *version = ctx.ReadEdgeOverlayVersion(e.Gid(), ms::View::NEW)) {
          ApplyOverlayProps(st, *version, ed.props);
        }
      }
      d.out.push_back(std::move(ed));
    }
    dump.push_back(std::move(d));
  }
  ctx.EndTransaction();
  Finish(dump);
  return dump;
}

inline std::vector<VertexDump> DumpMain(memgraph::dbms::Database &db) {
  auto &st = *db.storage();
  std::vector<VertexDump> dump;
  auto acc = db.Access(ms::READ);
  for (auto v : acc->Vertices(ms::View::OLD)) {
    VertexDump d;
    d.gid = v.Gid().AsUint();
    for (const auto label : *v.Labels(ms::View::OLD)) d.labels.insert(st.LabelToName(label));
    d.props = Named(st, *v.Properties(ms::View::OLD));
    for (const auto &e : v.OutEdges(ms::View::OLD)->edges) {
      d.out.push_back({.gid = e.Gid().AsUint(),
                       .type = st.EdgeTypeToName(e.EdgeType()),
                       .to = e.ToVertex().Gid().AsUint(),
                       .props = Named(st, *e.Properties(ms::View::OLD))});
    }
    dump.push_back(std::move(d));
  }
  Finish(dump);
  return dump;
}

inline const VertexDump *FindVertex(const std::vector<VertexDump> &dump, ms::Gid gid) {
  const auto it = std::ranges::find(dump, gid.AsUint(), &VertexDump::gid);
  return it == dump.end() ? nullptr : &*it;
}

// Attaches to `name` the way USE BRANCH does: the registry's core, built on first use, with its floors recorded.
inline std::unique_ptr<mv::BranchContext> Checkout(memgraph::dbms::Database &db, std::string_view name,
                                                   bool *built = nullptr) {
  auto *store = db.version_store();
  auto info = store->Get(name);
  EXPECT_TRUE(info.has_value());
  if (!info) return nullptr;
  auto core = store->GetOrBuildCore(info->number, [&]() -> std::expected<std::shared_ptr<mv::BranchCore>, std::string> {
    if (built != nullptr) *built = true;
    auto fresh = mv::BranchCore::Build(*Mem(db), info->fork_ts, info->number);
    if (!fresh) return std::unexpected(fresh.error().message);
    return std::move(*fresh);
  });
  EXPECT_TRUE(core.has_value());
  if (!core) return nullptr;
  EXPECT_TRUE(store->RecordGidFloors(info->number, (*core)->vertex_gid_floor(), (*core)->edge_gid_floor()).has_value());
  return std::make_unique<mv::BranchContext>(*core);
}

// Runs `body` as one branch transaction that commits. `diff` is the transaction's diff-engine accessor.
template <typename Body>
void BranchTxn(mv::BranchContext &ctx, Body &&body) {
  auto diff_acc = ctx.diff_engine().Access(ms::WRITE);
  ASSERT_TRUE(ctx.BeginTransaction(diff_acc.get()).has_value());
  body(ctx, *diff_acc);
  ASSERT_TRUE(ctx.CommitTransaction(memgraph::tests::MakeMainCommitArgs()).has_value());
  ctx.EndTransaction();
}

}  // namespace memgraph::tests::versioning_recovery
