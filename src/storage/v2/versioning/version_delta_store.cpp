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

#include "storage/v2/versioning/version_delta_store.hpp"

#include <cstdint>
#include <string>
#include <string_view>
#include <system_error>

namespace memgraph::storage {

namespace {

// Op-log key layout: every delta is stored under "d/" + a 20-digit zero-padded sequence number, so
// RocksDB's lexicographic key order matches insertion order.
constexpr std::string_view kDeltaPrefix = "d/";
constexpr int kSeqWidth = 20;  // digits in UINT64_MAX

std::string DeltaKey(uint64_t seq) {
  auto digits = std::to_string(seq);
  return std::string{kDeltaPrefix} + std::string(kSeqWidth - digits.size(), '0') + digits;
}

// --- Minimal little-endian binary codec (dependency-free, total over all fields) ---
void PutU8(std::string &out, uint8_t v) { out.push_back(static_cast<char>(v)); }

void PutU32(std::string &out, uint32_t v) {
  for (int i = 0; i < 4; ++i) out.push_back(static_cast<char>((v >> (8 * i)) & 0xFF));
}

void PutU64(std::string &out, uint64_t v) {
  for (int i = 0; i < 8; ++i) out.push_back(static_cast<char>((v >> (8 * i)) & 0xFF));
}

void PutStr(std::string &out, std::string_view s) {
  PutU32(out, static_cast<uint32_t>(s.size()));
  out.append(s);
}

struct Reader {
  std::string_view buf;
  size_t pos{0};

  uint8_t U8() { return static_cast<uint8_t>(buf[pos++]); }

  uint32_t U32() {
    uint32_t v = 0;
    for (int i = 0; i < 4; ++i) v |= static_cast<uint32_t>(static_cast<uint8_t>(buf[pos++])) << (8 * i);
    return v;
  }

  uint64_t U64() {
    uint64_t v = 0;
    for (int i = 0; i < 8; ++i) v |= static_cast<uint64_t>(static_cast<uint8_t>(buf[pos++])) << (8 * i);
    return v;
  }

  std::string Str() {
    auto n = U32();
    std::string s{buf.substr(pos, n)};
    pos += n;
    return s;
  }
};

std::string EncodeDelta(const OverlayDelta &d) {
  std::string out;
  PutU8(out, static_cast<uint8_t>(d.op));
  PutU64(out, d.gid);
  PutU32(out, d.label_id);
  PutU32(out, d.property_id);
  PutU32(out, d.edge_type_id);
  PutU64(out, d.from_gid);
  PutU64(out, d.to_gid);
  PutStr(out, d.properties);
  PutU32(out, static_cast<uint32_t>(d.labels.size()));
  for (auto label : d.labels) PutU32(out, label);
  PutU64(out, d.txn_start_timestamp);
  PutU64(out, d.ledger_time_ns);
  PutStr(out, d.query);
  return out;
}

OverlayDelta DecodeDelta(std::string_view value) {
  Reader r{value};
  OverlayDelta d;
  d.op = static_cast<OverlayOp>(r.U8());
  d.gid = r.U64();
  d.label_id = r.U32();
  d.property_id = r.U32();
  d.edge_type_id = r.U32();
  d.from_gid = r.U64();
  d.to_gid = r.U64();
  d.properties = r.Str();
  const auto label_count = r.U32();
  d.labels.reserve(label_count);
  for (uint32_t i = 0; i < label_count; ++i) d.labels.push_back(r.U32());
  d.txn_start_timestamp = r.U64();
  d.ledger_time_ns = r.U64();
  d.query = r.Str();
  return d;
}

}  // namespace

VersionDeltaStore::VersionDeltaStore(std::filesystem::path path)
    : store_lock_(VersioningStoreMutex()), store_(std::make_unique<kvstore::KVStore>(std::move(path))) {
  // Resume the sequence counter past any previously stored deltas.
  next_seq_ = store_->Size(std::string{kDeltaPrefix});
}

void VersionDeltaStore::Append(const OverlayDelta &delta) {
  if (!store_->Put(DeltaKey(next_seq_), EncodeDelta(delta))) {
    throw kvstore::KVStoreError("Failed to persist a version overlay delta.");
  }
  ++next_seq_;
}

std::vector<OverlayDelta> VersionDeltaStore::ReadAll() const {
  std::vector<OverlayDelta> deltas;
  const std::string prefix{kDeltaPrefix};
  for (auto it = store_->begin(prefix); it != store_->end(prefix); ++it) {
    deltas.push_back(DecodeDelta(it->second));
  }
  return deltas;
}

void VersionDeltaStore::ReplaceAll(const std::vector<OverlayDelta> &deltas) {
  store_->DeletePrefix(std::string{kDeltaPrefix});
  next_seq_ = 0;
  for (const auto &delta : deltas) {
    Append(delta);
  }
}

void VersionDeltaStore::Drop(const std::filesystem::path &path) {
  // Serialize against any concurrent open of this store (the directory must not be open while removed).
  std::lock_guard<std::recursive_mutex> guard(VersioningStoreMutex());
  std::error_code ec;
  std::filesystem::remove_all(path, ec);
  if (ec) {
    throw kvstore::KVStoreError("Failed to drop version store '{}': {}", path.string(), ec.message());
  }
}

}  // namespace memgraph::storage
