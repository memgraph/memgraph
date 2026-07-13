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

#pragma once

#include <cstdint>

#include "storage/v2/durability/wal.hpp"
#include "storage/v2/inmemory/storagefwd.hpp"

// Graph Versioning v1 durability. Companion extraction to wal_delta_apply.{hpp,cpp} (slice S3b):
// that slice partitioned InMemoryReplicationHandlers::ReadAndApplyDeltasSingleTxn's single big
// Overloaded into (A) the per-delta DATA-plane ops (storage::ApplyWalDataDelta) and (B) transaction
// FRAMING, which stayed caller-side. This file extracts the remaining, non-data-plane arms -- every
// schema/index/constraint/enum/TTL/description-store WAL operation -- into storage::
// ApplyWalSchemaDelta, following the exact same pattern: pure extraction, no behavior change, and
// still caller-owns-framing (accessor resolution/access-type hint, WalTransactionStart/End,
// res_builder progress, 2PC caching all stay in ReadAndApplyDeltasSingleTxn).
//
// Together, ApplyWalDataDelta + ApplyWalSchemaDelta + caller-side framing cover every WalDeltaData
// alternative, so any caller that has already opened its own write accessor (replica-apply; and,
// as of the BUG-1 fix, recovery's windowed branch-replay too -- see wal_window_replay.cpp, which
// now dispatches every non-data-plane delta here instead of warn-and-skipping it) can replay a WAL
// transaction's full delta set without duplicating ~40 arms.
namespace memgraph::storage {

// Applies a single pure SCHEMA-plane WAL delta -- every index/stats/constraint/enum/point/vector/
// text-index/TTL/description-store WalDeltaData alternative EXCEPT the eight data-plane kinds
// ApplyWalDataDelta owns (WalVertex*/WalEdge*) and the two framing kinds (WalTransactionStart/End)
// -- to an already-open `accessor`.
//
// `accessor` must already have been resolved by the caller with the correct StorageAccessType hint
// for this particular delta kind (most schema ops need StorageAccessType::UNIQUE; the two IndexStats
// Set/Clear pairs use the default WRITE hint) -- that resolution is access-type-sensitive replica
// framing and stays entirely caller-side, exactly mirroring how ApplyWalDataDelta's callers resolve
// their accessor before invoking it.
//
// `current_delta_idx` is used only for trace-log messages, matching today's wording byte-for-byte.
//
// Throws utils::BasicException on every failure condition the original inline Overloaded arms threw
// on (failed index/constraint/enum create-or-drop, bad stats JSON, ...) -- same messages, same call
// sites relative to one another -- so a catching caller's behavior is unchanged.
//
// Every schema-plane arm here calls only PUBLIC ReplicationAccessor / InMemoryStorage members
// (CreateIndex, NameToLabel, name_id_mapper_, ...) -- unlike ApplyWalDataDelta's WalEdgeSetProperty
// arm, none of these need a caller-supplied FindEdgeFallback-style callback into replica-private
// state, so this function takes no extra collaborator parameters.
//
// `delta` must hold one of the schema-plane alternatives; any other WalDeltaData alternative (a
// data-plane op, or transaction framing) throws utils::BasicException -- callers are expected to
// dispatch those themselves and never reach this function with them.
//
// AccessorT is `storage::ReplicationAccessor` today (the sole caller: replica-apply), same as
// ApplyWalDataDelta -- a future window-replay accessor exposing the same public surface can reuse
// this core by adding its own overload (or templating this function) at that time.
void ApplyWalSchemaDelta(ReplicationAccessor *accessor, InMemoryStorage *storage, durability::WalDeltaData const &delta,
                         uint64_t current_delta_idx);

}  // namespace memgraph::storage
