// Copyright 2023 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include "storage/v2/replication/rpc.hpp"

namespace memgraph {

namespace storage {

namespace replication {

void AppendDeltasReq::Save(const AppendDeltasReq &self, memgraph::slk::Builder *builder) {
  memgraph::slk::Save(self, builder);
}
void AppendDeltasReq::Load(AppendDeltasReq *self, memgraph::slk::Reader *reader) { memgraph::slk::Load(self, reader); }
void AppendDeltasRes::Save(const AppendDeltasRes &self, memgraph::slk::Builder *builder) {
  memgraph::slk::Save(self, builder);
}
void AppendDeltasRes::Load(AppendDeltasRes *self, memgraph::slk::Reader *reader) { memgraph::slk::Load(self, reader); }
void HeartbeatReq::Save(const HeartbeatReq &self, memgraph::slk::Builder *builder) {
  memgraph::slk::Save(self, builder);
}
void HeartbeatReq::Load(HeartbeatReq *self, memgraph::slk::Reader *reader) { memgraph::slk::Load(self, reader); }
void HeartbeatRes::Save(const HeartbeatRes &self, memgraph::slk::Builder *builder) {
  memgraph::slk::Save(self, builder);
}
void HeartbeatRes::Load(HeartbeatRes *self, memgraph::slk::Reader *reader) { memgraph::slk::Load(self, reader); }
void FrequentHeartbeatReq::Save(const FrequentHeartbeatReq &self, memgraph::slk::Builder *builder) {
  memgraph::slk::Save(self, builder);
}
void FrequentHeartbeatReq::Load(FrequentHeartbeatReq *self, memgraph::slk::Reader *reader) {
  memgraph::slk::Load(self, reader);
}
void FrequentHeartbeatRes::Save(const FrequentHeartbeatRes &self, memgraph::slk::Builder *builder) {
  memgraph::slk::Save(self, builder);
}
void FrequentHeartbeatRes::Load(FrequentHeartbeatRes *self, memgraph::slk::Reader *reader) {
  memgraph::slk::Load(self, reader);
}
void SnapshotReq::Save(const SnapshotReq &self, memgraph::slk::Builder *builder) { memgraph::slk::Save(self, builder); }
void SnapshotReq::Load(SnapshotReq *self, memgraph::slk::Reader *reader) { memgraph::slk::Load(self, reader); }
void SnapshotRes::Save(const SnapshotRes &self, memgraph::slk::Builder *builder) { memgraph::slk::Save(self, builder); }
void SnapshotRes::Load(SnapshotRes *self, memgraph::slk::Reader *reader) { memgraph::slk::Load(self, reader); }
void WalFilesReq::Save(const WalFilesReq &self, memgraph::slk::Builder *builder) { memgraph::slk::Save(self, builder); }
void WalFilesReq::Load(WalFilesReq *self, memgraph::slk::Reader *reader) { memgraph::slk::Load(self, reader); }
void WalFilesRes::Save(const WalFilesRes &self, memgraph::slk::Builder *builder) { memgraph::slk::Save(self, builder); }
void WalFilesRes::Load(WalFilesRes *self, memgraph::slk::Reader *reader) { memgraph::slk::Load(self, reader); }
void CurrentWalReq::Save(const CurrentWalReq &self, memgraph::slk::Builder *builder) {
  memgraph::slk::Save(self, builder);
}
void CurrentWalReq::Load(CurrentWalReq *self, memgraph::slk::Reader *reader) { memgraph::slk::Load(self, reader); }
void CurrentWalRes::Save(const CurrentWalRes &self, memgraph::slk::Builder *builder) {
  memgraph::slk::Save(self, builder);
}
void CurrentWalRes::Load(CurrentWalRes *self, memgraph::slk::Reader *reader) { memgraph::slk::Load(self, reader); }
void TimestampReq::Save(const TimestampReq &self, memgraph::slk::Builder *builder) {
  memgraph::slk::Save(self, builder);
}
void TimestampReq::Load(TimestampReq *self, memgraph::slk::Reader *reader) { memgraph::slk::Load(self, reader); }
void TimestampRes::Save(const TimestampRes &self, memgraph::slk::Builder *builder) {
  memgraph::slk::Save(self, builder);
}
void TimestampRes::Load(TimestampRes *self, memgraph::slk::Reader *reader) { memgraph::slk::Load(self, reader); }

}  // namespace replication
}  // namespace storage
}  // namespace memgraph
const memgraph::utils::TypeInfo memgraph::storage::replication::AppendDeltasReq::kType{0x2D0CAB0B9D315D3CULL,
                                                                                       "AppendDeltasReq", nullptr};

const memgraph::utils::TypeInfo memgraph::storage::replication::AppendDeltasRes::kType{0x2D0CAD0B9D3160A2ULL,
                                                                                       "AppendDeltasRes", nullptr};

const memgraph::utils::TypeInfo memgraph::storage::replication::HeartbeatReq::kType{0x228EF887DCA40C03ULL,
                                                                                    "HeartbeatReq", nullptr};

const memgraph::utils::TypeInfo memgraph::storage::replication::HeartbeatRes::kType{0x228EFA87DCA40F69ULL,
                                                                                    "HeartbeatRes", nullptr};

const memgraph::utils::TypeInfo memgraph::storage::replication::FrequentHeartbeatReq::kType{
    0x6DE0C7329F80CD41ULL, "FrequentHeartbeatReq", nullptr};

const memgraph::utils::TypeInfo memgraph::storage::replication::FrequentHeartbeatRes::kType{
    0x6DE0C5329F80C9DBULL, "FrequentHeartbeatRes", nullptr};

const memgraph::utils::TypeInfo memgraph::storage::replication::SnapshotReq::kType{0x37773E14410D9E5DULL, "SnapshotReq",
                                                                                   nullptr};

const memgraph::utils::TypeInfo memgraph::storage::replication::SnapshotRes::kType{0x37773C14410D9AF7ULL, "SnapshotRes",
                                                                                   nullptr};

const memgraph::utils::TypeInfo memgraph::storage::replication::WalFilesReq::kType{0xFF80AC7309545B88ULL, "WalFilesReq",
                                                                                   nullptr};

const memgraph::utils::TypeInfo memgraph::storage::replication::WalFilesRes::kType{0xFF80AE7309545EEEULL, "WalFilesRes",
                                                                                   nullptr};

const memgraph::utils::TypeInfo memgraph::storage::replication::CurrentWalReq::kType{0xFBFF598A6406C9C8ULL,
                                                                                     "CurrentWalReq", nullptr};

const memgraph::utils::TypeInfo memgraph::storage::replication::CurrentWalRes::kType{0xFBFF5B8A6406CD2EULL,
                                                                                     "CurrentWalRes", nullptr};

const memgraph::utils::TypeInfo memgraph::storage::replication::TimestampReq::kType{0x3629662224F9276BULL,
                                                                                    "TimestampReq", nullptr};

const memgraph::utils::TypeInfo memgraph::storage::replication::TimestampRes::kType{0x3629682224F92AD1ULL,
                                                                                    "TimestampRes", nullptr};

// Autogenerated SLK serialization code
namespace memgraph::slk {

// Serialize code for TimestampRes

void Save(const memgraph::storage::replication::TimestampRes &self, memgraph::slk::Builder *builder) {
  memgraph::slk::Save(self.success, builder);
  memgraph::slk::Save(self.current_commit_timestamp, builder);
}

void Load(memgraph::storage::replication::TimestampRes *self, memgraph::slk::Reader *reader) {
  memgraph::slk::Load(&self->success, reader);
  memgraph::slk::Load(&self->current_commit_timestamp, reader);
}

// Serialize code for TimestampReq

void Save(const memgraph::storage::replication::TimestampReq &self, memgraph::slk::Builder *builder) {}

void Load(memgraph::storage::replication::TimestampReq *self, memgraph::slk::Reader *reader) {}

// Serialize code for CurrentWalRes

void Save(const memgraph::storage::replication::CurrentWalRes &self, memgraph::slk::Builder *builder) {
  memgraph::slk::Save(self.success, builder);
  memgraph::slk::Save(self.current_commit_timestamp, builder);
}

void Load(memgraph::storage::replication::CurrentWalRes *self, memgraph::slk::Reader *reader) {
  memgraph::slk::Load(&self->success, reader);
  memgraph::slk::Load(&self->current_commit_timestamp, reader);
}

// Serialize code for CurrentWalReq

void Save(const memgraph::storage::replication::CurrentWalReq &self, memgraph::slk::Builder *builder) {}

void Load(memgraph::storage::replication::CurrentWalReq *self, memgraph::slk::Reader *reader) {}

// Serialize code for WalFilesRes

void Save(const memgraph::storage::replication::WalFilesRes &self, memgraph::slk::Builder *builder) {
  memgraph::slk::Save(self.success, builder);
  memgraph::slk::Save(self.current_commit_timestamp, builder);
}

void Load(memgraph::storage::replication::WalFilesRes *self, memgraph::slk::Reader *reader) {
  memgraph::slk::Load(&self->success, reader);
  memgraph::slk::Load(&self->current_commit_timestamp, reader);
}

// Serialize code for WalFilesReq

void Save(const memgraph::storage::replication::WalFilesReq &self, memgraph::slk::Builder *builder) {
  memgraph::slk::Save(self.file_number, builder);
}

void Load(memgraph::storage::replication::WalFilesReq *self, memgraph::slk::Reader *reader) {
  memgraph::slk::Load(&self->file_number, reader);
}

// Serialize code for SnapshotRes

void Save(const memgraph::storage::replication::SnapshotRes &self, memgraph::slk::Builder *builder) {
  memgraph::slk::Save(self.success, builder);
  memgraph::slk::Save(self.current_commit_timestamp, builder);
}

void Load(memgraph::storage::replication::SnapshotRes *self, memgraph::slk::Reader *reader) {
  memgraph::slk::Load(&self->success, reader);
  memgraph::slk::Load(&self->current_commit_timestamp, reader);
}

// Serialize code for SnapshotReq

void Save(const memgraph::storage::replication::SnapshotReq &self, memgraph::slk::Builder *builder) {}

void Load(memgraph::storage::replication::SnapshotReq *self, memgraph::slk::Reader *reader) {}

// Serialize code for FrequentHeartbeatRes

void Save(const memgraph::storage::replication::FrequentHeartbeatRes &self, memgraph::slk::Builder *builder) {
  memgraph::slk::Save(self.success, builder);
}

void Load(memgraph::storage::replication::FrequentHeartbeatRes *self, memgraph::slk::Reader *reader) {
  memgraph::slk::Load(&self->success, reader);
}

// Serialize code for FrequentHeartbeatReq

void Save(const memgraph::storage::replication::FrequentHeartbeatReq &self, memgraph::slk::Builder *builder) {}

void Load(memgraph::storage::replication::FrequentHeartbeatReq *self, memgraph::slk::Reader *reader) {}

// Serialize code for HeartbeatRes

void Save(const memgraph::storage::replication::HeartbeatRes &self, memgraph::slk::Builder *builder) {
  memgraph::slk::Save(self.success, builder);
  memgraph::slk::Save(self.current_commit_timestamp, builder);
  memgraph::slk::Save(self.epoch_id, builder);
}

void Load(memgraph::storage::replication::HeartbeatRes *self, memgraph::slk::Reader *reader) {
  memgraph::slk::Load(&self->success, reader);
  memgraph::slk::Load(&self->current_commit_timestamp, reader);
  memgraph::slk::Load(&self->epoch_id, reader);
}

// Serialize code for HeartbeatReq

void Save(const memgraph::storage::replication::HeartbeatReq &self, memgraph::slk::Builder *builder) {
  memgraph::slk::Save(self.main_commit_timestamp, builder);
  memgraph::slk::Save(self.epoch_id, builder);
}

void Load(memgraph::storage::replication::HeartbeatReq *self, memgraph::slk::Reader *reader) {
  memgraph::slk::Load(&self->main_commit_timestamp, reader);
  memgraph::slk::Load(&self->epoch_id, reader);
}

// Serialize code for AppendDeltasRes

void Save(const memgraph::storage::replication::AppendDeltasRes &self, memgraph::slk::Builder *builder) {
  memgraph::slk::Save(self.success, builder);
  memgraph::slk::Save(self.current_commit_timestamp, builder);
}

void Load(memgraph::storage::replication::AppendDeltasRes *self, memgraph::slk::Reader *reader) {
  memgraph::slk::Load(&self->success, reader);
  memgraph::slk::Load(&self->current_commit_timestamp, reader);
}

// Serialize code for AppendDeltasReq

void Save(const memgraph::storage::replication::AppendDeltasReq &self, memgraph::slk::Builder *builder) {
  memgraph::slk::Save(self.previous_commit_timestamp, builder);
  memgraph::slk::Save(self.seq_num, builder);
}

void Load(memgraph::storage::replication::AppendDeltasReq *self, memgraph::slk::Reader *reader) {
  memgraph::slk::Load(&self->previous_commit_timestamp, reader);
  memgraph::slk::Load(&self->seq_num, reader);
}

}  // namespace memgraph::slk
