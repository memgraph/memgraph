#pragma once

#include "durability/distributed/recovery.hpp"
#include "slk/serialization.hpp"

namespace slk {

inline void Save(const durability::RecoveryInfo &info, slk::Builder *builder) {
  slk::Save(info.durability_version, builder);
  slk::Save(info.snapshot_tx_id, builder);
  slk::Save(info.wal_recovered, builder);
}

inline void Load(durability::RecoveryInfo *info, slk::Reader *reader) {
  slk::Load(&info->durability_version, reader);
  slk::Load(&info->snapshot_tx_id, reader);
  slk::Load(&info->wal_recovered, reader);
}

inline void Save(const durability::RecoveryData &data, slk::Builder *builder) {
  slk::Save(data.snapshooter_tx_id, builder);
  slk::Save(data.wal_tx_to_recover, builder);
  slk::Save(data.snapshooter_tx_snapshot, builder);
  slk::Save(data.indexes, builder);
}

inline void Load(durability::RecoveryData *data, slk::Reader *reader) {
  slk::Load(&data->snapshooter_tx_id, reader);
  slk::Load(&data->wal_tx_to_recover, reader);
  slk::Load(&data->snapshooter_tx_snapshot, reader);
  slk::Load(&data->indexes, reader);
}

}  // namespace slk
