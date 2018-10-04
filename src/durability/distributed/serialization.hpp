#pragma once

#include "durability/distributed/recovery.hpp"
#include "durability/distributed/serialization.capnp.h"
#include "utils/serialization.hpp"

namespace durability {

inline void Save(const RecoveryInfo &info,
                 capnp::RecoveryInfo::Builder *builder) {
  builder->setDurabilityVersion(info.durability_version);
  builder->setSnapshotTxId(info.snapshot_tx_id);
  auto list_builder = builder->initWalRecovered(info.wal_recovered.size());
  utils::SaveVector(info.wal_recovered, &list_builder);
}

inline void Load(RecoveryInfo *info,
                 const capnp::RecoveryInfo::Reader &reader) {
  info->durability_version = reader.getDurabilityVersion();
  info->snapshot_tx_id = reader.getSnapshotTxId();
  auto list_reader = reader.getWalRecovered();
  utils::LoadVector(&info->wal_recovered, list_reader);
}

inline void Save(const RecoveryData &data,
                 capnp::RecoveryData::Builder *builder) {
  builder->setSnapshooterTxId(data.snapshooter_tx_id);
  {
    auto list_builder =
        builder->initWalTxToRecover(data.wal_tx_to_recover.size());
    utils::SaveVector(data.wal_tx_to_recover, &list_builder);
  }
  {
    auto list_builder =
        builder->initSnapshooterTxSnapshot(data.snapshooter_tx_snapshot.size());
    utils::SaveVector(data.snapshooter_tx_snapshot, &list_builder);
  }
  {
    auto list_builder = builder->initIndexes(data.indexes.size());
    utils::SaveVector<utils::capnp::Pair<::capnp::Text, ::capnp::Text>,
                      std::pair<std::string, std::string>>(
        data.indexes, &list_builder, [](auto *builder, const auto value) {
          builder->setFirst(value.first);
          builder->setSecond(value.second);
        });
  }
}

inline void Load(RecoveryData *data,
                 const capnp::RecoveryData::Reader &reader) {
  data->snapshooter_tx_id = reader.getSnapshooterTxId();
  {
    auto list_reader = reader.getWalTxToRecover();
    utils::LoadVector(&data->wal_tx_to_recover, list_reader);
  }
  {
    auto list_reader = reader.getSnapshooterTxSnapshot();
    utils::LoadVector(&data->snapshooter_tx_snapshot, list_reader);
  }
  {
    auto list_reader = reader.getIndexes();
    utils::LoadVector<utils::capnp::Pair<::capnp::Text, ::capnp::Text>,
                      std::pair<std::string, std::string>>(
        &data->indexes, list_reader, [](const auto &reader) {
          return std::make_pair(reader.getFirst(), reader.getSecond());
        });
  }
}

}  // namespace durability
