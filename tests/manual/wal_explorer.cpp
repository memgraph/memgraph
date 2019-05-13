#include <filesystem>
#include <iostream>
#include <limits>

#include <gflags/gflags.h>
#include <glog/logging.h>

#include "durability/hashed_file_reader.hpp"
#include "durability/single_node/recovery.hpp"
#include "durability/single_node/state_delta.hpp"
#include "durability/single_node/version.hpp"
#include "durability/single_node/wal.hpp"
#include "transactions/type.hpp"

DEFINE_string(wal_file, "", "WAL file location");

using communication::bolt::Value;
namespace fs = std::filesystem;

std::string StateDeltaTypeToString(database::StateDelta::Type type) {
  switch (type) {
    case database::StateDelta::Type::TRANSACTION_BEGIN:
      return "TRANSACTION_BEGIN";
    case database::StateDelta::Type::TRANSACTION_COMMIT:
      return "TRANSACTION_COMMIT";
    case database::StateDelta::Type::TRANSACTION_ABORT:
      return "TRANSACTION_ABORT";
    case database::StateDelta::Type::CREATE_VERTEX:
      return "CREATE_VERTEX";
    case database::StateDelta::Type::CREATE_EDGE:
      return "CREATE_EDGE";
    case database::StateDelta::Type::SET_PROPERTY_VERTEX:
      return "SET_PROPERTY_VERTEX";
    case database::StateDelta::Type::SET_PROPERTY_EDGE:
      return "SET_PROPERTY_EDGE";
    case database::StateDelta::Type::ADD_LABEL:
      return "ADD_LABEL";
    case database::StateDelta::Type::REMOVE_LABEL:
      return "REMOVE_LABEL";
    case database::StateDelta::Type::REMOVE_VERTEX:
      return "REMOVE_VERTEX";
    case database::StateDelta::Type::REMOVE_EDGE:
      return "REMOVE_EDGE";
    case database::StateDelta::Type::BUILD_INDEX:
      return "BUILD_INDEX";
    case database::StateDelta::Type::DROP_INDEX:
      return "DROP_INDEX";
    case database::StateDelta::Type::BUILD_UNIQUE_CONSTRAINT:
      return "BUILD_UNIQUE_CONSTRAINT";
    case database::StateDelta::Type::DROP_UNIQUE_CONSTRAINT:
      return "DROP_UNIQUE_CONSTRAINT";
  }
}

int main(int argc, char *argv[]) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  google::InitGoogleLogging(argv[0]);

  fs::path wal_path(FLAGS_wal_file);
  CHECK(fs::exists(wal_path)) << "File doesn't exist!";

  HashedFileReader wal_reader;
  CHECK(wal_reader.Open(wal_path)) << "Couldn't open wal file!";

  communication::bolt::Decoder<HashedFileReader> decoder(wal_reader);

  auto magic_number = durability::kWalMagic;
  wal_reader.Read(magic_number.data(), magic_number.size());
  CHECK(magic_number == durability::kWalMagic) << "Wal magic number mismatch";

  communication::bolt::Value dv;
  decoder.ReadValue(&dv);
  CHECK(dv.ValueInt() == durability::kVersion) << "Wal version mismatch";

  tx::TransactionId max_observed_tx_id{0};
  tx::TransactionId min_observed_tx_id{std::numeric_limits<uint64_t>::max()};

  std::vector<std::string> wal_entries;

  while (true) {
    auto delta = database::StateDelta::Decode(wal_reader, decoder);
    if (!delta) break;

    max_observed_tx_id = std::max(max_observed_tx_id, delta->transaction_id);
    min_observed_tx_id = std::min(min_observed_tx_id, delta->transaction_id);
    LOG(INFO) << "Found tx: " << delta->transaction_id << " "
              << StateDeltaTypeToString(delta->type);
  }

  LOG(INFO) << "Min tx " << min_observed_tx_id;
  LOG(INFO) << "Max tx " << max_observed_tx_id;

  return 0;
}
