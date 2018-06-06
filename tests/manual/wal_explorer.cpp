#include <experimental/filesystem>
#include <iostream>
#include <limits>

#include <gflags/gflags.h>
#include <glog/logging.h>

#include "database/state_delta.hpp"
#include "durability/hashed_file_reader.hpp"
#include "durability/recovery.hpp"
#include "durability/wal.hpp"
#include "transactions/type.hpp"

DEFINE_string(wal_file, "", "WAL file location");

using communication::bolt::Value;
namespace fs = std::experimental::filesystem;

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
    case database::StateDelta::Type::ADD_OUT_EDGE:
      return "ADD_OUT_EDGE";
    case database::StateDelta::Type::REMOVE_OUT_EDGE:
      return "REMOVE_OUT_EDGE";
    case database::StateDelta::Type::ADD_IN_EDGE:
      return "ADD_IN_EDGE";
    case database::StateDelta::Type::REMOVE_IN_EDGE:
      return "REMOVE_IN_EDGE";
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
