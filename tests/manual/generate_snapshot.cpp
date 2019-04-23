#include <algorithm>
#include <fstream>
#include <memory>
#include <optional>
#include <random>
#include <unordered_map>
#include <vector>

#include <fmt/format.h>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <json/json.hpp>

#include "communication/bolt/v1/encoder/base_encoder.hpp"
#include "durability/distributed/version.hpp"
#include "durability/hashed_file_writer.hpp"
#include "transactions/type.hpp"
#include "utils/string.hpp"
#include "utils/timer.hpp"

#include "snapshot_generation/graph_state.hpp"
#include "snapshot_generation/snapshot_writer.hpp"

DEFINE_int32(num_workers, 1, "number of workers");
DEFINE_string(out, "tmp", "Destination for the created snapshot file");
DEFINE_string(config, "", "Path to config JSON file");

using namespace snapshot_generation;

/**
 * This file contains the program for generating a snapshot based on a JSON
 * definition. The JSON config has the following form:
 *
 * {
 *   "indexes" : ["Person.id", "Company.id"],
 *   "nodes" : [
 *     {
 *       "count_per_worker" : 10000,
 *       "labels" : ["Person"],
 *       "properties" : {
 *         "id" : { "type" : "counter", "param": "Person.id" },
 *         "name" : { "type" : "randstring", "param" :
 *            { "type": "randint", "param" : [10, 20]}},
 *         "is_happy" :  { "type" : "bernoulli", "param" : 0.2 }
 *       }
 *     },
 *     {
 *       "count_per_worker" : 200,
 *       "labels" : ["Company"],
 *       "properties" : {
 *         "id" : { "type" : "counter", "param": "Company.id" },
 *         "name" : { "type" : "randstring", "param" :
 *            { "type": "randint", "param" : [10, 20]}},
 *         "description" : { "type" : "optional", "param" :
 *            [0.2, { "type" : "randstring", "param": 1024 }]}
 *        }
 *     }
 *   ],
 *   "edges" : [
 *     {
 *       "kind": "unique",
 *       "from" : "Person",
 *       "to" : "Company",
 *       "type" : "WORKS_IN",
 *       "hop_probability": 0.05
 *     },
 *     {
 *       "kind": "random",
 *       "count" : 20,
 *       "from" : "Person",
 *       "to" : "Company",
 *       "type" : "LIKES",
 *       "hop_probability": 0.1
 *     }
 *   ]
 * }
 */

int main(int argc, char **argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  google::InitGoogleLogging(argv[0]);

  // Read the config JSON
  nlohmann::json config;
  {
    std::ifstream config_file(FLAGS_config);
    config_file >> config;
  }

  GraphState state = BuildFromConfig(FLAGS_num_workers, config);
  WriteToSnapshot(state, FLAGS_out);

  return 0;
}
