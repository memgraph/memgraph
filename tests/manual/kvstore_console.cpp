#include <gflags/gflags.h>
#include <glog/logging.h>

#include "storage/kvstore.hpp"
#include "utils/string.hpp"

DEFINE_string(path, "", "Path to the storage directory.");

int main(int argc, char **argv) {
  gflags::SetVersionString("kvstore_console");
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  google::InitGoogleLogging(argv[0]);

  CHECK(FLAGS_path != "") << "Please specify a path to the KVStore!";

  storage::KVStore kvstore(std::experimental::filesystem::path{FLAGS_path});

  while (true) {
    std::string s;
    std::getline(std::cin, s);
    if (s == "") {
      break;
    }

    auto split = utils::Split(utils::Trim(s));
    if (split[0] == "list") {
      if (split.size() != 1) {
        std::cout << "`list' takes no arguments!" << std::endl;
      }
      for (auto it = kvstore.begin(); it != kvstore.end(); ++it) {
        std::cout << it->first << " --> " << it->second << std::endl;
      }
    } else if (split[0] == "get") {
      if (split.size() != 2) {
        std::cout << "`get' takes exactly one argument!" << std::endl;
      }
      auto item = kvstore.Get(split[1]);
      if (item != std::experimental::nullopt) {
        std::cout << split[1] << " --> " << *item << std::endl;
      } else {
        std::cout << "Key doesn't exist in the database!" << std::endl;
      }
    } else if (split[0] == "put") {
      if (split.size() != 3) {
        std::cout << "`put' takes exactly two arguments!" << std::endl;
      }
      if (kvstore.Put(split[1], split[2])) {
        std::cout << "Success." << std::endl;
      } else {
        std::cout << "Failure!" << std::endl;
      }
    } else if (split[0] == "delete") {
      if (split.size() != 2) {
        std::cout << "`delete' takes exactly one argument!" << std::endl;
      }
      if (kvstore.Delete(split[1])) {
        std::cout << "Success." << std::endl;
      } else {
        std::cout << "Failure!" << std::endl;
      }
    }
    std::cout << std::endl;
  }

  return 0;
}
