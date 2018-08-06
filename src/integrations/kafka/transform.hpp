#pragma once

#include <experimental/filesystem>
#include <map>
#include <string>

#include "rdkafkacpp.h"

#include "communication/bolt/v1/value.hpp"

namespace integrations::kafka {

struct TargetArguments {
  std::experimental::filesystem::path transform_script_path;
  int pipe_to_python{-1};
  int pipe_from_python{-1};
};

class Transform final {
 private:
  const int kStackSizeBytes = 262144;

 public:
  explicit Transform(const std::string &transform_script_path);

  bool Start();

  void Apply(const std::vector<std::unique_ptr<RdKafka::Message>> &batch,
             std::function<void(
                 const std::string &,
                 const std::map<std::string, communication::bolt::Value> &)>
                 query_function);

  ~Transform();

 private:
  std::string transform_script_path_;
  pid_t pid_{-1};
  int status_{0};
  // The stack used for the `clone` system call must be heap allocated.
  std::unique_ptr<uint8_t[]> stack_{new uint8_t[kStackSizeBytes]};
  // The target arguments passed to the new process must be heap allocated.
  std::unique_ptr<TargetArguments> target_arguments_{new TargetArguments()};
  int pipe_to_python_[2] = {-1, -1};
  int pipe_from_python_[2] = {-1, -1};
};

}  // namespace integrations::kafka
