#pragma once

#include <memory>
#include <string>
#include <vector>

#include "rdkafkacpp.h"

namespace integrations {
namespace kafka {

class Transform final {
 public:
  Transform(const std::string &transform_script_path);

  std::vector<std::string> Apply(
      const std::vector<std::unique_ptr<RdKafka::Message>> &batch);

  auto transform_script_path() const { return transform_script_path_; }

 private:
  std::string transform_script_path_;
};

}  // namespace kafka
}  // namespace integrations
