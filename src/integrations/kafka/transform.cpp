#include "integrations/kafka/transform.hpp"

namespace integrations {
namespace kafka {

Transform::Transform(const std::string &transform_script_path)
    : transform_script_path_(transform_script_path) {}

std::vector<std::string> Transform::Apply(
    const std::vector<std::unique_ptr<RdKafka::Message>> &batch) {
  // TODO (msantl): dummy transform, do the actual transform later @mferencevic
  std::vector<std::string> transformed_batch;
  transformed_batch.reserve(batch.size());
  for (auto &record : batch) {
    transformed_batch.push_back(reinterpret_cast<char *>(record->payload()));
  }

  return transformed_batch;
}

}  // namespace kafka
}  // namespace integrations
