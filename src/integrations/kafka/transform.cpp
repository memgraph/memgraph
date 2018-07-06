#include "integrations/kafka/transform.hpp"

#include "integrations/kafka/exceptions.hpp"
#include "requests/requests.hpp"

namespace integrations {
namespace kafka {

Transform::Transform(const std::string &transform_script_uri,
                     const std::string &transform_script_path)
    : transform_script_path_(transform_script_path) {
  if (!requests::CreateAndDownloadFile(transform_script_uri,
                                       transform_script_path)) {
    throw TransformScriptDownloadException(transform_script_uri);
  }
}

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
