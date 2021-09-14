#include <gtest/gtest.h>

#include "utils/license.hpp"

TEST(License, Encode) {
  const auto result = utils::license::Encode({"Memgraph", 9999999999999});
  spdlog::critical(result);

  auto maybe_license = utils::license::Decode(result);
  spdlog::critical(maybe_license->organization_name);
  spdlog::critical(maybe_license->valid_until);
}
