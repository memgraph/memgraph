// Copyright 2026 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include <gtest/gtest.h>

#include <filesystem>
#include <string>
#include <unordered_set>
#include <vector>

#include "storage/v2/durability/buffer_encoder.hpp"
#include "storage/v2/durability/marker.hpp"
#include "storage/v2/durability/serialization.hpp"
#include "storage/v2/pipeline_budget.hpp"
#include "storage/v2/property_value.hpp"
#include "storage/v2/temporal.hpp"
#include "utils/file.hpp"

using namespace memgraph::storage;
using namespace memgraph::storage::durability;

namespace {

uint32_t WriteAll(BaseEncoder &e) {
  e.ResetCrcAcc();
  e.WriteMarker(Marker::SECTION_DELTA);
  e.WriteUint(42);
  e.WriteBool(true);
  e.WriteDouble(3.5);
  e.WriteString("pipelined");
  e.WriteExternalPropertyValue(ExternalPropertyValue(
      std::vector<ExternalPropertyValue>{ExternalPropertyValue(int64_t{1}), ExternalPropertyValue("x")}));
  ExternalPropertyValue::map_t map;
  map.emplace("k", ExternalPropertyValue(2.0));
  e.WriteExternalPropertyValue(ExternalPropertyValue(std::move(map)));
  e.WriteExternalPropertyValue(ExternalPropertyValue(TemporalData{TemporalType::Date, 17}));
  return e.WriteCrc();
}

}  // namespace

TEST(BufferEncoder, MatchesFileEncoderBytesAndCrc) {
  auto const dir = std::filesystem::temp_directory_path() / "mg_buffer_encoder_test";
  std::filesystem::remove_all(dir);
  std::filesystem::create_directories(dir);
  Encoder<memgraph::utils::OutputFile> file_encoder;
  ASSERT_TRUE(file_encoder.Initialize(dir / "reference.bin", "TEST", 1));
  auto const header_size = file_encoder.GetPosition();
  PipelineBudget budget{1 << 20};
  BufferEncoder buffer_encoder{TxnAllocPolicy{&budget, 0}};
  auto const crc_file = WriteAll(file_encoder);
  auto const crc_buffer = WriteAll(buffer_encoder);
  file_encoder.Finalize();
  memgraph::utils::InputFile file;
  ASSERT_TRUE(file.Open(dir / "reference.bin"));
  std::vector<uint8_t> reference(file.GetSize() - header_size);
  ASSERT_TRUE(file.SetPosition(memgraph::utils::InputFile::Position::SET, header_size).has_value());
  ASSERT_TRUE(file.Read(reference.data(), reference.size()));
  auto const bytes = buffer_encoder.bytes();
  EXPECT_EQ(std::vector<uint8_t>(bytes.begin(), bytes.end()), reference);
  EXPECT_EQ(crc_buffer, crc_file);
  EXPECT_EQ(buffer_encoder.GetPosition(), reference.size());
  EXPECT_GE(buffer_encoder.charged_bytes(), reference.size());
  EXPECT_EQ(budget.InFlightBytes(), buffer_encoder.charged_bytes());
  std::filesystem::remove_all(dir);
}

TEST(BufferEncoder, BudgetIsChargedBeforeGrowthAndReleasedOnDestruction) {
  PipelineBudget budget{200};
  {
    BufferEncoder e{TxnAllocPolicy{&budget, 0}};
    e.WriteString(std::string(100, 'a'));  // fits
    EXPECT_THROW(e.WriteString(std::string(300, 'b')), PipelineBudgetExceeded);
    EXPECT_LE(budget.InFlightBytes(), 200);                // never charged beyond max
    EXPECT_EQ(budget.InFlightBytes(), e.charged_bytes());  // the failed growth released its charge
    EXPECT_GT(e.charged_bytes(), 0);
  }
  EXPECT_EQ(budget.InFlightBytes(), 0);
}

TEST(BufferEncoder, NullBudgetNeverChargesOrRefuses) {
  BufferEncoder e{TxnAllocPolicy{nullptr, 0}};
  e.WriteString(std::string(1 << 16, 'c'));
  EXPECT_EQ(e.charged_bytes(), 0);
  EXPECT_EQ(e.GetPosition(), (1 << 16) + 1 + sizeof(uint64_t));
}

TEST(BudgetAllocator, RefusalInjectionFiresOnceForTheSelectedTicketAndSite) {
  PipelineBudget budget{1 << 20};
  BudgetRefuse refuse;
  refuse.ticket = 7;
  refuse.site = BudgetRefuse::kEncoder;
  // A different ticket at the same site is untouched.
  {
    BufferEncoder other{TxnAllocPolicy{&budget, 8, &refuse, BudgetRefuse::kEncoder}};
    other.WriteUint(1);
    EXPECT_FALSE(refuse.fired.load());
  }
  // The same ticket at a different site is untouched.
  {
    BudgetVector<int> materializer{
        BudgetAllocator<int>{TxnAllocPolicy{&budget, 7, &refuse, BudgetRefuse::kMaterializer}}};
    materializer.push_back(1);
    EXPECT_FALSE(refuse.fired.load());
  }
  // The selected committer's first allocation at the site throws, later ones proceed.
  {
    BufferEncoder selected{TxnAllocPolicy{&budget, 7, &refuse, BudgetRefuse::kEncoder}};
    EXPECT_THROW(selected.WriteUint(1), PipelineBudgetExceeded);
    EXPECT_TRUE(refuse.fired.load());
    selected.WriteUint(1);
    EXPECT_EQ(selected.GetPosition(), 1 + sizeof(uint64_t));
  }
  EXPECT_EQ(budget.InFlightBytes(), 0);
  // The runtime-error mode is the generic fault.
  refuse.fired = false;
  refuse.mode = BudgetRefuse::kThrowRuntimeError;
  BufferEncoder faulted{TxnAllocPolicy{&budget, 7, &refuse, BudgetRefuse::kEncoder}};
  EXPECT_THROW(faulted.WriteUint(1), std::runtime_error);
}

TEST(BudgetAllocator, ReboundContainersChargeAndRelease) {
  PipelineBudget budget{1 << 20};
  {
    std::unordered_set<int, std::hash<int>, std::equal_to<>, BudgetAllocator<int>> set{
        BudgetAllocator<int>{TxnAllocPolicy{&budget, 0}}};
    for (int i = 0; i < 1000; ++i) set.insert(i);
    EXPECT_GT(budget.InFlightBytes(), 1000 * sizeof(int));
  }
  EXPECT_EQ(budget.InFlightBytes(), 0);
}
