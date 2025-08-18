// Copyright 2025 Memgraph Ltd.
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
#include <chrono>
#include <filesystem>
#include <thread>
#include <tuple>
#include <type_traits>

#include "dbms/database.hpp"
#include "disk_test_utils.hpp"
#include "flags/run_time_configurable.hpp"
#include "query/interpreter_context.hpp"
#include "storage/v2/disk/storage.hpp"
#include "storage/v2/inmemory/storage.hpp"
#include "storage/v2/storage_mode.hpp"
#include "storage/v2/ttl.hpp"
#include "tests/test_commit_args_helper.hpp"
#include "utils/on_scope_exit.hpp"
#include "utils/settings.hpp"

namespace {
std::filesystem::path GetCleanDataDirectory() {
  const auto path = std::filesystem::temp_directory_path() / "ttl";
  std::filesystem::remove_all(path);
  return path;
}

template <typename T>
int GetPart(auto &current) {
  const int whole_part = std::chrono::duration_cast<T>(current).count();
  current -= T{whole_part};
  return whole_part;
}
}  // namespace

template <typename TestType>
class TTLFixture : public ::testing::Test {
  using StorageType = typename std::tuple_element<0, TestType>::type;
  using PropOnEdge = typename std::tuple_element<1, TestType>::type;

 public:
  bool HasPropOnEdge() const { return PropOnEdge::value; }

  bool RunEdgeTTL() const {
    return db_->config().salient.items.properties_on_edges &&
           db_->GetStorageMode() != memgraph::storage::StorageMode::ON_DISK_TRANSACTIONAL;
  }

 protected:
  const std::string testSuite = "ttl";
  std::filesystem::path data_directory_{GetCleanDataDirectory()};
  memgraph::storage::Config config{
      [&]() {
        memgraph::storage::Config config{};
        config.durability.storage_directory = data_directory_;
        config.disk.main_storage_directory = config.durability.storage_directory / "disk";
        config.salient.items.properties_on_edges = PropOnEdge::value;
        if constexpr (std::is_same_v<StorageType, memgraph::storage::DiskStorage>) {
          config.disk = disk_test_utils::GenerateOnDiskConfig(testSuite).disk;
          config.force_on_disk = true;
        }
        return config;
      }()  // iile
  };

  memgraph::utils::Synchronized<memgraph::replication::ReplicationState, memgraph::utils::RWSpinLock> repl_state{
      memgraph::storage::ReplicationStateRootPath(config)};
  memgraph::utils::Gatekeeper<memgraph::dbms::Database> db_gk{config, repl_state};
  memgraph::dbms::DatabaseAccess db_{
      [&]() {
        auto db_acc_opt = db_gk.access();
        MG_ASSERT(db_acc_opt, "Failed to access db");
        auto &db_acc = *db_acc_opt;
        MG_ASSERT(db_acc->GetStorageMode() == (std::is_same_v<StorageType, memgraph::storage::DiskStorage>
                                                   ? memgraph::storage::StorageMode::ON_DISK_TRANSACTIONAL
                                                   : memgraph::storage::StorageMode::IN_MEMORY_TRANSACTIONAL),
                  "Wrong storage mode!");
        return db_acc;
      }()  // iile
  };
  memgraph::system::System system_state;
  memgraph::query::AllowEverythingAuthChecker auth_checker;
  memgraph::query::InterpreterContext interpreter_context_{memgraph::query::InterpreterConfig{},
                                                           nullptr,
                                                           repl_state,
                                                           system_state,
#ifdef MG_ENTERPRISE
                                                           std::nullopt,
                                                           nullptr,
#endif
                                                           nullptr,
                                                           &auth_checker};

  memgraph::storage::ttl::TTL *ttl_{&db_->ttl()};

  void SetUp() override {
    // Storage now has a safe default database protector factory
    // No additional setup needed for tests
    if constexpr (std::is_same_v<StorageType, memgraph::storage::InMemoryStorage>) {
      ttl_->SetUserCheck([]() -> bool { return true; });
    }
  }

  // Helper to ensure TTL indices are created and ready - matches TTL system expectations
  void EnsureTTLIndicesReady() {
    // First determine if edge TTL should run based on the TTL system's logic
    bool should_run_edge_ttl = this->RunEdgeTTL();

    auto ttl_label = db_->storage()->NameToLabel("TTL");
    auto ttl_property = db_->storage()->NameToProperty("ttl");
    std::vector<memgraph::storage::PropertyPath> ttl_property_path = {ttl_property};

    // Create indices synchronously using UniqueAccess (blocking) - exactly like TTL system does
    {
      auto unique_acc = db_->storage()->UniqueAccess();

      // Always create label+property index (TTL system always needs this)
      if (!unique_acc->LabelPropertyIndexExists(ttl_label, ttl_property_path)) {
        auto result = unique_acc->CreateIndex(ttl_label, ttl_property_path);
        ASSERT_FALSE(result.HasError()) << "Failed to create label+property index";
      }

      // Create edge property index only if needed (matches TTL condition exactly)
      if (should_run_edge_ttl && !unique_acc->EdgePropertyIndexExists(ttl_property)) {
        auto result = unique_acc->CreateGlobalEdgeIndex(ttl_property);
        ASSERT_FALSE(result.HasError()) << "Failed to create edge property index";
      }

      // Commit the index creation using the test helper
      auto commit_result = unique_acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs());
      ASSERT_FALSE(commit_result.HasError()) << "Failed to commit index creation";
    }

    // Wait for indices to be ready - matches TTL system's readiness check exactly
    for (int i = 0; i < 100; ++i) {  // Increased timeout to 10 seconds
      auto acc = db_->Access();
      bool label_prop_ready = acc->LabelPropertyIndexReady(ttl_label, ttl_property_path);
      bool edge_prop_ready = !should_run_edge_ttl || acc->EdgePropertyIndexReady(ttl_property);

      if (label_prop_ready && edge_prop_ready) {
        return;
      }

      std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    FAIL() << "TTL indices not ready after synchronous creation and 10 second wait";
  }

  void TearDown() override {
    db_->StopAllBackgroundTasks();
    if (std::is_same<StorageType, memgraph::storage::DiskStorage>::value) {
      disk_test_utils::RemoveRocksDbDirs(testSuite);
    }
    std::filesystem::remove_all(data_directory_);
  }
};

using TestTypes = ::testing::Types<std::tuple<memgraph::storage::InMemoryStorage, std::true_type>,
                                   std::tuple<memgraph::storage::InMemoryStorage, std::false_type>>;

// std::tuple<memgraph::storage::DiskStorage, std::true_type>,
// std::tuple<memgraph::storage::DiskStorage, std::false_type>>;
TYPED_TEST_SUITE(TTLFixture, TestTypes);

TYPED_TEST(TTLFixture, EnableTest) {
  const auto period = std::chrono::days(1);
  const auto start_time = std::chrono::system_clock::now();
  const bool should_run_edge_ttl = this->RunEdgeTTL();

  EXPECT_FALSE(this->ttl_->Enabled());
  EXPECT_THROW(this->ttl_->Configure(should_run_edge_ttl), memgraph::storage::ttl::TtlException);
  EXPECT_THROW(this->ttl_->SetInterval(period, start_time), memgraph::storage::ttl::TtlException);
  this->ttl_->Enable();
  EXPECT_TRUE(this->ttl_->Enabled());
  EXPECT_NO_THROW(this->ttl_->Configure(should_run_edge_ttl));
  EXPECT_NO_THROW(this->ttl_->SetInterval(period, start_time));
  this->ttl_->Resume();
  EXPECT_THROW(this->ttl_->Configure(should_run_edge_ttl), memgraph::storage::ttl::TtlException);
  this->ttl_->Pause();
  EXPECT_NO_THROW(this->ttl_->SetInterval(period, start_time));
  this->ttl_->Resume();
  this->ttl_->Disable();
  EXPECT_FALSE(this->ttl_->Enabled());
  EXPECT_THROW(this->ttl_->SetInterval(period, start_time), memgraph::storage::ttl::TtlException);
}

TYPED_TEST(TTLFixture, Periodic) {
  auto lbl = this->db_->storage()->NameToLabel("L");
  auto prop = this->db_->storage()->NameToProperty("prop");
  auto ttl_lbl = this->db_->storage()->NameToLabel("TTL");
  auto ttl_prop = this->db_->storage()->NameToProperty("ttl");
  auto now = std::chrono::system_clock::now();
  auto older = now - std::chrono::seconds(10);
  auto older_ts = std::chrono::duration_cast<std::chrono::microseconds>(older.time_since_epoch()).count();
  auto newer = now + std::chrono::seconds(3);
  auto newer_ts = std::chrono::duration_cast<std::chrono::microseconds>(newer.time_since_epoch()).count();
  {
    auto acc = this->db_->Access();
    auto v1 = acc->CreateVertex();  // No label no property
    auto v2 = acc->CreateVertex();  // A label a property
    auto v3 = acc->CreateVertex();  // TTL label no ttl property
    auto v4 = acc->CreateVertex();  // No TTL label, with ttl property
    auto v5 = acc->CreateVertex();  // TTL label and ttl property (older)
    auto v6 = acc->CreateVertex();  // TTL label and ttl property (newer)
    ASSERT_FALSE(v2.AddLabel(lbl).HasError());
    ASSERT_FALSE(v2.SetProperty(prop, memgraph::storage::PropertyValue(42)).HasError());
    ASSERT_FALSE(v3.AddLabel(ttl_lbl).HasError());
    ASSERT_FALSE(v4.SetProperty(ttl_prop, memgraph::storage::PropertyValue(42)).HasError());
    ASSERT_FALSE(v5.AddLabel(ttl_lbl).HasError());
    ASSERT_FALSE(v5.SetProperty(ttl_prop, memgraph::storage::PropertyValue(older_ts)).HasError());
    ASSERT_FALSE(v6.AddLabel(ttl_lbl).HasError());
    ASSERT_FALSE(v6.SetProperty(ttl_prop, memgraph::storage::PropertyValue(newer_ts)).HasError());
    ASSERT_FALSE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).HasError());
  }
  {
    auto acc = this->db_->Access();
    auto all_v = acc->Vertices(memgraph::storage::View::NEW);
    size_t size = 0;
    for (const auto v : acc->Vertices(memgraph::storage::View::NEW)) ++size;
    EXPECT_EQ(size, 6);
  }
  this->ttl_->Enable();
  this->ttl_->Configure(this->RunEdgeTTL());
  EXPECT_NO_THROW(this->ttl_->SetInterval(std::chrono::milliseconds(700)));
  this->ttl_->Resume();
  this->EnsureTTLIndicesReady();  // Ensure indices are created and ready
  std::this_thread::sleep_for(std::chrono::seconds(1));
  {
    auto acc = this->db_->Access();
    auto all_v = acc->Vertices(memgraph::storage::View::NEW);
    size_t size = 0;
    for (const auto v : acc->Vertices(memgraph::storage::View::NEW))
      if (v.IsVisible(memgraph::storage::View::NEW)) ++size;
    EXPECT_EQ(size, 5);
  }
  std::this_thread::sleep_for(std::chrono::seconds(3));
  {
    auto acc = this->db_->Access();
    size_t size = 0;
    for (const auto v : acc->Vertices(memgraph::storage::View::NEW))
      if (v.IsVisible(memgraph::storage::View::NEW)) ++size;
    EXPECT_EQ(size, 4);
  }
}

TYPED_TEST(TTLFixture, StartTime) {
  auto lbl = this->db_->storage()->NameToLabel("L");
  auto prop = this->db_->storage()->NameToProperty("prop");
  auto ttl_lbl = this->db_->storage()->NameToLabel("TTL");
  auto ttl_prop = this->db_->storage()->NameToProperty("ttl");
  auto now = std::chrono::system_clock::now();
  auto older = now - std::chrono::seconds(10);
  auto older_ts = std::chrono::duration_cast<std::chrono::microseconds>(older.time_since_epoch()).count();
  auto newer = now + std::chrono::seconds(4);
  auto newer_ts = std::chrono::duration_cast<std::chrono::microseconds>(newer.time_since_epoch()).count();
  {
    auto acc = this->db_->Access();
    auto v1 = acc->CreateVertex();  // No label no property
    auto v2 = acc->CreateVertex();  // A label a property
    auto v3 = acc->CreateVertex();  // TTL label no ttl property
    auto v4 = acc->CreateVertex();  // No TTL label, with ttl property
    auto v5 = acc->CreateVertex();  // TTL label and ttl property (older)
    auto v6 = acc->CreateVertex();  // TTL label and ttl property (newer)
    ASSERT_FALSE(v2.AddLabel(lbl).HasError());
    ASSERT_FALSE(v2.SetProperty(prop, memgraph::storage::PropertyValue(42)).HasError());
    ASSERT_FALSE(v3.AddLabel(ttl_lbl).HasError());
    ASSERT_FALSE(v4.SetProperty(ttl_prop, memgraph::storage::PropertyValue(42)).HasError());
    ASSERT_FALSE(v5.AddLabel(ttl_lbl).HasError());
    ASSERT_FALSE(v5.SetProperty(ttl_prop, memgraph::storage::PropertyValue(older_ts)).HasError());
    ASSERT_FALSE(v6.AddLabel(ttl_lbl).HasError());
    ASSERT_FALSE(v6.SetProperty(ttl_prop, memgraph::storage::PropertyValue(newer_ts)).HasError());
    ASSERT_FALSE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).HasError());
  }
  {
    auto acc = this->db_->Access();
    auto all_v = acc->Vertices(memgraph::storage::View::NEW);
    size_t size = 0;
    for (const auto v : acc->Vertices(memgraph::storage::View::NEW)) ++size;
    EXPECT_EQ(size, 6);
  }
  this->ttl_->Enable();
  this->ttl_->Configure(this->RunEdgeTTL());
  EXPECT_NO_THROW(this->ttl_->SetInterval(std::chrono::milliseconds(100),
                                          std::chrono::system_clock::now() + std::chrono::seconds(3)));
  this->ttl_->Resume();
  // Shouldn't start still
  for (int i = 0; i < 3; ++i) {
    std::this_thread::sleep_for(std::chrono::milliseconds(800));
    {
      auto acc = this->db_->Access();
      auto all_v = acc->Vertices(memgraph::storage::View::NEW);
      size_t size = 0;
      for (const auto v : acc->Vertices(memgraph::storage::View::NEW))
        if (v.IsVisible(memgraph::storage::View::NEW)) ++size;
      EXPECT_EQ(size, 6);
    }
  }
  std::this_thread::sleep_for(std::chrono::milliseconds(800));
  {
    auto acc = this->db_->Access();
    size_t size = 0;
    for (const auto v : acc->Vertices(memgraph::storage::View::NEW))
      if (v.IsVisible(memgraph::storage::View::NEW)) ++size;
    EXPECT_EQ(size, 5);
  }
  std::this_thread::sleep_for(std::chrono::seconds(1));
  {
    auto acc = this->db_->Access();
    size_t size = 0;
    for (const auto v : acc->Vertices(memgraph::storage::View::NEW))
      if (v.IsVisible(memgraph::storage::View::NEW)) ++size;
    EXPECT_EQ(size, 4);
  }
}

TYPED_TEST(TTLFixture, Edge) {
  auto lbl = this->db_->storage()->NameToLabel("L");
  auto prop = this->db_->storage()->NameToProperty("prop");
  auto ttl_lbl = this->db_->storage()->NameToLabel("TTL");
  auto ttl_prop = this->db_->storage()->NameToProperty("ttl");
  auto et1 = this->db_->storage()->NameToEdgeType("t1");
  auto et2 = this->db_->storage()->NameToEdgeType("t2");
  auto now = std::chrono::system_clock::now();
  auto older = now - std::chrono::seconds(10);
  auto older_ts = std::chrono::duration_cast<std::chrono::microseconds>(older.time_since_epoch()).count();
  auto newer = now + std::chrono::seconds(3);
  auto newer_ts = std::chrono::duration_cast<std::chrono::microseconds>(newer.time_since_epoch()).count();
  {
    auto acc = this->db_->Access();
    auto v1 = acc->CreateVertex();  // No label no property
    auto v2 = acc->CreateVertex();  // A label a property
    auto v3 = acc->CreateVertex();  // TTL label no ttl property
    auto v4 = acc->CreateVertex();  // No TTL label, with ttl property
    auto v5 = acc->CreateVertex();  // TTL label and ttl property (older)
    auto v6 = acc->CreateVertex();  // TTL label and ttl property (newer)
    ASSERT_FALSE(v2.AddLabel(lbl).HasError());
    ASSERT_FALSE(v2.SetProperty(prop, memgraph::storage::PropertyValue(42)).HasError());
    ASSERT_FALSE(v3.AddLabel(ttl_lbl).HasError());
    ASSERT_FALSE(v4.SetProperty(ttl_prop, memgraph::storage::PropertyValue(42)).HasError());
    ASSERT_FALSE(v5.AddLabel(ttl_lbl).HasError());
    ASSERT_FALSE(v5.SetProperty(ttl_prop, memgraph::storage::PropertyValue(older_ts)).HasError());
    ASSERT_FALSE(v6.AddLabel(ttl_lbl).HasError());
    ASSERT_FALSE(v6.SetProperty(ttl_prop, memgraph::storage::PropertyValue(newer_ts)).HasError());

    auto e1 = acc->CreateEdge(&v1, &v2, et1);  // stable vertices no ttl prop
    ASSERT_TRUE(e1.HasValue());
    auto e2 = acc->CreateEdge(&v1, &v2, et2);  // stable vertices older ttl ts
    ASSERT_TRUE(e2.HasValue());
    auto e3 = acc->CreateEdge(&v5, &v2, et1);  // older ttl vertex, no ttl prop
    ASSERT_TRUE(e3.HasValue());
    auto e4 = acc->CreateEdge(&v6, &v2, et1);  // newer ttl vertex, with older ttl prop
    ASSERT_TRUE(e4.HasValue());
    auto e5 = acc->CreateEdge(&v2, &v3, et1);  // stable vertices, newer ttl prop
    ASSERT_TRUE(e5.HasValue());

    if (this->HasPropOnEdge()) {  // edge prop enabled
      ASSERT_FALSE(e2->SetProperty(ttl_prop, memgraph::storage::PropertyValue(older_ts)).HasError());
      ASSERT_FALSE(e4->SetProperty(ttl_prop, memgraph::storage::PropertyValue(older_ts)).HasError());
      ASSERT_FALSE(e5->SetProperty(ttl_prop, memgraph::storage::PropertyValue(newer_ts)).HasError());
    }

    ASSERT_FALSE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).HasError());
  }
  {
    auto acc = this->db_->Access();
    size_t size = 0;
    for (const auto v : acc->Vertices(memgraph::storage::View::NEW)) ++size;
    EXPECT_EQ(size, 6);
  }
  this->ttl_->Enable();
  this->ttl_->Configure(this->RunEdgeTTL());
  EXPECT_NO_THROW(this->ttl_->SetInterval(std::chrono::milliseconds(700)));
  this->ttl_->Resume();
  this->EnsureTTLIndicesReady();  // Ensure indices are created and ready
  std::this_thread::sleep_for(std::chrono::seconds(1));
  {
    auto acc = this->db_->Access();
    size_t size = 0;
    size_t edge_size = 0;
    for (const auto v : acc->Vertices(memgraph::storage::View::NEW)) {
      if (v.IsVisible(memgraph::storage::View::NEW)) {
        ++size;
        auto edges = v.OutEdges(memgraph::storage::View::NEW);
        ASSERT_TRUE(edges.HasValue());
        for (const auto e : edges.GetValue().edges) {
          edge_size += e.IsVisible(memgraph::storage::View::NEW);
        }
      }
    }
    EXPECT_EQ(size, 5);
    if (this->RunEdgeTTL()) {
      EXPECT_EQ(edge_size, 2);
    } else {
      EXPECT_EQ(edge_size, 4);
    }
  }
  std::this_thread::sleep_for(std::chrono::seconds(3));
  {
    auto acc = this->db_->Access();
    size_t size = 0;
    size_t edge_size = 0;
    for (const auto v : acc->Vertices(memgraph::storage::View::NEW)) {
      if (v.IsVisible(memgraph::storage::View::NEW)) {
        ++size;
        auto edges = v.OutEdges(memgraph::storage::View::NEW);
        ASSERT_TRUE(edges.HasValue());
        for (const auto e : edges.GetValue().edges) {
          edge_size += e.IsVisible(memgraph::storage::View::NEW);
        }
      }
    }
    EXPECT_EQ(size, 4);
    if (this->RunEdgeTTL()) {
      EXPECT_EQ(edge_size, 1);
    } else {
      EXPECT_EQ(edge_size, 3);
    }
  }
}

// Needs user-defined timezone
TEST(TtlInfo, PersistentTimezone) {
  memgraph::utils::global_settings.Initialize("/tmp/ttl");
  memgraph::flags::run_time::Initialize();
  // Default value
  EXPECT_EQ(memgraph::flags::run_time::GetTimezone()->name(), "Etc/UTC");
  // New value
  memgraph::utils::global_settings.SetValue("timezone", "Europe/Rome");
  EXPECT_EQ(memgraph::flags::run_time::GetTimezone()->name(), "Europe/Rome");
  memgraph::utils::global_settings.Finalize();

  // Recover previous value
  memgraph::utils::global_settings.Initialize("/tmp/ttl");
  memgraph::flags::run_time::Initialize();
  EXPECT_EQ(memgraph::flags::run_time::GetTimezone()->name(), "Europe/Rome");
  memgraph::utils::global_settings.Finalize();

  memgraph::utils::OnScopeExit clean_up([] {
    memgraph::utils::global_settings.Finalize();
    std::filesystem::remove_all("/tmp/ttl");
  });
}

// Needs user-defined timezone
TEST(TtlInfo, String) {
  memgraph::utils::global_settings.Initialize("/tmp/ttl");
  memgraph::flags::run_time::Initialize();

  memgraph::utils::OnScopeExit clean_up([] {
    memgraph::utils::global_settings.Finalize();
    std::filesystem::remove_all("/tmp/ttl");
  });

  {
    auto period = std::chrono::hours(1) + std::chrono::minutes(23) + std::chrono::seconds(59);
    auto period_str = memgraph::storage::ttl::TtlInfo::StringifyPeriod(period);
    EXPECT_EQ(period_str, "1h23m59s");
    EXPECT_EQ(period, memgraph::storage::ttl::TtlInfo::ParsePeriod(period_str));
  }
  {
    auto period = std::chrono::days(45) + std::chrono::seconds(120 + 59);
    auto period_str = memgraph::storage::ttl::TtlInfo::StringifyPeriod(period);
    EXPECT_EQ(period_str, "45d2m59s");
    EXPECT_EQ(period, memgraph::storage::ttl::TtlInfo::ParsePeriod(period_str));
  }
  {
    auto period = std::chrono::hours(25);
    auto period_str = memgraph::storage::ttl::TtlInfo::StringifyPeriod(period);
    EXPECT_EQ(period_str, "1d1h");
    EXPECT_EQ(period, memgraph::storage::ttl::TtlInfo::ParsePeriod(period_str));
  }
  {
    // Has to handle time zones (hours can differ)
    memgraph::utils::global_settings.SetValue("timezone", "UTC");
    auto time = memgraph::storage::ttl::TtlInfo::ParseStartTime("03:45:10");
    auto epoch = time.time_since_epoch();
    GetPart<std::chrono::hours>(epoch);  // consume and ignore
    EXPECT_EQ(GetPart<std::chrono::minutes>(epoch), 45);
    EXPECT_EQ(GetPart<std::chrono::seconds>(epoch), 10);
    auto time_str = memgraph::storage::ttl::TtlInfo::StringifyStartTime(time);
    EXPECT_EQ(time_str, "03:45:10");
  }
  {
    // Has to handle time zones (hours can differ)
    memgraph::utils::global_settings.SetValue("timezone", "Europe/Rome");
    auto time = memgraph::storage::ttl::TtlInfo::ParseStartTime("03:45:10");
    auto epoch = time.time_since_epoch();
    GetPart<std::chrono::hours>(epoch);  // consume and ignore
    EXPECT_EQ(GetPart<std::chrono::minutes>(epoch), 45);
    EXPECT_EQ(GetPart<std::chrono::seconds>(epoch), 10);
    auto time_str = memgraph::storage::ttl::TtlInfo::StringifyStartTime(time);
    EXPECT_EQ(time_str, "03:45:10");
  }
  {
    // Has to handle time zones (hours can differ)
    memgraph::utils::global_settings.SetValue("timezone", "America/Los_Angeles");
    auto time = memgraph::storage::ttl::TtlInfo::ParseStartTime("03:45:10");
    auto epoch = time.time_since_epoch();
    GetPart<std::chrono::hours>(epoch);  // consume and ignore
    EXPECT_EQ(GetPart<std::chrono::minutes>(epoch), 45);
    EXPECT_EQ(GetPart<std::chrono::seconds>(epoch), 10);
    auto time_str = memgraph::storage::ttl::TtlInfo::StringifyStartTime(time);
    EXPECT_EQ(time_str, "03:45:10");
  }
  {
    const auto time_str = "12:34:56";
    memgraph::utils::global_settings.SetValue("timezone", "UTC");
    auto utc = memgraph::storage::ttl::TtlInfo::ParseStartTime(time_str);
    memgraph::utils::global_settings.SetValue("timezone", "Europe/Rome");
    auto rome = memgraph::storage::ttl::TtlInfo::ParseStartTime(time_str);
    memgraph::utils::global_settings.SetValue("timezone", "America/Los_Angeles");
    auto la = memgraph::storage::ttl::TtlInfo::ParseStartTime(time_str);
    // Time is converted to local date time; so might be influenced by day-light savings
    EXPECT_TRUE(utc == rome + std::chrono::hours(2) || utc == rome + std::chrono::hours(1))
        << "[ERROR] UTC " << utc << " Rome " << rome;
    EXPECT_TRUE(utc == la - std::chrono::hours(7) || utc == la - std::chrono::hours(8))
        << "[ERROR] UTC " << utc << " LA " << la;
  }
}

// Test for user-defined check functionality
TEST(TTLUserCheckTest, UserCheckFunctionality) {
  // Create a simple storage for testing
  memgraph::storage::Config config{};
  config.durability.storage_directory = std::filesystem::temp_directory_path() / "ttl_user_check_test";
  std::filesystem::remove_all(config.durability.storage_directory);

  memgraph::utils::Synchronized<memgraph::replication::ReplicationState, memgraph::utils::RWSpinLock> repl_state{
      memgraph::storage::ReplicationStateRootPath(config)};
  memgraph::utils::Gatekeeper<memgraph::dbms::Database> db_gk{config, repl_state};

  auto db_acc_opt = db_gk.access();
  ASSERT_TRUE(db_acc_opt) << "Failed to access db";
  auto &db_acc = *db_acc_opt;

  auto *ttl = &db_acc->ttl();
  auto ttl_lbl = db_acc->storage()->NameToLabel("TTL");
  auto ttl_prop = db_acc->storage()->NameToProperty("ttl");

  // Create test vertices with TTL properties
  auto now = std::chrono::system_clock::now();
  auto older = now - std::chrono::seconds(10);
  auto older_ts = std::chrono::duration_cast<std::chrono::microseconds>(older.time_since_epoch()).count();
  auto newer = now + std::chrono::seconds(5);
  auto newer_ts = std::chrono::duration_cast<std::chrono::microseconds>(newer.time_since_epoch()).count();

  // Create vertices: 2 with TTL label and older timestamp (should be deleted), 1 with newer timestamp (should stay)
  {
    auto acc = db_acc->Access();
    auto v1 = acc->CreateVertex();  // TTL label and older timestamp (should be deleted)
    auto v2 = acc->CreateVertex();  // TTL label and older timestamp (should be deleted)
    auto v3 = acc->CreateVertex();  // TTL label and newer timestamp (should stay)

    ASSERT_FALSE(v1.AddLabel(ttl_lbl).HasError());
    ASSERT_FALSE(v1.SetProperty(ttl_prop, memgraph::storage::PropertyValue(older_ts)).HasError());
    ASSERT_FALSE(v2.AddLabel(ttl_lbl).HasError());
    ASSERT_FALSE(v2.SetProperty(ttl_prop, memgraph::storage::PropertyValue(older_ts)).HasError());
    ASSERT_FALSE(v3.AddLabel(ttl_lbl).HasError());
    ASSERT_FALSE(v3.SetProperty(ttl_prop, memgraph::storage::PropertyValue(newer_ts)).HasError());

    ASSERT_FALSE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).HasError());
  }

  // Verify initial count (should be 3 vertices)
  {
    auto acc = db_acc->Access();
    size_t size = 0;
    for (const auto v : acc->Vertices(memgraph::storage::View::NEW)) ++size;
    EXPECT_EQ(size, 3) << "Initial vertex count should be 3";
  }

  // Test 1: Set user check to always return false (simulating replica) - TTL should not run
  ttl->Enable();
  ttl->Configure(false);
  ttl->SetInterval(std::chrono::milliseconds(100));
  ttl->SetUserCheck([]() -> bool { return false; });
  ttl->Resume();

  // Wait for TTL to potentially run - but it shouldn't because user check returns false
  std::this_thread::sleep_for(std::chrono::milliseconds(500));

  // Verify vertices are still there (TTL should not have run)
  {
    auto acc = db_acc->Access();
    size_t size = 0;
    for (const auto v : acc->Vertices(memgraph::storage::View::NEW)) ++size;
    EXPECT_EQ(size, 3) << "Vertices should still be there when user check returns false";
  }

  // Test 2: Set user check to always return true (simulating main) - TTL should run
  ttl->SetUserCheck([]() -> bool { return true; });

  // Wait for TTL to run and delete the older vertices
  std::this_thread::sleep_for(std::chrono::milliseconds(500));

  // Verify only 1 vertex remains (the newer one)
  {
    auto acc = db_acc->Access();
    size_t size = 0;
    for (const auto v : acc->Vertices(memgraph::storage::View::NEW)) ++size;
    EXPECT_EQ(size, 1) << "Only the newer vertex should remain when user check returns true";
  }

  // Set user check to false - TTL should not run
  bool user_bool = false;
  ttl->SetUserCheck([&user_bool]() -> bool { return user_bool; });

  // Test 3: Test dynamic behavior - create new vertices and toggle the check
  {
    auto acc = db_acc->Access();
    auto v4 = acc->CreateVertex();  // TTL label and older timestamp
    ASSERT_FALSE(v4.AddLabel(ttl_lbl).HasError());
    ASSERT_FALSE(v4.SetProperty(ttl_prop, memgraph::storage::PropertyValue(older_ts)).HasError());
    ASSERT_FALSE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).HasError());
  }

  // Verify we now have 2 vertices
  {
    auto acc = db_acc->Access();
    size_t size = 0;
    for (const auto v : acc->Vertices(memgraph::storage::View::NEW)) ++size;
    EXPECT_EQ(size, 2) << "Should have 2 vertices after adding new one";
  }

  std::this_thread::sleep_for(std::chrono::milliseconds(500));

  // Verify vertices are still there
  {
    auto acc = db_acc->Access();
    size_t size = 0;
    for (const auto v : acc->Vertices(memgraph::storage::View::NEW)) ++size;
    EXPECT_EQ(size, 2) << "Vertices should still be there when user check returns false";
  }

  // Set user check back to true - TTL should run and delete the older vertex
  user_bool = true;
  std::this_thread::sleep_for(std::chrono::milliseconds(500));

  // Verify only 1 vertex remains again
  {
    auto acc = db_acc->Access();
    size_t size = 0;
    for (const auto v : acc->Vertices(memgraph::storage::View::NEW)) ++size;
    EXPECT_EQ(size, 1) << "Only the newer vertex should remain after TTL runs again";
  }

  // Cleanup
  ttl->Disable();
  db_acc->StopAllBackgroundTasks();
  std::filesystem::remove_all(config.durability.storage_directory);
}
