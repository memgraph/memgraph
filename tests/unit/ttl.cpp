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
#include "query/time_to_live/time_to_live.hpp"
#include "storage/v2/disk/storage.hpp"
#include "storage/v2/inmemory/storage.hpp"
#include "storage/v2/storage_mode.hpp"
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

  memgraph::query::ttl::TTL *ttl_{&db_->ttl()};

  void SetUp() override {}

  void TearDown() override {
    db_->StopAllBackgroundTasks();
    if (std::is_same<StorageType, memgraph::storage::DiskStorage>::value) {
      disk_test_utils::RemoveRocksDbDirs(testSuite);
    }
    std::filesystem::remove_all(data_directory_);
  }
};

using TestTypes = ::testing::Types<std::tuple<memgraph::storage::InMemoryStorage, std::true_type>,
                                   std::tuple<memgraph::storage::InMemoryStorage, std::false_type>,
                                   std::tuple<memgraph::storage::DiskStorage, std::true_type>,
                                   std::tuple<memgraph::storage::DiskStorage, std::false_type>>;
TYPED_TEST_SUITE(TTLFixture, TestTypes);

TYPED_TEST(TTLFixture, EnableTest) {
  const memgraph::query::ttl::TtlInfo ttl_info{std::chrono::days(1), std::chrono::system_clock::now()};
  EXPECT_FALSE(this->ttl_->Enabled());
  EXPECT_THROW(this->ttl_->Configure(ttl_info), memgraph::query::ttl::TtlException);
  EXPECT_THROW(this->ttl_->Setup(this->db_, &this->interpreter_context_, this->RunEdgeTTL()),
               memgraph::query::ttl::TtlException);
  this->ttl_->Enable();
  EXPECT_TRUE(this->ttl_->Enabled());
  EXPECT_THROW(this->ttl_->Configure({}), memgraph::query::ttl::TtlException);
  EXPECT_THROW(this->ttl_->Setup(this->db_, &this->interpreter_context_, this->RunEdgeTTL()),
               memgraph::query::ttl::TtlException);
  EXPECT_NO_THROW(this->ttl_->Configure(ttl_info));
  EXPECT_EQ(this->ttl_->Config(), ttl_info);
  EXPECT_NO_THROW(this->ttl_->Setup(this->db_, &this->interpreter_context_, this->RunEdgeTTL()));
  EXPECT_THROW(this->ttl_->Configure(ttl_info), memgraph::query::ttl::TtlException);
  this->ttl_->Stop();
  EXPECT_NO_THROW(this->ttl_->Setup(this->db_, &this->interpreter_context_, this->RunEdgeTTL()));
  EXPECT_EQ(this->ttl_->Config(), ttl_info);
  this->ttl_->Disable();
  EXPECT_FALSE(this->ttl_->Enabled());
  EXPECT_THROW(this->ttl_->Setup(this->db_, &this->interpreter_context_, this->RunEdgeTTL()),
               memgraph::query::ttl::TtlException);
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
    ASSERT_FALSE(acc->PrepareForCommitPhase().HasError());
  }
  {
    auto acc = this->db_->Access();
    auto all_v = acc->Vertices(memgraph::storage::View::NEW);
    size_t size = 0;
    for (const auto v : acc->Vertices(memgraph::storage::View::NEW)) ++size;
    EXPECT_EQ(size, 6);
  }
  this->ttl_->Enable();
  this->ttl_->Configure(memgraph::query::ttl::TtlInfo{std::chrono::milliseconds(700), {}});
  EXPECT_NO_THROW(this->ttl_->Setup(this->db_, &this->interpreter_context_, this->RunEdgeTTL()));
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
    ASSERT_FALSE(acc->PrepareForCommitPhase().HasError());
  }
  {
    auto acc = this->db_->Access();
    auto all_v = acc->Vertices(memgraph::storage::View::NEW);
    size_t size = 0;
    for (const auto v : acc->Vertices(memgraph::storage::View::NEW)) ++size;
    EXPECT_EQ(size, 6);
  }
  this->ttl_->Enable();
  this->ttl_->Configure(memgraph::query::ttl::TtlInfo{std::chrono::milliseconds(100),
                                                      std::chrono::system_clock::now() + std::chrono::seconds(3)});
  EXPECT_NO_THROW(this->ttl_->Setup(this->db_, &this->interpreter_context_, this->RunEdgeTTL()));
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

    ASSERT_FALSE(acc->PrepareForCommitPhase().HasError());
  }
  {
    auto acc = this->db_->Access();
    size_t size = 0;
    for (const auto v : acc->Vertices(memgraph::storage::View::NEW)) ++size;
    EXPECT_EQ(size, 6);
  }
  this->ttl_->Enable();
  this->ttl_->Configure(memgraph::query::ttl::TtlInfo{std::chrono::milliseconds(700), {}});
  EXPECT_NO_THROW(this->ttl_->Setup(this->db_, &this->interpreter_context_, this->RunEdgeTTL()));
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

TYPED_TEST(TTLFixture, Durability) {
  const auto path = GetCleanDataDirectory();
  ASSERT_TRUE(memgraph::utils::EnsureDir(path));
  auto clean_up = memgraph::utils::OnScopeExit([&] { std::filesystem::remove_all(path); });
  memgraph::query::ttl::TtlInfo ttl_info;
  {
    {
      memgraph::query::ttl::TTL ttl(path);
      ttl.Restore(this->db_, &this->interpreter_context_);
      EXPECT_FALSE(ttl.Enabled());
      EXPECT_EQ(ttl.Config(), memgraph::query::ttl::TtlInfo{});
      EXPECT_FALSE(ttl.Running());
    }
    {
      memgraph::query::ttl::TTL ttl(path);
      ttl.Enable();
    }
    {
      memgraph::query::ttl::TTL ttl(path);
      ttl.Restore(this->db_, &this->interpreter_context_);
      EXPECT_TRUE(ttl.Enabled());
      EXPECT_EQ(ttl.Config(), memgraph::query::ttl::TtlInfo{});
      EXPECT_FALSE(ttl.Running());
    }
    {
      ttl_info.period = std::chrono::minutes(12);
      memgraph::query::ttl::TTL ttl(path);
      ttl.Enable();
      ttl.Configure(ttl_info);
    }
    {
      memgraph::query::ttl::TTL ttl(path);
      ttl.Restore(this->db_, &this->interpreter_context_);
      EXPECT_TRUE(ttl.Enabled());
      EXPECT_EQ(ttl.Config(), ttl_info);
      EXPECT_FALSE(ttl.Running());
    }
    {
      ttl_info.period = std::chrono::hours(34);
      ttl_info.start_time = std::chrono::system_clock::now();
      memgraph::query::ttl::TTL ttl(path);
      ttl.Enable();
      ttl.Configure(ttl_info);
      ttl.Setup(this->db_, &this->interpreter_context_, this->RunEdgeTTL());
    }
    {
      memgraph::query::ttl::TTL ttl(path);
      ttl.Restore(this->db_, &this->interpreter_context_);
      EXPECT_TRUE(ttl.Enabled());
      EXPECT_EQ(ttl.Config().period, ttl_info.period);
      ASSERT_TRUE(ttl.Config().start_time && ttl_info.start_time);
      EXPECT_EQ(*ttl.Config().start_time, std::chrono::time_point_cast<std::chrono::seconds>(
                                              *ttl_info.start_time));  // Durability has seconds precision
      EXPECT_TRUE(ttl.Running());
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
    auto period_str = memgraph::query::ttl::TtlInfo::StringifyPeriod(period);
    EXPECT_EQ(period_str, "1h23m59s");
    EXPECT_EQ(period, memgraph::query::ttl::TtlInfo::ParsePeriod(period_str));
  }
  {
    auto period = std::chrono::days(45) + std::chrono::seconds(120 + 59);
    auto period_str = memgraph::query::ttl::TtlInfo::StringifyPeriod(period);
    EXPECT_EQ(period_str, "45d2m59s");
    EXPECT_EQ(period, memgraph::query::ttl::TtlInfo::ParsePeriod(period_str));
  }
  {
    auto period = std::chrono::hours(25);
    auto period_str = memgraph::query::ttl::TtlInfo::StringifyPeriod(period);
    EXPECT_EQ(period_str, "1d1h");
    EXPECT_EQ(period, memgraph::query::ttl::TtlInfo::ParsePeriod(period_str));
  }
  {
    // Has to handle time zones (hours can differ)
    memgraph::utils::global_settings.SetValue("timezone", "UTC");
    auto time = memgraph::query::ttl::TtlInfo::ParseStartTime("03:45:10");
    auto epoch = time.time_since_epoch();
    GetPart<std::chrono::hours>(epoch);  // consume and ignore
    EXPECT_EQ(GetPart<std::chrono::minutes>(epoch), 45);
    EXPECT_EQ(GetPart<std::chrono::seconds>(epoch), 10);
    auto time_str = memgraph::query::ttl::TtlInfo::StringifyStartTime(time);
    EXPECT_EQ(time_str, "03:45:10");
  }
  {
    // Has to handle time zones (hours can differ)
    memgraph::utils::global_settings.SetValue("timezone", "Europe/Rome");
    auto time = memgraph::query::ttl::TtlInfo::ParseStartTime("03:45:10");
    auto epoch = time.time_since_epoch();
    GetPart<std::chrono::hours>(epoch);  // consume and ignore
    EXPECT_EQ(GetPart<std::chrono::minutes>(epoch), 45);
    EXPECT_EQ(GetPart<std::chrono::seconds>(epoch), 10);
    auto time_str = memgraph::query::ttl::TtlInfo::StringifyStartTime(time);
    EXPECT_EQ(time_str, "03:45:10");
  }
  {
    // Has to handle time zones (hours can differ)
    memgraph::utils::global_settings.SetValue("timezone", "America/Los_Angeles");
    auto time = memgraph::query::ttl::TtlInfo::ParseStartTime("03:45:10");
    auto epoch = time.time_since_epoch();
    GetPart<std::chrono::hours>(epoch);  // consume and ignore
    EXPECT_EQ(GetPart<std::chrono::minutes>(epoch), 45);
    EXPECT_EQ(GetPart<std::chrono::seconds>(epoch), 10);
    auto time_str = memgraph::query::ttl::TtlInfo::StringifyStartTime(time);
    EXPECT_EQ(time_str, "03:45:10");
  }
  {
    const auto time_str = "12:34:56";
    memgraph::utils::global_settings.SetValue("timezone", "UTC");
    auto utc = memgraph::query::ttl::TtlInfo::ParseStartTime(time_str);
    memgraph::utils::global_settings.SetValue("timezone", "Europe/Rome");
    auto rome = memgraph::query::ttl::TtlInfo::ParseStartTime(time_str);
    memgraph::utils::global_settings.SetValue("timezone", "America/Los_Angeles");
    auto la = memgraph::query::ttl::TtlInfo::ParseStartTime(time_str);
    // Time is converted to local date time; so might be influenced by day-light savings
    EXPECT_TRUE(utc == rome + std::chrono::hours(2) || utc == rome + std::chrono::hours(1))
        << "[ERROR] UTC " << utc << " Rome " << rome;
    EXPECT_TRUE(utc == la - std::chrono::hours(7) || utc == la - std::chrono::hours(8))
        << "[ERROR] UTC " << utc << " LA " << la;
  }
}
