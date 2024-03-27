// Copyright 2024 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include <chrono>
#include <cstring>
#include <filesystem>
#include <iostream>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include "storage/v2/id_types.hpp"
#include "storage/v2/property_disk_store.hpp"
#include "storage/v2/property_value.hpp"
#include "storage/v2/temporal.hpp"

const static std::filesystem::path test_root{"/tmp/MG_pds_test"};

class PdsTest : public ::testing::Test {
 protected:
  PdsTest() { memgraph::storage::PDS::Init(test_root); }

  ~PdsTest() override {
    memgraph::storage::PDS::Deinit();
    try {
      if (std::filesystem::exists(test_root)) {
        std::filesystem::remove_all(test_root);
      }
    } catch (...) {
    }
  }
};

TEST_F(PdsTest, Keys) {
  using namespace memgraph::storage;
  auto *pds = PDS::get();

  auto gid = Gid::FromUint(13);
  const auto gid_sv = pds->ToPrefix(gid);
  EXPECT_TRUE(memcmp(&gid, gid_sv.data(), sizeof(uint64_t)) == 0);
  EXPECT_EQ(gid, pds->ToGid(gid_sv));

  auto pid = PropertyId::FromUint(243);
  const auto key = pds->ToKey(gid, pid);
  EXPECT_TRUE(memcmp(&gid, key.data(), sizeof(uint64_t)) == 0);
  EXPECT_TRUE(memcmp(&pid, &key[sizeof(gid)], sizeof(uint32_t)) == 0);
  EXPECT_EQ(pid, pds->ToPid(key));
}

TEST_F(PdsTest, BasicUsage) {
  using namespace memgraph::storage;
  auto *pds = PDS::get();

  Gid gid;
  PropertyId pid;

  PropertyValue pv_bf(false);
  PropertyValue pv_bt(true);
  PropertyValue pv_int0(0);
  PropertyValue pv_int1(1);
  PropertyValue pv_int16(16);
  PropertyValue pv_640(int64_t(0));
  PropertyValue pv_641(int64_t(1));
  PropertyValue pv_64256(int64_t(256));
  PropertyValue pv_double0(0.0);
  PropertyValue pv_double1(1.0);
  PropertyValue pv_double1024(10.24);
  PropertyValue pv_str("");
  PropertyValue pv_str0("0");
  PropertyValue pv_strabc("abc");
  PropertyValue pv_tdldt0(TemporalData(TemporalType::LocalDateTime, 0));
  PropertyValue pv_tdlt0(TemporalData(TemporalType::LocalTime, 0));
  PropertyValue pv_tdd0(TemporalData(TemporalType::Date, 0));
  PropertyValue pv_tddur0(TemporalData(TemporalType::Duration, 0));
  PropertyValue pv_tdldt1(TemporalData(TemporalType::LocalDateTime, 100000));
  PropertyValue pv_tdlt1(TemporalData(TemporalType::LocalTime, 100000));
  PropertyValue pv_tdd1(TemporalData(TemporalType::Date, 100000));
  PropertyValue pv_tddur1(TemporalData(TemporalType::Duration, 100000));
  PropertyValue pv_v(std::vector<PropertyValue>{PropertyValue(false), PropertyValue(1), PropertyValue(256),
                                                PropertyValue(1.123), PropertyValue("")});
  PropertyValue pv_vv(std::vector<PropertyValue>{
      PropertyValue{std::vector<PropertyValue>{PropertyValue(false), PropertyValue(1), PropertyValue(256),
                                               PropertyValue(1.123), PropertyValue("")}},
      PropertyValue{"string"}, PropertyValue{"list"}});

  auto test = [&] {
    {
      ASSERT_TRUE(pds->Set(gid, pid, pv_bf));
      const auto val = pds->Get(gid, pid);
      ASSERT_TRUE(val);
      ASSERT_TRUE(val->IsBool());
      ASSERT_FALSE(val->ValueBool());
    }
    {
      ASSERT_TRUE(pds->Set(gid, pid, pv_bt));
      const auto val = pds->Get(gid, pid);
      ASSERT_TRUE(val);
      ASSERT_TRUE(val->IsBool());
      ASSERT_TRUE(val->ValueBool());
    }
    {
      ASSERT_TRUE(pds->Set(gid, pid, pv_int0));
      const auto val = pds->Get(gid, pid);
      ASSERT_TRUE(val);
      ASSERT_TRUE(val->IsInt());
      ASSERT_EQ(val->ValueInt(), 0);
    }
    {
      ASSERT_TRUE(pds->Set(gid, pid, pv_int1));
      const auto val = pds->Get(gid, pid);
      ASSERT_TRUE(val);
      ASSERT_TRUE(val->IsInt());
      ASSERT_EQ(val->ValueInt(), 1);
    }
    {
      ASSERT_TRUE(pds->Set(gid, pid, pv_int16));
      const auto val = pds->Get(gid, pid);
      ASSERT_TRUE(val);
      ASSERT_TRUE(val->IsInt());
      ASSERT_EQ(val->ValueInt(), 16);
    }
    {
      ASSERT_TRUE(pds->Set(gid, pid, pv_640));
      const auto val = pds->Get(gid, pid);
      ASSERT_TRUE(val);
      ASSERT_TRUE(val->IsInt());
      ASSERT_EQ(val->ValueInt(), 0);
    }
    {
      ASSERT_TRUE(pds->Set(gid, pid, pv_641));
      const auto val = pds->Get(gid, pid);
      ASSERT_TRUE(val);
      ASSERT_TRUE(val->IsInt());
      ASSERT_EQ(val->ValueInt(), 1);
    }
    {
      ASSERT_TRUE(pds->Set(gid, pid, pv_64256));
      const auto val = pds->Get(gid, pid);
      ASSERT_TRUE(val);
      ASSERT_TRUE(val->IsInt());
      ASSERT_EQ(val->ValueInt(), 256);
    }
    {
      ASSERT_TRUE(pds->Set(gid, pid, pv_double0));
      const auto val = pds->Get(gid, pid);
      ASSERT_TRUE(val);
      ASSERT_TRUE(val->IsDouble());
      ASSERT_EQ(val->ValueDouble(), 0.0);
    }
    {
      ASSERT_TRUE(pds->Set(gid, pid, pv_double1));
      const auto val = pds->Get(gid, pid);
      ASSERT_TRUE(val);
      ASSERT_TRUE(val->IsDouble());
      ASSERT_EQ(val->ValueDouble(), 1.0);
    }
    {
      ASSERT_TRUE(pds->Set(gid, pid, pv_double1024));
      const auto val = pds->Get(gid, pid);
      ASSERT_TRUE(val);
      ASSERT_TRUE(val->IsDouble());
      ASSERT_EQ(val->ValueDouble(), 10.24);
    }
    {
      ASSERT_TRUE(pds->Set(gid, pid, pv_str));
      const auto val = pds->Get(gid, pid);
      ASSERT_TRUE(val);
      ASSERT_TRUE(val->IsString());
      ASSERT_EQ(val->ValueString(), "");
    }
    {
      ASSERT_TRUE(pds->Set(gid, pid, pv_str0));
      const auto val = pds->Get(gid, pid);
      ASSERT_TRUE(val);
      ASSERT_TRUE(val->IsString());
      ASSERT_EQ(val->ValueString(), "0");
    }
    {
      ASSERT_TRUE(pds->Set(gid, pid, pv_strabc));
      const auto val = pds->Get(gid, pid);
      ASSERT_TRUE(val);
      ASSERT_TRUE(val->IsString());
      ASSERT_EQ(val->ValueString(), "abc");
    }
    {
      ASSERT_TRUE(pds->Set(gid, pid, pv_tdd0));
      const auto val = pds->Get(gid, pid);
      ASSERT_TRUE(val);
      ASSERT_TRUE(val->IsTemporalData());
      ASSERT_EQ(val->ValueTemporalData(), pv_tdd0.ValueTemporalData());
    }
    {
      ASSERT_TRUE(pds->Set(gid, pid, pv_tdd1));
      const auto val = pds->Get(gid, pid);
      ASSERT_TRUE(val);
      ASSERT_TRUE(val->IsTemporalData());
      ASSERT_EQ(val->ValueTemporalData(), pv_tdd1.ValueTemporalData());
    }
    {
      ASSERT_TRUE(pds->Set(gid, pid, pv_tddur0));
      const auto val = pds->Get(gid, pid);
      ASSERT_TRUE(val);
      ASSERT_TRUE(val->IsTemporalData());
      ASSERT_EQ(val->ValueTemporalData(), pv_tddur0.ValueTemporalData());
    }
    {
      ASSERT_TRUE(pds->Set(gid, pid, pv_tddur1));
      const auto val = pds->Get(gid, pid);
      ASSERT_TRUE(val);
      ASSERT_TRUE(val->IsTemporalData());
      ASSERT_EQ(val->ValueTemporalData(), pv_tddur1.ValueTemporalData());
    }
    {
      ASSERT_TRUE(pds->Set(gid, pid, pv_tdldt0));
      const auto val = pds->Get(gid, pid);
      ASSERT_TRUE(val);
      ASSERT_TRUE(val->IsTemporalData());
      ASSERT_EQ(val->ValueTemporalData(), pv_tdldt0.ValueTemporalData());
    }
    {
      ASSERT_TRUE(pds->Set(gid, pid, pv_tdldt1));
      const auto val = pds->Get(gid, pid);
      ASSERT_TRUE(val);
      ASSERT_TRUE(val->IsTemporalData());
      ASSERT_EQ(val->ValueTemporalData(), pv_tdldt1.ValueTemporalData());
    }
    {
      ASSERT_TRUE(pds->Set(gid, pid, pv_tdlt0));
      const auto val = pds->Get(gid, pid);
      ASSERT_TRUE(val);
      ASSERT_TRUE(val->IsTemporalData());
      ASSERT_EQ(val->ValueTemporalData(), pv_tdlt0.ValueTemporalData());
    }
    {
      ASSERT_TRUE(pds->Set(gid, pid, pv_tdlt1));
      const auto val = pds->Get(gid, pid);
      ASSERT_TRUE(val);
      ASSERT_TRUE(val->IsTemporalData());
      ASSERT_EQ(val->ValueTemporalData(), pv_tdlt1.ValueTemporalData());
    }
    {
      ASSERT_TRUE(pds->Set(gid, pid, pv_v));
      const auto val = pds->Get(gid, pid);
      ASSERT_TRUE(val);
      ASSERT_TRUE(val->IsList());
      const auto list = val->ValueList();
      ASSERT_EQ(list.size(), 5);
      ASSERT_EQ(list[0], PropertyValue(false));
      ASSERT_EQ(list[1], PropertyValue(1));
      ASSERT_EQ(list[2], PropertyValue(256));
      ASSERT_EQ(list[3], PropertyValue(1.123));
      ASSERT_EQ(list[4], PropertyValue(""));
    }
    {
      ASSERT_TRUE(pds->Set(gid, pid, pv_vv));
      const auto val = pds->Get(gid, pid);
      ASSERT_TRUE(val);
      ASSERT_TRUE(val->IsList());
      const auto list = val->ValueList();
      ASSERT_EQ(list.size(), 3);
      {
        const auto &val = list[0];
        ASSERT_TRUE(val.IsList());
        const auto list = val.ValueList();
        ASSERT_EQ(list.size(), 5);
        ASSERT_EQ(list[0], PropertyValue(false));
        ASSERT_EQ(list[1], PropertyValue(1));
        ASSERT_EQ(list[2], PropertyValue(256));
        ASSERT_EQ(list[3], PropertyValue(1.123));
        ASSERT_EQ(list[4], PropertyValue(""));
      }
      ASSERT_EQ(list[1], PropertyValue("string"));
      ASSERT_EQ(list[2], PropertyValue("list"));
    }
  };

  gid.FromUint(0);
  pid.FromUint(0);
  test();

  gid.FromUint(0);
  pid.FromUint(1);
  test();

  gid.FromUint(1);
  pid.FromUint(0);
  test();

  gid.FromUint(1);
  pid.FromUint(1);
  test();

  gid.FromUint(0);
  pid.FromUint(5446516);
  test();

  gid.FromUint(654645);
  pid.FromUint(0);
  test();

  gid.FromUint(987615);
  pid.FromUint(565);
  test();
}

TEST_F(PdsTest, Get) {
  using namespace memgraph::storage;
  auto *pds = PDS::get();
  pds->Set(Gid::FromUint(0), PropertyId::FromUint(1), PropertyValue{"test1"});
  pds->Set(Gid::FromUint(0), PropertyId::FromUint(2), PropertyValue{"test2"});
  pds->Set(Gid::FromUint(0), PropertyId::FromUint(3), PropertyValue{"test3"});
  pds->Set(Gid::FromUint(1), PropertyId::FromUint(0), PropertyValue{"test0"});
  pds->Set(Gid::FromUint(1), PropertyId::FromUint(2), PropertyValue{"test02"});

  auto all_0 = pds->Get(Gid::FromUint(0));
  ASSERT_EQ(all_0.size(), 3);
  ASSERT_EQ(all_0[PropertyId::FromUint(1)], PropertyValue{"test1"});
  ASSERT_EQ(all_0[PropertyId::FromUint(2)], PropertyValue{"test2"});
  ASSERT_EQ(all_0[PropertyId::FromUint(3)], PropertyValue{"test3"});
}
