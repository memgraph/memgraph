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

#include <unistd.h>

#include <gtest/gtest.h>

#include "kvstore/kvstore.hpp"
#include "utils/file.hpp"

namespace fs = std::filesystem;

class KVStore : public ::testing::Test {
 protected:
  void SetUp() override { memgraph::utils::EnsureDir(test_folder_); }

  void TearDown() override { fs::remove_all(test_folder_); }

  fs::path test_folder_{fs::temp_directory_path() /
                        ("unit_kvstore_test_" + std::to_string(static_cast<int>(getpid())))};
};

TEST_F(KVStore, PutGet) {
  memgraph::kvstore::KVStore kvstore(test_folder_ / "PutGet");
  ASSERT_TRUE(kvstore.Put("key", "value"));
  ASSERT_EQ(kvstore.Get("key").value(), "value");
}

TEST_F(KVStore, PutMultipleGet) {
  memgraph::kvstore::KVStore kvstore(test_folder_ / "PutMultipleGet");
  ASSERT_TRUE(kvstore.PutMultiple({{"key1", "value1"}, {"key2", "value2"}}));
  ASSERT_EQ(kvstore.Get("key1").value(), "value1");
  ASSERT_EQ(kvstore.Get("key2").value(), "value2");
}

TEST_F(KVStore, PutGetDeleteGet) {
  memgraph::kvstore::KVStore kvstore(test_folder_ / "PutGetDeleteGet");
  ASSERT_TRUE(kvstore.Put("key", "value"));
  ASSERT_EQ(kvstore.Get("key").value(), "value");
  ASSERT_TRUE(kvstore.Delete("key"));
  ASSERT_FALSE(static_cast<bool>(kvstore.Get("key")));
}

TEST_F(KVStore, PutMultipleGetDeleteMultipleGet) {
  memgraph::kvstore::KVStore kvstore(test_folder_ / "PutMultipleGetDeleteMultipleGet");
  ASSERT_TRUE(kvstore.PutMultiple({{"key1", "value1"}, {"key2", "value2"}}));
  ASSERT_EQ(kvstore.Get("key1").value(), "value1");
  ASSERT_EQ(kvstore.Get("key2").value(), "value2");
  ASSERT_TRUE(kvstore.DeleteMultiple({"key1", "key2", "key3"}));
  ASSERT_FALSE(static_cast<bool>(kvstore.Get("key1")));
  ASSERT_FALSE(static_cast<bool>(kvstore.Get("key2")));
  ASSERT_FALSE(static_cast<bool>(kvstore.Get("key3")));
}

TEST_F(KVStore, PutMultipleGetPutAndDeleteMultipleGet) {
  memgraph::kvstore::KVStore kvstore(test_folder_ / "PutMultipleGetPutAndDeleteMultipleGet");
  ASSERT_TRUE(kvstore.PutMultiple({{"key1", "value1"}, {"key2", "value2"}}));
  ASSERT_EQ(kvstore.Get("key1").value(), "value1");
  ASSERT_EQ(kvstore.Get("key2").value(), "value2");
  ASSERT_TRUE(kvstore.PutAndDeleteMultiple({{"key3", "value3"}}, {"key1", "key2"}));
  ASSERT_FALSE(static_cast<bool>(kvstore.Get("key1")));
  ASSERT_FALSE(static_cast<bool>(kvstore.Get("key2")));
  ASSERT_EQ(kvstore.Get("key3").value(), "value3");
}

TEST_F(KVStore, Durability) {
  {
    memgraph::kvstore::KVStore kvstore(test_folder_ / "Durability");
    ASSERT_TRUE(kvstore.Put("key", "value"));
  }
  {
    memgraph::kvstore::KVStore kvstore(test_folder_ / "Durability");
    ASSERT_EQ(kvstore.Get("key").value(), "value");
  }
}

TEST_F(KVStore, Size) {
  memgraph::kvstore::KVStore kvstore(test_folder_ / "Size");

  ASSERT_TRUE(kvstore.Put("prefix_1", "jedan"));
  ASSERT_TRUE(kvstore.Put("prefix_2", "dva"));
  ASSERT_TRUE(kvstore.Put("prefix_3", "tri"));
  ASSERT_TRUE(kvstore.Put("prefix_4", "cetiri"));
  ASSERT_TRUE(kvstore.Put("prefix_5", "pet"));

  EXPECT_EQ(kvstore.Size("a"), 0);
  EXPECT_EQ(kvstore.Size(), 5);
  EXPECT_EQ(kvstore.Size("prefix_"), 5);
  EXPECT_EQ(kvstore.Size("prefix_1"), 1);

  ASSERT_TRUE(kvstore.Put("predmetak_1", "jedan"));
  ASSERT_TRUE(kvstore.Put("predmetak_2", "dva"));
  ASSERT_TRUE(kvstore.Put("predmetak_3", "tri"));
  ASSERT_TRUE(kvstore.Put("predmetak_4", "cetiri"));

  EXPECT_EQ(kvstore.Size("a"), 0);
  EXPECT_EQ(kvstore.Size(), 9);
  EXPECT_EQ(kvstore.Size("prefix_"), 5);
  EXPECT_EQ(kvstore.Size("predmetak_"), 4);
  EXPECT_EQ(kvstore.Size("p"), 9);
  EXPECT_EQ(kvstore.Size("pre"), 9);
  EXPECT_EQ(kvstore.Size("pred"), 4);
  EXPECT_EQ(kvstore.Size("pref"), 5);
  EXPECT_EQ(kvstore.Size("prex"), 0);
}

TEST_F(KVStore, DeletePrefix) {
  memgraph::kvstore::KVStore kvstore(test_folder_ / "DeletePrefix");

  ASSERT_TRUE(kvstore.Put("prefix_1", "jedan"));
  ASSERT_TRUE(kvstore.Put("prefix_2", "dva"));
  ASSERT_TRUE(kvstore.Put("prefix_3", "tri"));
  ASSERT_TRUE(kvstore.Put("prefix_4", "cetiri"));
  ASSERT_TRUE(kvstore.Put("prefix_5", "pet"));

  EXPECT_EQ(kvstore.Size(), 5);

  ASSERT_TRUE(kvstore.DeletePrefix("prefix_5"));

  EXPECT_EQ(kvstore.Size(), 4);
  EXPECT_EQ(kvstore.Size("prefix_"), 4);
  EXPECT_EQ(kvstore.Size("prefix_1"), 1);

  ASSERT_TRUE(kvstore.Put("predmetak_1", "jedan"));
  ASSERT_TRUE(kvstore.Put("predmetak_2", "dva"));
  ASSERT_TRUE(kvstore.Put("predmetak_3", "tri"));
  ASSERT_TRUE(kvstore.Put("predmetak_4", "cetiri"));

  EXPECT_EQ(kvstore.Size(), 8);

  ASSERT_TRUE(kvstore.DeletePrefix("predmetak_1"));
  EXPECT_EQ(kvstore.Size(), 7);

  ASSERT_TRUE(kvstore.DeletePrefix("prefix_"));
  EXPECT_EQ(kvstore.Size(), 3);

  ASSERT_TRUE(kvstore.DeletePrefix("predmetak_"));
  EXPECT_EQ(kvstore.Size(), 0);

  ASSERT_TRUE(kvstore.DeletePrefix("whatever"));
  EXPECT_EQ(kvstore.Size(), 0);
  EXPECT_EQ(kvstore.Size("any_prefix"), 0);
}

TEST_F(KVStore, Iterator) {
  memgraph::kvstore::KVStore kvstore(test_folder_ / "Iterator");

  for (int i = 1; i <= 4; ++i) ASSERT_TRUE(kvstore.Put("key" + std::to_string(i), "value" + std::to_string(i)));

  auto it = kvstore.begin();
  ASSERT_TRUE(it.IsValid());
  EXPECT_EQ(it->first, "key1");
  EXPECT_EQ((*it).second, "value1");

  ++it;
  ASSERT_TRUE(it.IsValid());
  EXPECT_EQ((*it).first, "key2");
  EXPECT_EQ(it->second, "value2");

  ++it;
  ASSERT_TRUE(it.IsValid());
  EXPECT_EQ(it->first, "key3");
  EXPECT_EQ((*it).second, "value3");

  ++it;
  ASSERT_TRUE(it.IsValid());
  EXPECT_EQ((*it).first, "key4");
  EXPECT_EQ(it->second, "value4");

  ++it;
  ASSERT_FALSE(it.IsValid());
}

TEST_F(KVStore, IteratorPrefix) {
  memgraph::kvstore::KVStore kvstore(test_folder_ / "Iterator");

  ASSERT_TRUE(kvstore.Put("a_1", "value1"));
  ASSERT_TRUE(kvstore.Put("a_2", "value2"));

  ASSERT_TRUE(kvstore.Put("aa_1", "value1"));
  ASSERT_TRUE(kvstore.Put("aa_2", "value2"));

  ASSERT_TRUE(kvstore.Put("b_1", "value1"));
  ASSERT_TRUE(kvstore.Put("b_2", "value2"));

  auto it = kvstore.begin("a");
  ASSERT_TRUE(it.IsValid());
  EXPECT_EQ(it->first, "a_1");

  ++it;
  ASSERT_TRUE(it.IsValid());
  EXPECT_EQ(it->first, "a_2");

  ++it;
  ASSERT_TRUE(it.IsValid());
  EXPECT_EQ(it->first, "aa_1");

  ++it;
  ASSERT_TRUE(it.IsValid());
  EXPECT_EQ(it->first, "aa_2");

  ++it;
  ASSERT_FALSE(it.IsValid());

  it = kvstore.begin("aa_");
  ASSERT_TRUE(it.IsValid());
  EXPECT_EQ(it->first, "aa_1");

  ++it;
  ASSERT_TRUE(it.IsValid());
  EXPECT_EQ(it->first, "aa_2");

  ++it;
  ASSERT_FALSE(it.IsValid());

  it = kvstore.begin("b_");
  ASSERT_TRUE(it.IsValid());
  EXPECT_EQ(it->first, "b_1");

  ++it;
  ASSERT_TRUE(it.IsValid());
  EXPECT_EQ(it->first, "b_2");

  ++it;
  ASSERT_FALSE(it.IsValid());

  it = kvstore.begin("unexisting_prefix");
  ASSERT_FALSE(it.IsValid());
}
