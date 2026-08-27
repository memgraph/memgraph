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

#include <string>
#include <vector>

#include "auth/atomic_auth_overlay.hpp"
#include "kvstore/kvstore.hpp"
#include "utils/file.hpp"

namespace fs = std::filesystem;
using memgraph::auth::AtomicAuthOverlay;

class AtomicAuthOverlayTest : public ::testing::Test {
 protected:
  void SetUp() override {
    memgraph::utils::EnsureDir(test_folder_);
    store_.emplace(test_folder_ / "overlay_test");
  }

  void TearDown() override { fs::remove_all(test_folder_); }

  fs::path test_folder_{fs::temp_directory_path() / "MG_tests_unit_atomic_auth_overlay"};
  std::optional<memgraph::kvstore::KVStore> store_;
};

// --- Basic read/write against the overlay ---

TEST_F(AtomicAuthOverlayTest, GetPassthroughToBase) {
  store_->Put("user:alice", "alice_data");

  AtomicAuthOverlay overlay(*store_);
  auto val = overlay.Get("user:alice");
  ASSERT_TRUE(val.has_value());
  EXPECT_EQ(*val, "alice_data");
}

TEST_F(AtomicAuthOverlayTest, GetNonexistentReturnsNullopt) {
  AtomicAuthOverlay overlay(*store_);
  auto val = overlay.Get("user:alice");
  EXPECT_FALSE(val.has_value());
}

TEST_F(AtomicAuthOverlayTest, PutThenGetReadsFromWriteSet) {
  store_->Put("user:alice", "old_data");

  AtomicAuthOverlay overlay(*store_);
  overlay.Put("user:alice", "new_data");
  auto val = overlay.Get("user:alice");
  ASSERT_TRUE(val.has_value());
  EXPECT_EQ(*val, "new_data");
}

TEST_F(AtomicAuthOverlayTest, PutNewKeyThenGet) {
  AtomicAuthOverlay overlay(*store_);
  overlay.Put("user:bob", "bob_data");
  auto val = overlay.Get("user:bob");
  ASSERT_TRUE(val.has_value());
  EXPECT_EQ(*val, "bob_data");
}

TEST_F(AtomicAuthOverlayTest, DeleteThenGetReturnsNullopt) {
  store_->Put("user:alice", "alice_data");

  AtomicAuthOverlay overlay(*store_);
  overlay.Delete("user:alice");
  auto val = overlay.Get("user:alice");
  EXPECT_FALSE(val.has_value());
}

TEST_F(AtomicAuthOverlayTest, DeleteThenPutSameKey) {
  store_->Put("user:alice", "old_data");

  AtomicAuthOverlay overlay(*store_);
  overlay.Delete("user:alice");
  overlay.Put("user:alice", "resurrected");
  auto val = overlay.Get("user:alice");
  ASSERT_TRUE(val.has_value());
  EXPECT_EQ(*val, "resurrected");
}

// --- Iteration ---

TEST_F(AtomicAuthOverlayTest, IterationIncludesBaseEntries) {
  store_->Put("user:alice", "a");
  store_->Put("user:bob", "b");

  AtomicAuthOverlay overlay(*store_);
  std::vector<std::pair<std::string, std::string>> entries;
  for (auto it = overlay.begin("user:"); it != overlay.end("user:"); ++it) {
    entries.emplace_back(*it);
  }
  ASSERT_EQ(entries.size(), 2);
  EXPECT_EQ(entries[0].first, "user:alice");
  EXPECT_EQ(entries[1].first, "user:bob");
}

TEST_F(AtomicAuthOverlayTest, IterationIncludesNewEntries) {
  store_->Put("user:alice", "a");

  AtomicAuthOverlay overlay(*store_);
  overlay.Put("user:bob", "b");

  std::vector<std::pair<std::string, std::string>> entries;
  for (auto it = overlay.begin("user:"); it != overlay.end("user:"); ++it) {
    entries.emplace_back(*it);
  }
  ASSERT_EQ(entries.size(), 2);
  EXPECT_EQ(entries[0].first, "user:alice");
  EXPECT_EQ(entries[1].first, "user:bob");
}

TEST_F(AtomicAuthOverlayTest, IterationExcludesDeletedEntries) {
  store_->Put("user:alice", "a");
  store_->Put("user:bob", "b");

  AtomicAuthOverlay overlay(*store_);
  overlay.Delete("user:alice");

  std::vector<std::pair<std::string, std::string>> entries;
  for (auto it = overlay.begin("user:"); it != overlay.end("user:"); ++it) {
    entries.emplace_back(*it);
  }
  ASSERT_EQ(entries.size(), 1);
  EXPECT_EQ(entries[0].first, "user:bob");
}

TEST_F(AtomicAuthOverlayTest, IterationReflectsUpdatedEntries) {
  store_->Put("user:alice", "old");

  AtomicAuthOverlay overlay(*store_);
  overlay.Put("user:alice", "new");

  std::vector<std::pair<std::string, std::string>> entries;
  for (auto it = overlay.begin("user:"); it != overlay.end("user:"); ++it) {
    entries.emplace_back(*it);
  }
  ASSERT_EQ(entries.size(), 1);
  EXPECT_EQ(entries[0].first, "user:alice");
  EXPECT_EQ(entries[0].second, "new");
}

TEST_F(AtomicAuthOverlayTest, IterationMergesSorted) {
  store_->Put("user:bob", "b");

  AtomicAuthOverlay overlay(*store_);
  overlay.Put("user:alice", "a");
  overlay.Put("user:charlie", "c");

  std::vector<std::string> keys;
  for (auto it = overlay.begin("user:"); it != overlay.end("user:"); ++it) {
    keys.emplace_back(it->first);
  }
  ASSERT_EQ(keys.size(), 3);
  EXPECT_EQ(keys[0], "user:alice");
  EXPECT_EQ(keys[1], "user:bob");
  EXPECT_EQ(keys[2], "user:charlie");
}

// --- Flush (commit) ---

TEST_F(AtomicAuthOverlayTest, FlushSucceedsWhenBaseUnchanged) {
  store_->Put("user:alice", "original");

  AtomicAuthOverlay overlay(*store_);
  overlay.Get("user:alice");  // snapshot the read
  overlay.Put("user:alice", "modified");

  EXPECT_TRUE(overlay.Flush());
  EXPECT_EQ(store_->Get("user:alice").value(), "modified");
}

TEST_F(AtomicAuthOverlayTest, FlushPersistsNewKeys) {
  AtomicAuthOverlay overlay(*store_);
  overlay.Put("user:bob", "bob_data");

  EXPECT_TRUE(overlay.Flush());
  EXPECT_EQ(store_->Get("user:bob").value(), "bob_data");
}

TEST_F(AtomicAuthOverlayTest, FlushPersistsDeletes) {
  store_->Put("user:alice", "data");

  AtomicAuthOverlay overlay(*store_);
  overlay.Get("user:alice");
  overlay.Delete("user:alice");

  EXPECT_TRUE(overlay.Flush());
  EXPECT_FALSE(store_->Get("user:alice").has_value());
}

TEST_F(AtomicAuthOverlayTest, FlushDetectsConflictOnModifiedKey) {
  store_->Put("user:alice", "original");

  AtomicAuthOverlay overlay(*store_);
  overlay.Get("user:alice");  // snapshot
  overlay.Put("user:alice", "our_change");

  // Concurrent modification
  store_->Put("user:alice", "concurrent_change");

  EXPECT_FALSE(overlay.Flush());
  // Base should retain the concurrent change
  EXPECT_EQ(store_->Get("user:alice").value(), "concurrent_change");
}

TEST_F(AtomicAuthOverlayTest, FlushDetectsConflictOnDeletedKey) {
  store_->Put("user:alice", "original");

  AtomicAuthOverlay overlay(*store_);
  overlay.Get("user:alice");  // snapshot
  overlay.Put("user:alice", "our_change");

  // Concurrent deletion
  store_->Delete("user:alice");

  EXPECT_FALSE(overlay.Flush());
  EXPECT_FALSE(store_->Get("user:alice").has_value());
}

TEST_F(AtomicAuthOverlayTest, FlushDetectsConflictOnConcurrentCreate) {
  AtomicAuthOverlay overlay(*store_);
  overlay.Get("user:alice");  // snapshot: doesn't exist
  overlay.Put("user:alice", "our_alice");

  // Someone else creates alice concurrently
  store_->Put("user:alice", "their_alice");

  EXPECT_FALSE(overlay.Flush());
  EXPECT_EQ(store_->Get("user:alice").value(), "their_alice");
}

TEST_F(AtomicAuthOverlayTest, FlushNoConflictOnUnrelatedChange) {
  store_->Put("user:alice", "alice_data");
  store_->Put("user:bob", "bob_data");

  AtomicAuthOverlay overlay(*store_);
  overlay.Get("user:alice");  // only snapshot alice
  overlay.Put("user:alice", "alice_modified");

  // Concurrent modification to bob (not in our read-set)
  store_->Put("user:bob", "bob_modified");

  EXPECT_TRUE(overlay.Flush());
  EXPECT_EQ(store_->Get("user:alice").value(), "alice_modified");
  EXPECT_EQ(store_->Get("user:bob").value(), "bob_modified");
}

// --- Discard (rollback) ---

TEST_F(AtomicAuthOverlayTest, DiscardLeavesBaseUnchanged) {
  store_->Put("user:alice", "original");

  {
    AtomicAuthOverlay overlay(*store_);
    overlay.Put("user:alice", "modified");
    overlay.Put("user:bob", "new_user");
    // overlay goes out of scope without Flush
  }

  EXPECT_EQ(store_->Get("user:alice").value(), "original");
  EXPECT_FALSE(store_->Get("user:bob").has_value());
}

// --- PutAndDeleteMultiple ---

TEST_F(AtomicAuthOverlayTest, PutAndDeleteMultiple) {
  store_->Put("user:alice", "a");
  store_->Put("link:alice", "link_a");

  AtomicAuthOverlay overlay(*store_);
  std::map<std::string, std::string> puts{{"user:alice", "a_updated"}, {"role:admin", "admin_data"}};
  std::vector<std::string> deletes{"link:alice"};
  overlay.PutAndDeleteMultiple(puts, deletes);

  EXPECT_EQ(overlay.Get("user:alice").value(), "a_updated");
  EXPECT_EQ(overlay.Get("role:admin").value(), "admin_data");
  EXPECT_FALSE(overlay.Get("link:alice").has_value());
}
