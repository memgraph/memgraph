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

#include <algorithm>
#include <array>
#include <map>
#include <optional>
#include <random>
#include <set>
#include <string>
#include <vector>

#include "auth/atomic_auth_overlay.hpp"
#include "auth/repository.hpp"
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

// --- Scans participate in conflict detection ---

namespace {
size_t CountUnder(AtomicAuthOverlay const &overlay, std::string const &prefix) {
  size_t n = 0;
  for (auto it = overlay.begin(prefix), e = overlay.end(prefix); it != e; ++it) ++n;
  return n;
}
}  // namespace

TEST_F(AtomicAuthOverlayTest, FlushDetectsKeyAppearingUnderScannedPrefix) {
  AtomicAuthOverlay overlay(*store_);
  EXPECT_EQ(CountUnder(overlay, "user:"), 0);
  overlay.Put("role:admin", "admin_data");

  store_->Put("user:alice", "their_alice");

  EXPECT_FALSE(overlay.Flush());
  EXPECT_FALSE(store_->Get("role:admin").has_value());
}

// Asking only whether a prefix is inhabited reads no values, so a concurrent change to one must not fail the
// commit. The scan stops at the first key, which is why it must not adopt that key's value on the way past.
TEST_F(AtomicAuthOverlayTest, AnEmptinessOnlyScanToleratesAValueChange) {
  store_->Put("user:alice", "alice_data");

  AtomicAuthOverlay overlay(*store_);
  memgraph::auth::Repository repo{overlay};
  EXPECT_TRUE(repo.HasAnyUser());
  overlay.Put("role:admin", "admin_data");

  store_->Put("user:alice", "alice_modified");

  EXPECT_TRUE(overlay.Flush()) << "a scan that only asked whether the prefix was inhabited never read the value";
  EXPECT_EQ(store_->Get("role:admin"), "admin_data");
}

// The narrowing goes no further than the values: what the scan did conclude is still enforced, so the prefix
// becoming empty underneath it is a conflict.
TEST_F(AtomicAuthOverlayTest, AnEmptinessOnlyScanStillConflictsOnThePrefixEmptying) {
  store_->Put("user:alice", "alice_data");

  AtomicAuthOverlay overlay(*store_);
  memgraph::auth::Repository repo{overlay};
  EXPECT_TRUE(repo.HasAnyUser());
  overlay.Put("role:admin", "admin_data");

  store_->Delete("user:alice");

  EXPECT_FALSE(overlay.Flush()) << "the prefix was inhabited when scanned and is not now";
}

// A later full scan of the same prefix widens what the transaction depends on; it does not replace what the earlier
// scan concluded.
TEST_F(AtomicAuthOverlayTest, ALaterFullScanKeepsAnEarlierEmptinessObservation) {
  store_->Put("user:alice", "alice_data");

  AtomicAuthOverlay overlay(*store_);
  memgraph::auth::Repository repo{overlay};
  EXPECT_TRUE(repo.HasAnyUser());
  overlay.Put("user:bob", "bob_data");

  store_->Delete("user:alice");

  EXPECT_EQ(CountUnder(overlay, "user:"), 1);
  EXPECT_FALSE(overlay.Flush()) << "the prefix was inhabited when first scanned and is not now";
}

TEST_F(AtomicAuthOverlayTest, FlushDetectsModificationOfScannedKey) {
  store_->Put("user:alice", "alice_data");

  AtomicAuthOverlay overlay(*store_);
  EXPECT_EQ(CountUnder(overlay, "user:"), 1);
  overlay.Put("role:admin", "admin_data");

  store_->Put("user:alice", "alice_modified");

  EXPECT_FALSE(overlay.Flush());
}

TEST_F(AtomicAuthOverlayTest, FlushDetectsRemovalOfScannedKey) {
  store_->Put("user:alice", "alice_data");

  AtomicAuthOverlay overlay(*store_);
  EXPECT_EQ(CountUnder(overlay, "user:"), 1);
  overlay.Put("role:admin", "admin_data");

  store_->Delete("user:alice");

  EXPECT_FALSE(overlay.Flush());
}

TEST_F(AtomicAuthOverlayTest, FlushIgnoresChangeOutsideScannedPrefix) {
  AtomicAuthOverlay overlay(*store_);
  EXPECT_EQ(CountUnder(overlay, "user:"), 0);
  overlay.Put("user:alice", "our_alice");

  store_->Put("role:admin", "their_admin");

  EXPECT_TRUE(overlay.Flush());
  EXPECT_EQ(store_->Get("user:alice").value(), "our_alice");
}

// A key a full scan no longer finds is an observation too: gone and recreated with the same bytes, it must still
// fail the commit, since the transaction acted on its absence.
TEST_F(AtomicAuthOverlayTest, AKeyThatVanishesBetweenScansConflictsEvenWhenRecreated) {
  store_->Put("link:u", "with_r");

  AtomicAuthOverlay overlay(*store_);
  EXPECT_EQ(CountUnder(overlay, "link:"), 1);

  store_->Delete("link:u");
  EXPECT_EQ(CountUnder(overlay, "link:"), 0);
  overlay.Delete("role:r");

  store_->Put("link:u", "with_r");

  EXPECT_FALSE(overlay.Flush()) << "the second scan acted on link:u being gone";
}

// Whether a prefix is inhabited is an observation too: a transaction that saw no users, then saw one, acted on
// both answers and must not commit even though the prefix is empty again.
TEST_F(AtomicAuthOverlayTest, APrefixSeenEmptyThenInhabitedConflictsEvenWhenEmptiedAgain) {
  AtomicAuthOverlay overlay(*store_);
  memgraph::auth::Repository repo{overlay};
  EXPECT_EQ(CountUnder(overlay, "user:"), 0);

  store_->Put("user:x", "x_data");
  EXPECT_TRUE(repo.HasAnyUser());
  overlay.Put("user:admin", "admin_data");

  store_->Delete("user:x");

  EXPECT_FALSE(overlay.Flush()) << "the transaction saw the prefix both empty and inhabited";
}

// A key the transaction wrote is not observed by its scans, so another session changing it and changing it back
// conflicts with nothing the transaction saw.
TEST_F(AtomicAuthOverlayTest, AScanDoesNotObserveAKeyTheTransactionWrote) {
  store_->Put("link:u", "with_r");

  AtomicAuthOverlay overlay(*store_);
  overlay.Put("link:u", "ours");

  store_->Put("link:u", "changed");
  EXPECT_EQ(CountUnder(overlay, "link:"), 1);
  store_->Put("link:u", "with_r");

  EXPECT_TRUE(overlay.Flush());
}

// Walking a key again at the value already read is the same observation, not a second one.
TEST_F(AtomicAuthOverlayTest, RescanningAnUnchangedReadKeyDoesNotConflict) {
  store_->Put("link:u", "with_r");

  AtomicAuthOverlay overlay(*store_);
  EXPECT_EQ(overlay.Get("link:u").value(), "with_r");
  EXPECT_EQ(CountUnder(overlay, "link:"), 1);
  overlay.Delete("role:x");

  EXPECT_TRUE(overlay.Flush());
}

// A committed transaction is serialised at its commit: every value it observed, and every key a full scan found
// absent, must equal durable state as Flush finds it. Random interleavings of the transaction's reads, scans and
// writes with concurrent changes check that Flush never accepts otherwise. It may still refuse conservatively.
TEST_F(AtomicAuthOverlayTest, ACommittedTransactionSawOnlyTheStateItCommitsAgainst) {
  std::array<std::string, 2> const keys{"p:a", "p:b"};
  std::mt19937 rng{20261009};
  auto const pick = [&rng](size_t n) { return std::uniform_int_distribution<size_t>{0, n - 1}(rng); };

  for (int round = 0; round < 20000; ++round) {
    std::string start;
    for (auto const &key : keys) {
      store_->Delete(key);
      if (pick(2) != 0) {
        store_->Put(key, std::to_string(pick(2)));
        start += " " + key;
      }
    }

    AtomicAuthOverlay overlay(*store_);
    // The transaction's own writes so far; its observations through them depend on nothing durable.
    std::map<std::string, std::optional<std::string>> written;
    std::vector<std::pair<std::string, std::optional<std::string>>> observed;
    // An emptiness answer is taken over durable state as T's writes stood when it asked.
    std::vector<std::pair<bool, std::map<std::string, std::optional<std::string>>>> observed_inhabited;
    bool changed_concurrently = false;
    std::string history = " base{" + start + " }:";
    for (int step = 0; step < 10; ++step) {
      auto const &key = keys[pick(keys.size())];
      switch (pick(6)) {
        case 0:
          history += " get(" + key + ")";
          if (!written.contains(key)) observed.emplace_back(key, overlay.Get(key));
          break;
        case 1: {
          history += " scan";
          std::set<std::string> found;
          for (auto it = overlay.begin("p:"), e = overlay.end("p:"); it != e; ++it) {
            auto const &[scanned, value] = *it;
            found.insert(scanned);
            if (!written.contains(scanned)) observed.emplace_back(scanned, value);
          }
          for (auto const &absent : keys) {
            if (!found.contains(absent) && !written.contains(absent)) observed.emplace_back(absent, std::nullopt);
          }
          break;
        }
        case 2:
          history += " put(" + key + ")";
          overlay.Put(key, "ours");
          written[key] = "ours";
          break;
        case 3:
          history += " del(" + key + ")";
          overlay.Delete(key);
          written[key] = std::nullopt;
          break;
        case 4: {
          // A scan that stops at the first key and depends only on whether the prefix is inhabited.
          bool const inhabited = overlay.begin("p:") != overlay.end("p:");
          history += inhabited ? " any=yes" : " any=no";
          overlay.ScanDependsOnEmptinessOnly("p:");
          observed_inhabited.emplace_back(inhabited, written);
          break;
        }
        default:
          changed_concurrently = true;
          if (pick(2) != 0) {
            auto const value = std::to_string(pick(2));
            history += " S:put(" + key + "=" + value + ")";
            store_->Put(key, value);
          } else {
            history += " S:del(" + key + ")";
            store_->Delete(key);
          }
      }
    }

    std::map<std::string, std::optional<std::string>> durable;
    for (auto const &key : keys) durable[key] = store_->Get(key);
    if (!overlay.Flush()) {
      ASSERT_TRUE(changed_concurrently) << "round " << round << ": refused with no concurrent change";
      continue;
    }
    for (auto const &[inhabited, writes_then] : observed_inhabited) {
      auto const inhabited_then = std::ranges::any_of(keys, [&](auto const &key) {
        auto const own = writes_then.find(key);
        return own != writes_then.end() ? own->second.has_value() : durable[key].has_value();
      });
      ASSERT_EQ(inhabited, inhabited_then)
          << "round " << round << ": committed after seeing p: " << (inhabited ? "inhabited" : "empty") << ";"
          << history;
    }
    for (auto const &[key, value] : observed) {
      ASSERT_EQ(value, durable[key]) << "round " << round << ": committed after observing " << key
                                     << " in a state durable storage no longer had;" << history;
    }
  }
}

// An emptiness check that steps over keys the transaction deleted answers through the first key left, so that key
// is what it depends on: base staying inhabited by the deleted keys alone must not let the commit through.
TEST_F(AtomicAuthOverlayTest, HasAnyAnsweredThroughOwnTombstoneConflictsWhenItsWitnessGoes) {
  store_->Put("user:alice", "alice_data");
  store_->Put("user:bob", "bob_data");

  AtomicAuthOverlay overlay(*store_);
  memgraph::auth::Repository repo{overlay};
  overlay.Delete("user:alice");
  EXPECT_TRUE(repo.HasAnyUser());
  overlay.Put("user:carol", "carol_data");

  store_->Delete("user:bob");

  EXPECT_FALSE(overlay.Flush()) << "the answer rested on bob, who is gone";
}

TEST_F(AtomicAuthOverlayTest, HasAnyAnsweredThroughOwnTombstoneCommitsWhileItsWitnessStays) {
  store_->Put("user:alice", "alice_data");
  store_->Put("user:bob", "bob_data");

  AtomicAuthOverlay overlay(*store_);
  memgraph::auth::Repository repo{overlay};
  overlay.Delete("user:alice");
  EXPECT_TRUE(repo.HasAnyUser());
  overlay.Put("user:carol", "carol_data");

  EXPECT_TRUE(overlay.Flush());
}

// As WritingAKeyThatAppearedAfterAScanConflicts, but with the prefix already inhabited, so only the key-set check
// can catch bob: a write to a key that appeared after the scan must not exempt that key from it.
TEST_F(AtomicAuthOverlayTest, WritingAKeyThatAppearedUnderAnInhabitedScannedPrefixConflicts) {
  store_->Put("user:alice", "alice_data");

  AtomicAuthOverlay overlay(*store_);
  EXPECT_EQ(CountUnder(overlay, "user:"), 1);

  store_->Put("user:bob", "their_bob");
  overlay.Put("user:bob", "our_bob");

  EXPECT_FALSE(overlay.Flush());
  EXPECT_EQ(store_->Get("user:bob").value(), "their_bob");
}

// A scan that walks a key this transaction already read, and finds a different value, means the transaction acted
// on two states of that key. It must not commit even if the key has since changed back.
TEST_F(AtomicAuthOverlayTest, AScanThatSeesAReadKeyChangedConflictsEvenAfterItChangesBack) {
  store_->Put("link:u", "with_r");

  AtomicAuthOverlay overlay(*store_);
  EXPECT_EQ(overlay.Get("link:u").value(), "with_r");

  store_->Put("link:u", "without_r");
  EXPECT_EQ(CountUnder(overlay, "link:"), 1);
  overlay.Delete("role:r");

  store_->Put("link:u", "with_r");

  EXPECT_FALSE(overlay.Flush()) << "the scan acted on a value the earlier read never saw";
}

// A listing that missed bob, followed by a write to bob, must not commit once bob has appeared: no serial order
// lets the listing miss a user the transaction then changed.
TEST_F(AtomicAuthOverlayTest, WritingAKeyThatAppearedAfterAScanConflicts) {
  AtomicAuthOverlay overlay(*store_);
  EXPECT_EQ(CountUnder(overlay, "user:"), 0);

  store_->Put("user:bob", "their_bob");

  EXPECT_EQ(overlay.Get("user:bob").value(), "their_bob");
  overlay.Put("user:bob", "our_bob");

  EXPECT_FALSE(overlay.Flush());
  EXPECT_EQ(store_->Get("user:bob").value(), "their_bob");
}

TEST_F(AtomicAuthOverlayTest, ScanDoesNotConflictWithOwnWrites) {
  store_->Put("user:alice", "alice_data");

  AtomicAuthOverlay overlay(*store_);
  overlay.Put("user:bob", "bob_data");
  EXPECT_EQ(CountUnder(overlay, "user:"), 2);
  overlay.Put("user:carol", "carol_data");

  EXPECT_TRUE(overlay.Flush());
  EXPECT_EQ(store_->Get("user:bob").value(), "bob_data");
  EXPECT_EQ(store_->Get("user:carol").value(), "carol_data");
}

// Two sessions both find an empty instance and both elevate their user. Without the scanned-prefix check both
// flushes validate, because neither read the key the other wrote, and the instance ends up with two superusers.
TEST_F(AtomicAuthOverlayTest, TwoConcurrentFirstUsersConflict) {
  auto first_user_txn = [this](std::string const &username) {
    auto overlay = std::make_unique<AtomicAuthOverlay>(*store_);
    bool const first_user = CountUnder(*overlay, "user:") == 0;
    EXPECT_TRUE(first_user);
    overlay->Put("user:" + username, first_user ? "superuser" : "ordinary");
    return overlay;
  };

  auto alice = first_user_txn("alice");
  auto bob = first_user_txn("bob");

  EXPECT_TRUE(alice->Flush());
  EXPECT_FALSE(bob->Flush());

  EXPECT_EQ(store_->Get("user:alice").value(), "superuser");
  EXPECT_FALSE(store_->Get("user:bob").has_value());
}
