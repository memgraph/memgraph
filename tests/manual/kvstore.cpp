#include <glog/logging.h>
#include <gtest/gtest.h>

#include "durability/paths.hpp"
#include "storage/kvstore.hpp"
#include "utils/file.hpp"

namespace fs = std::experimental::filesystem;

class KVStore : public ::testing::Test {
 protected:
  virtual void SetUp() { utils::EnsureDir(test_folder_); }

  virtual void TearDown() { fs::remove_all(test_folder_); }

  fs::path test_folder_{fs::path("kvstore_test")};
};

TEST_F(KVStore, PutGet) {
  storage::KVStore kvstore(test_folder_ / "PutGet");
  ASSERT_TRUE(kvstore.Put("key", "value"));
  ASSERT_EQ(kvstore.Get("key").value(), "value");
}

TEST_F(KVStore, PutGetDeleteGet) {
  storage::KVStore kvstore(test_folder_ / "PutGetDeleteGet");
  ASSERT_TRUE(kvstore.Put("key", "value"));
  ASSERT_EQ(kvstore.Get("key").value(), "value");
  ASSERT_TRUE(kvstore.Delete("key"));
  ASSERT_FALSE(static_cast<bool>(kvstore.Get("key")));
}

TEST_F(KVStore, Durability) {
  {
    storage::KVStore kvstore(test_folder_ / "Durability");
    ASSERT_TRUE(kvstore.Put("key", "value"));
  }
  {
    storage::KVStore kvstore(test_folder_ / "Durability");
    ASSERT_EQ(kvstore.Get("key").value(), "value");
  }
}

int main(int argc, char **argv) {
  google::InitGoogleLogging(argv[0]);
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
