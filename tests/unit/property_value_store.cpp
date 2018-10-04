#include <unistd.h>
#include <vector>

#include <gflags/gflags.h>
#include <gtest/gtest.h>

#include "storage/common/property_value.hpp"
#include "storage/common/property_value_store.hpp"
#include "utils/file.hpp"

using Location = storage::Location;

namespace fs = std::experimental::filesystem;

DECLARE_string(durability_directory);
DECLARE_string(properties_on_disk);

class PropertyValueStoreTest : public ::testing::Test {
 protected:
  PropertyValueStore props_;

  void SetUp() override {
    // we need this to test the copy constructor
    FLAGS_properties_on_disk = "not empty";

    auto durability_path = fs::temp_directory_path() /
                           ("unit_property_value_store_durability_" +
                            std::to_string(static_cast<int>(getpid())));
    FLAGS_durability_directory = durability_path.string();
    utils::EnsureDir(fs::path(FLAGS_durability_directory));
  }

  void Set(int key, Location location, PropertyValue value) {
    props_.set(storage::Property(key, location), value);
  }

  PropertyValue At(int key, Location location) {
    return props_.at(storage::Property(key, location));
  }

  auto Erase(int key, Location location) {
    return props_.erase(storage::Property(key, location));
  }

  auto Begin() { return props_.begin(); }

  auto End() { return props_.end(); }

  void TearDown() override {
    props_.clear();
    fs::remove_all(fs::path(FLAGS_durability_directory));
  }
};

TEST_F(PropertyValueStoreTest, AtMemory) {
  std::string some_string = "something";

  EXPECT_EQ(PropertyValue(At(0, Location::Memory)).type(),
            PropertyValue::Type::Null);
  Set(0, Location::Memory, some_string);
  EXPECT_EQ(PropertyValue(At(0, Location::Memory)).Value<std::string>(),
            some_string);
  Set(120, Location::Memory, 42);
  EXPECT_EQ(PropertyValue(At(120, Location::Memory)).Value<int64_t>(), 42);
}

TEST_F(PropertyValueStoreTest, AtDisk) {
  std::string some_string = "something";

  EXPECT_EQ(PropertyValue(At(0, Location::Disk)).type(),
            PropertyValue::Type::Null);
  Set(0, Location::Disk, some_string);
  EXPECT_EQ(PropertyValue(At(0, Location::Disk)).Value<std::string>(),
            some_string);
  Set(120, Location::Disk, 42);
  EXPECT_EQ(PropertyValue(At(120, Location::Disk)).Value<int64_t>(), 42);
}

TEST_F(PropertyValueStoreTest, AtNull) {
  EXPECT_EQ(At(0, Location::Memory).type(), PropertyValue::Type::Null);
  EXPECT_EQ(At(100, Location::Memory).type(), PropertyValue::Type::Null);

  EXPECT_EQ(At(0, Location::Disk).type(), PropertyValue::Type::Null);
  EXPECT_EQ(At(100, Location::Disk).type(), PropertyValue::Type::Null);

  // set one prop and test it's not null
  Set(0, Location::Memory, true);
  EXPECT_NE(At(0, Location::Memory).type(), PropertyValue::Type::Null);
  EXPECT_EQ(At(100, Location::Memory).type(), PropertyValue::Type::Null);

  Set(0, Location::Disk, true);
  EXPECT_NE(At(0, Location::Disk).type(), PropertyValue::Type::Null);
  EXPECT_EQ(At(100, Location::Disk).type(), PropertyValue::Type::Null);
}

TEST_F(PropertyValueStoreTest, SetNull) {
  Set(11, Location::Memory, PropertyValue::Null);
  EXPECT_EQ(0, props_.size());

  Set(100, Location::Disk, PropertyValue::Null);
  EXPECT_EQ(0, props_.size());
}

TEST_F(PropertyValueStoreTest, RemoveMemory) {
  // set some props
  Set(11, Location::Memory, "a");
  Set(30, Location::Memory, "b");
  EXPECT_NE(At(11, Location::Memory).type(), PropertyValue::Type::Null);
  EXPECT_NE(At(30, Location::Memory).type(), PropertyValue::Type::Null);
  EXPECT_EQ(props_.size(), 2);

  Erase(11, Location::Memory);
  EXPECT_EQ(props_.size(), 1);
  EXPECT_EQ(At(11, Location::Memory).type(), PropertyValue::Type::Null);

  EXPECT_EQ(Erase(30, Location::Memory), 1);
  EXPECT_EQ(props_.size(), 0);
  EXPECT_EQ(At(30, Location::Memory).type(), PropertyValue::Type::Null);

  EXPECT_EQ(Erase(1000, Location::Memory), 1);
}

TEST_F(PropertyValueStoreTest, RemoveDisk) {
  // set some props
  Set(11, Location::Disk, "a");
  Set(30, Location::Disk, "b");
  EXPECT_NE(At(11, Location::Disk).type(), PropertyValue::Type::Null);
  EXPECT_NE(At(30, Location::Disk).type(), PropertyValue::Type::Null);
  EXPECT_EQ(props_.size(), 2);

  Erase(11, Location::Disk);
  EXPECT_EQ(props_.size(), 1);
  EXPECT_EQ(At(11, Location::Disk).type(), PropertyValue::Type::Null);

  EXPECT_EQ(Erase(30, Location::Disk), 1);
  EXPECT_EQ(props_.size(), 0);
  EXPECT_EQ(At(30, Location::Disk).type(), PropertyValue::Type::Null);

  EXPECT_EQ(Erase(1000, Location::Disk), 1);
}

TEST_F(PropertyValueStoreTest, ClearMemory) {
  EXPECT_EQ(props_.size(), 0);
  Set(11, Location::Memory, "a");
  Set(30, Location::Memory, "b");
  EXPECT_EQ(props_.size(), 2);
}

TEST_F(PropertyValueStoreTest, ClearDisk) {
  EXPECT_EQ(props_.size(), 0);
  Set(11, Location::Disk, "a");
  Set(30, Location::Disk, "b");
  EXPECT_EQ(props_.size(), 2);
}

TEST_F(PropertyValueStoreTest, ReplaceMemory) {
  Set(10, Location::Memory, 42);
  EXPECT_EQ(At(10, Location::Memory).Value<int64_t>(), 42);
  Set(10, Location::Memory, 0.25f);
  EXPECT_EQ(At(10, Location::Memory).type(), PropertyValue::Type::Double);
  EXPECT_FLOAT_EQ(At(10, Location::Memory).Value<double>(), 0.25);
}

TEST_F(PropertyValueStoreTest, ReplaceDisk) {
  Set(10, Location::Disk, 42);
  EXPECT_EQ(At(10, Location::Disk).Value<int64_t>(), 42);
  Set(10, Location::Disk, 0.25f);
  EXPECT_EQ(At(10, Location::Disk).type(), PropertyValue::Type::Double);
  EXPECT_FLOAT_EQ(At(10, Location::Disk).Value<double>(), 0.25);
}

TEST_F(PropertyValueStoreTest, SizeMemory) {
  EXPECT_EQ(props_.size(), 0);

  Set(0, Location::Memory, "something");
  EXPECT_EQ(props_.size(), 1);
  Set(0, Location::Memory, true);
  EXPECT_EQ(props_.size(), 1);
  Set(1, Location::Memory, true);
  EXPECT_EQ(props_.size(), 2);

  for (int i = 0; i < 100; ++i) Set(i + 20, Location::Memory, true);
  EXPECT_EQ(props_.size(), 102);

  Erase(0, Location::Memory);
  EXPECT_EQ(props_.size(), 101);
  Erase(0, Location::Memory);
  EXPECT_EQ(props_.size(), 101);
  Erase(1, Location::Memory);
  EXPECT_EQ(props_.size(), 100);
}

TEST_F(PropertyValueStoreTest, SizeDisk) {
  EXPECT_EQ(props_.size(), 0);

  Set(0, Location::Disk, "something");
  EXPECT_EQ(props_.size(), 1);
  Set(0, Location::Disk, true);
  EXPECT_EQ(props_.size(), 1);
  Set(1, Location::Disk, true);
  EXPECT_EQ(props_.size(), 2);

  for (int i = 0; i < 100; ++i) Set(i + 20, Location::Disk, true);
  EXPECT_EQ(props_.size(), 102);

  Erase(0, Location::Disk);
  EXPECT_EQ(props_.size(), 101);
  Erase(0, Location::Disk);
  EXPECT_EQ(props_.size(), 101);
  Erase(1, Location::Disk);
  EXPECT_EQ(props_.size(), 100);
}

TEST_F(PropertyValueStoreTest, Size) {
  EXPECT_EQ(props_.size(), 0);

  for (int i = 0; i < 100; ++i) Set(i, Location::Disk, true);
  EXPECT_EQ(props_.size(), 100);

  for (int i = 0; i < 200; ++i) Set(i + 100, Location::Memory, true);
  EXPECT_EQ(props_.size(), 300);

  Erase(0, Location::Disk);
  EXPECT_EQ(props_.size(), 299);
  Erase(99, Location::Disk);
  EXPECT_EQ(props_.size(), 298);
  Erase(100, Location::Memory);
  EXPECT_EQ(props_.size(), 297);
  Erase(299, Location::Memory);
  EXPECT_EQ(props_.size(), 296);
}

TEST_F(PropertyValueStoreTest, InsertRetrieveListMemory) {
  Set(0, Location::Memory, std::vector<PropertyValue>{1, true, 2.5, "something",
                                                      PropertyValue::Null});
  auto p = At(0, Location::Memory);

  EXPECT_EQ(p.type(), PropertyValue::Type::List);
  auto l = p.Value<std::vector<PropertyValue>>();
  EXPECT_EQ(l.size(), 5);
  EXPECT_EQ(l[0].type(), PropertyValue::Type::Int);
  EXPECT_EQ(l[0].Value<int64_t>(), 1);
  EXPECT_EQ(l[1].type(), PropertyValue::Type::Bool);
  EXPECT_EQ(l[1].Value<bool>(), true);
  EXPECT_EQ(l[2].type(), PropertyValue::Type::Double);
  EXPECT_EQ(l[2].Value<double>(), 2.5);
  EXPECT_EQ(l[3].type(), PropertyValue::Type::String);
  EXPECT_EQ(l[3].Value<std::string>(), "something");
  EXPECT_EQ(l[4].type(), PropertyValue::Type::Null);
}

TEST_F(PropertyValueStoreTest, InsertRetrieveListDisk) {
  Set(0, Location::Disk, std::vector<PropertyValue>{1, true, 2.5, "something",
                                                    PropertyValue::Null});
  auto p = At(0, Location::Disk);

  EXPECT_EQ(p.type(), PropertyValue::Type::List);
  auto l = p.Value<std::vector<PropertyValue>>();
  EXPECT_EQ(l.size(), 5);
  EXPECT_EQ(l[0].type(), PropertyValue::Type::Int);
  EXPECT_EQ(l[0].Value<int64_t>(), 1);
  EXPECT_EQ(l[1].type(), PropertyValue::Type::Bool);
  EXPECT_EQ(l[1].Value<bool>(), true);
  EXPECT_EQ(l[2].type(), PropertyValue::Type::Double);
  EXPECT_EQ(l[2].Value<double>(), 2.5);
  EXPECT_EQ(l[3].type(), PropertyValue::Type::String);
  EXPECT_EQ(l[3].Value<std::string>(), "something");
  EXPECT_EQ(l[4].type(), PropertyValue::Type::Null);
}

TEST_F(PropertyValueStoreTest, InsertRetrieveMap) {
  Set(0, Location::Memory, std::map<std::string, PropertyValue>{
                               {"a", 1}, {"b", true}, {"c", "something"}});

  auto p = At(0, Location::Memory);
  EXPECT_EQ(p.type(), PropertyValue::Type::Map);
  auto m = p.Value<std::map<std::string, PropertyValue>>();
  EXPECT_EQ(m.size(), 3);
  auto get = [&m](const std::string &prop_name) {
    return m.find(prop_name)->second;
  };
  EXPECT_EQ(get("a").type(), PropertyValue::Type::Int);
  EXPECT_EQ(get("a").Value<int64_t>(), 1);
  EXPECT_EQ(get("b").type(), PropertyValue::Type::Bool);
  EXPECT_EQ(get("b").Value<bool>(), true);
  EXPECT_EQ(get("c").type(), PropertyValue::Type::String);
  EXPECT_EQ(get("c").Value<std::string>(), "something");
}

TEST_F(PropertyValueStoreTest, InsertRetrieveMapDisk) {
  Set(0, Location::Disk, std::map<std::string, PropertyValue>{
                             {"a", 1}, {"b", true}, {"c", "something"}});

  auto p = At(0, Location::Disk);
  EXPECT_EQ(p.type(), PropertyValue::Type::Map);
  auto m = p.Value<std::map<std::string, PropertyValue>>();
  EXPECT_EQ(m.size(), 3);
  auto get = [&m](const std::string &prop_name) {
    return m.find(prop_name)->second;
  };
  EXPECT_EQ(get("a").type(), PropertyValue::Type::Int);
  EXPECT_EQ(get("a").Value<int64_t>(), 1);
  EXPECT_EQ(get("b").type(), PropertyValue::Type::Bool);
  EXPECT_EQ(get("b").Value<bool>(), true);
  EXPECT_EQ(get("c").type(), PropertyValue::Type::String);
  EXPECT_EQ(get("c").Value<std::string>(), "something");
}

TEST_F(PropertyValueStoreTest, Iterator) {
  Set(0, Location::Memory, "a");
  Set(1, Location::Memory, 1);
  Set(2, Location::Disk, "b");
  Set(3, Location::Disk, 2);

  auto it = Begin();
  ASSERT_TRUE(it != End());
  EXPECT_EQ(it->first.Id(), 0);
  EXPECT_EQ((*it).second.Value<std::string>(), "a");

  ++it;
  ASSERT_TRUE(it != End());
  EXPECT_EQ((*it).first.Id(), 1);
  EXPECT_EQ(it->second.Value<int64_t>(), 1);

  ++it;
  ASSERT_TRUE(it != End());
  EXPECT_EQ(it->first.Id(), 2);
  EXPECT_EQ((*it).second.Value<std::string>(), "b");

  ++it;
  ASSERT_TRUE(it != End());
  EXPECT_EQ((*it).first.Id(), 3);
  EXPECT_EQ(it->second.Value<int64_t>(), 2);

  ++it;
  ASSERT_TRUE(it == End());
}

TEST_F(PropertyValueStoreTest, CopyConstructor) {
  PropertyValueStore props;
  for (int i = 1; i <= 3; ++i)
    props.set(storage::Property(i, Location::Memory),
              "mem_" + std::to_string(i));
  for (int i = 4; i <= 5; ++i)
    props.set(storage::Property(i, Location::Disk),
              "disk_" + std::to_string(i));

  PropertyValueStore new_props = props;
  for (int i = 1; i <= 3; ++i)
    EXPECT_EQ(new_props.at(storage::Property(i, Location::Memory))
                  .Value<std::string>(),
              "mem_" + std::to_string(i));
  for (int i = 4; i <= 5; ++i)
    EXPECT_EQ(
        new_props.at(storage::Property(i, Location::Disk)).Value<std::string>(),
        "disk_" + std::to_string(i));

  props.set(storage::Property(1, Location::Memory), "mem_1_update");
  EXPECT_EQ(
      new_props.at(storage::Property(1, Location::Memory)).Value<std::string>(),
      "mem_1");

  new_props.set(storage::Property(2, Location::Memory), "mem_2_update");
  EXPECT_EQ(
      props.at(storage::Property(2, Location::Memory)).Value<std::string>(),
      "mem_2");

  props.set(storage::Property(4, Location::Disk), "disk_4_update");
  EXPECT_EQ(
      new_props.at(storage::Property(4, Location::Disk)).Value<std::string>(),
      "disk_4");

  new_props.set(storage::Property(5, Location::Disk), "disk_5_update");
  EXPECT_EQ(props.at(storage::Property(5, Location::Disk)).Value<std::string>(),
            "disk_5");
}
