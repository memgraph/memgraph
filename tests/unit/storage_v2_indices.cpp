#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "storage/v2/storage.hpp"

// NOLINTNEXTLINE(google-build-using-namespace)
using namespace storage;

using testing::IsEmpty;
using testing::UnorderedElementsAre;

// NOLINTNEXTLINE(cppcoreguidelines-macro-usage)
#define ASSERT_NO_ERROR(result) ASSERT_FALSE((result).HasError())

class IndexTest : public testing::Test {
 protected:
  void SetUp() override {
    auto acc = storage.Access();
    prop_id = acc.NameToProperty("id");
    prop_val = acc.NameToProperty("val");
    label1 = acc.NameToLabel("label1");
    label2 = acc.NameToLabel("label2");
    vertex_id = 0;
  }

  Storage storage;
  PropertyId prop_id;
  PropertyId prop_val;
  LabelId label1;
  LabelId label2;

  VertexAccessor CreateVertex(Storage::Accessor *accessor) {
    VertexAccessor vertex = accessor->CreateVertex();
    CHECK(!vertex.SetProperty(prop_id, PropertyValue(vertex_id++)).HasError());
    return vertex;
  }

  template <class TIterable>
  std::vector<int64_t> GetIds(TIterable iterable, View view = View::OLD) {
    std::vector<int64_t> ret;
    for (auto vertex : iterable) {
      ret.push_back(vertex.GetProperty(prop_id, view)->ValueInt());
    }
    return ret;
  }

 private:
  int vertex_id;
};

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST_F(IndexTest, LabelIndexCreate) {
  {
    auto acc = storage.Access();
    EXPECT_FALSE(acc.LabelIndexExists(label1));
  }
  EXPECT_EQ(storage.ListAllIndices().label.size(), 0);

  {
    auto acc = storage.Access();
    for (int i = 0; i < 10; ++i) {
      auto vertex = CreateVertex(&acc);
      ASSERT_NO_ERROR(vertex.AddLabel(i % 2 ? label1 : label2));
    }
    ASSERT_NO_ERROR(acc.Commit());
  }

  EXPECT_TRUE(storage.CreateIndex(label1));

  {
    auto acc = storage.Access();
    EXPECT_THAT(GetIds(acc.Vertices(label1, View::OLD), View::OLD),
                UnorderedElementsAre(1, 3, 5, 7, 9));
    EXPECT_THAT(GetIds(acc.Vertices(label1, View::NEW), View::NEW),
                UnorderedElementsAre(1, 3, 5, 7, 9));
  }

  {
    auto acc = storage.Access();
    for (int i = 10; i < 20; ++i) {
      auto vertex = CreateVertex(&acc);
      ASSERT_NO_ERROR(vertex.AddLabel(i % 2 ? label1 : label2));
    }

    EXPECT_THAT(GetIds(acc.Vertices(label1, View::OLD), View::OLD),
                UnorderedElementsAre(1, 3, 5, 7, 9));
    EXPECT_THAT(GetIds(acc.Vertices(label1, View::NEW), View::NEW),
                UnorderedElementsAre(1, 3, 5, 7, 9, 11, 13, 15, 17, 19));

    acc.AdvanceCommand();

    EXPECT_THAT(GetIds(acc.Vertices(label1, View::OLD), View::NEW),
                UnorderedElementsAre(1, 3, 5, 7, 9, 11, 13, 15, 17, 19));
    EXPECT_THAT(GetIds(acc.Vertices(label1, View::NEW), View::NEW),
                UnorderedElementsAre(1, 3, 5, 7, 9, 11, 13, 15, 17, 19));

    acc.Abort();
  }

  {
    auto acc = storage.Access();
    for (int i = 10; i < 20; ++i) {
      auto vertex = CreateVertex(&acc);
      ASSERT_NO_ERROR(vertex.AddLabel(i % 2 ? label1 : label2));
    }

    EXPECT_THAT(GetIds(acc.Vertices(label1, View::OLD), View::OLD),
                UnorderedElementsAre(1, 3, 5, 7, 9));
    EXPECT_THAT(GetIds(acc.Vertices(label1, View::NEW), View::NEW),
                UnorderedElementsAre(1, 3, 5, 7, 9, 21, 23, 25, 27, 29));

    acc.AdvanceCommand();

    EXPECT_THAT(GetIds(acc.Vertices(label1, View::OLD), View::NEW),
                UnorderedElementsAre(1, 3, 5, 7, 9, 21, 23, 25, 27, 29));
    EXPECT_THAT(GetIds(acc.Vertices(label1, View::NEW), View::NEW),
                UnorderedElementsAre(1, 3, 5, 7, 9, 21, 23, 25, 27, 29));

    ASSERT_NO_ERROR(acc.Commit());
  }

  {
    auto acc = storage.Access();
    EXPECT_THAT(GetIds(acc.Vertices(label1, View::OLD), View::OLD),
                UnorderedElementsAre(1, 3, 5, 7, 9, 21, 23, 25, 27, 29));
    EXPECT_THAT(GetIds(acc.Vertices(label1, View::NEW), View::NEW),
                UnorderedElementsAre(1, 3, 5, 7, 9, 21, 23, 25, 27, 29));

    acc.AdvanceCommand();

    EXPECT_THAT(GetIds(acc.Vertices(label1, View::OLD), View::NEW),
                UnorderedElementsAre(1, 3, 5, 7, 9, 21, 23, 25, 27, 29));
    EXPECT_THAT(GetIds(acc.Vertices(label1, View::NEW), View::NEW),
                UnorderedElementsAre(1, 3, 5, 7, 9, 21, 23, 25, 27, 29));

    ASSERT_NO_ERROR(acc.Commit());
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST_F(IndexTest, LabelIndexDrop) {
  {
    auto acc = storage.Access();
    EXPECT_FALSE(acc.LabelIndexExists(label1));
  }
  EXPECT_EQ(storage.ListAllIndices().label.size(), 0);

  {
    auto acc = storage.Access();
    for (int i = 0; i < 10; ++i) {
      auto vertex = CreateVertex(&acc);
      ASSERT_NO_ERROR(vertex.AddLabel(i % 2 ? label1 : label2));
    }
    ASSERT_NO_ERROR(acc.Commit());
  }

  EXPECT_TRUE(storage.CreateIndex(label1));

  {
    auto acc = storage.Access();
    EXPECT_THAT(GetIds(acc.Vertices(label1, View::OLD), View::OLD),
                UnorderedElementsAre(1, 3, 5, 7, 9));
    EXPECT_THAT(GetIds(acc.Vertices(label1, View::NEW), View::NEW),
                UnorderedElementsAre(1, 3, 5, 7, 9));
  }

  EXPECT_TRUE(storage.DropIndex(label1));
  {
    auto acc = storage.Access();
    EXPECT_FALSE(acc.LabelIndexExists(label1));
  }
  EXPECT_EQ(storage.ListAllIndices().label.size(), 0);

  EXPECT_FALSE(storage.DropIndex(label1));
  {
    auto acc = storage.Access();
    EXPECT_FALSE(acc.LabelIndexExists(label1));
  }
  EXPECT_EQ(storage.ListAllIndices().label.size(), 0);

  {
    auto acc = storage.Access();
    for (int i = 10; i < 20; ++i) {
      auto vertex = CreateVertex(&acc);
      ASSERT_NO_ERROR(vertex.AddLabel(i % 2 ? label1 : label2));
    }
    ASSERT_NO_ERROR(acc.Commit());
  }

  EXPECT_TRUE(storage.CreateIndex(label1));
  {
    auto acc = storage.Access();
    EXPECT_TRUE(acc.LabelIndexExists(label1));
  }
  EXPECT_THAT(storage.ListAllIndices().label, UnorderedElementsAre(label1));

  {
    auto acc = storage.Access();

    EXPECT_THAT(GetIds(acc.Vertices(label1, View::OLD), View::OLD),
                UnorderedElementsAre(1, 3, 5, 7, 9, 11, 13, 15, 17, 19));
    EXPECT_THAT(GetIds(acc.Vertices(label1, View::NEW), View::NEW),
                UnorderedElementsAre(1, 3, 5, 7, 9, 11, 13, 15, 17, 19));

    acc.AdvanceCommand();

    EXPECT_THAT(GetIds(acc.Vertices(label1, View::OLD), View::NEW),
                UnorderedElementsAre(1, 3, 5, 7, 9, 11, 13, 15, 17, 19));
    EXPECT_THAT(GetIds(acc.Vertices(label1, View::NEW), View::NEW),
                UnorderedElementsAre(1, 3, 5, 7, 9, 11, 13, 15, 17, 19));
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST_F(IndexTest, LabelIndexBasic) {
  // The following steps are performed and index correctness is validated after
  // each step:
  // 1. Create 10 vertices numbered from 0 to 9.
  // 2. Add Label1 to odd numbered, and Label2 to even numbered vertices.
  // 3. Remove Label1 from odd numbered vertices, and add it to even numbered
  //    vertices.
  // 4. Delete even numbered vertices.
  EXPECT_TRUE(storage.CreateIndex(label1));
  EXPECT_TRUE(storage.CreateIndex(label2));

  auto acc = storage.Access();
  EXPECT_THAT(storage.ListAllIndices().label,
              UnorderedElementsAre(label1, label2));
  EXPECT_THAT(GetIds(acc.Vertices(label1, View::OLD), View::OLD), IsEmpty());
  EXPECT_THAT(GetIds(acc.Vertices(label2, View::OLD), View::OLD), IsEmpty());
  EXPECT_THAT(GetIds(acc.Vertices(label1, View::NEW), View::NEW), IsEmpty());
  EXPECT_THAT(GetIds(acc.Vertices(label2, View::NEW), View::NEW), IsEmpty());

  for (int i = 0; i < 10; ++i) {
    auto vertex = CreateVertex(&acc);
    ASSERT_NO_ERROR(vertex.AddLabel(i % 2 ? label1 : label2));
  }

  EXPECT_THAT(GetIds(acc.Vertices(label1, View::OLD), View::OLD), IsEmpty());
  EXPECT_THAT(GetIds(acc.Vertices(label2, View::OLD), View::OLD), IsEmpty());
  EXPECT_THAT(GetIds(acc.Vertices(label1, View::NEW), View::NEW),
              UnorderedElementsAre(1, 3, 5, 7, 9));
  EXPECT_THAT(GetIds(acc.Vertices(label2, View::NEW), View::NEW),
              UnorderedElementsAre(0, 2, 4, 6, 8));

  acc.AdvanceCommand();
  EXPECT_THAT(GetIds(acc.Vertices(label1, View::OLD), View::OLD),
              UnorderedElementsAre(1, 3, 5, 7, 9));
  EXPECT_THAT(GetIds(acc.Vertices(label2, View::OLD), View::OLD),
              UnorderedElementsAre(0, 2, 4, 6, 8));
  EXPECT_THAT(GetIds(acc.Vertices(label1, View::NEW), View::NEW),
              UnorderedElementsAre(1, 3, 5, 7, 9));
  EXPECT_THAT(GetIds(acc.Vertices(label2, View::NEW), View::NEW),
              UnorderedElementsAre(0, 2, 4, 6, 8));

  for (auto vertex : acc.Vertices(View::OLD)) {
    int64_t id = vertex.GetProperty(prop_id, View::OLD)->ValueInt();
    if (id % 2) {
      ASSERT_NO_ERROR(vertex.RemoveLabel(label1));
    } else {
      ASSERT_NO_ERROR(vertex.AddLabel(label1));
    }
  }

  EXPECT_THAT(GetIds(acc.Vertices(label1, View::OLD), View::OLD),
              UnorderedElementsAre(1, 3, 5, 7, 9));
  EXPECT_THAT(GetIds(acc.Vertices(label2, View::OLD), View::OLD),
              UnorderedElementsAre(0, 2, 4, 6, 8));
  EXPECT_THAT(GetIds(acc.Vertices(label1, View::NEW), View::NEW),
              UnorderedElementsAre(0, 2, 4, 6, 8));
  EXPECT_THAT(GetIds(acc.Vertices(label2, View::NEW), View::NEW),
              UnorderedElementsAre(0, 2, 4, 6, 8));

  for (auto vertex : acc.Vertices(View::OLD)) {
    int64_t id = vertex.GetProperty(prop_id, View::OLD)->ValueInt();
    if (id % 2 == 0) {
      ASSERT_NO_ERROR(acc.DeleteVertex(&vertex));
    }
  }

  EXPECT_THAT(GetIds(acc.Vertices(label1, View::OLD), View::OLD),
              UnorderedElementsAre(1, 3, 5, 7, 9));
  EXPECT_THAT(GetIds(acc.Vertices(label2, View::OLD), View::OLD),
              UnorderedElementsAre(0, 2, 4, 6, 8));
  EXPECT_THAT(GetIds(acc.Vertices(label1, View::NEW), View::NEW), IsEmpty());
  EXPECT_THAT(GetIds(acc.Vertices(label2, View::NEW), View::NEW), IsEmpty());

  acc.AdvanceCommand();

  EXPECT_THAT(GetIds(acc.Vertices(label1, View::OLD), View::OLD), IsEmpty());
  EXPECT_THAT(GetIds(acc.Vertices(label2, View::OLD), View::OLD), IsEmpty());
  EXPECT_THAT(GetIds(acc.Vertices(label1, View::NEW), View::NEW), IsEmpty());
  EXPECT_THAT(GetIds(acc.Vertices(label2, View::NEW), View::NEW), IsEmpty());
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST_F(IndexTest, LabelIndexDuplicateVersions) {
  // By removing labels and adding them again we create duplicate entries for
  // the same vertex in the index (they only differ by the timestamp). This test
  // checks that duplicates are properly filtered out.
  EXPECT_TRUE(storage.CreateIndex(label1));
  EXPECT_TRUE(storage.CreateIndex(label2));

  {
    auto acc = storage.Access();
    for (int i = 0; i < 5; ++i) {
      auto vertex = CreateVertex(&acc);
      ASSERT_NO_ERROR(vertex.AddLabel(label1));
    }

    EXPECT_THAT(GetIds(acc.Vertices(label1, View::NEW), View::NEW),
                UnorderedElementsAre(0, 1, 2, 3, 4));

    ASSERT_NO_ERROR(acc.Commit());
  }

  {
    auto acc = storage.Access();
    EXPECT_THAT(GetIds(acc.Vertices(label1, View::OLD), View::OLD),
                UnorderedElementsAre(0, 1, 2, 3, 4));

    for (auto vertex : acc.Vertices(View::OLD)) {
      ASSERT_NO_ERROR(vertex.RemoveLabel(label1));
    }

    EXPECT_THAT(GetIds(acc.Vertices(label1, View::OLD), View::OLD),
                UnorderedElementsAre(0, 1, 2, 3, 4));
    EXPECT_THAT(GetIds(acc.Vertices(label1, View::NEW), View::NEW), IsEmpty());

    for (auto vertex : acc.Vertices(View::OLD)) {
      ASSERT_NO_ERROR(vertex.AddLabel(label1));
    }
    EXPECT_THAT(GetIds(acc.Vertices(label1, View::OLD), View::OLD),
                UnorderedElementsAre(0, 1, 2, 3, 4));
    EXPECT_THAT(GetIds(acc.Vertices(label1, View::NEW), View::NEW),
                UnorderedElementsAre(0, 1, 2, 3, 4));
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST_F(IndexTest, LabelIndexTransactionalIsolation) {
  // Check that transactions only see entries they are supposed to see.
  EXPECT_TRUE(storage.CreateIndex(label1));
  EXPECT_TRUE(storage.CreateIndex(label2));

  auto acc_before = storage.Access();
  auto acc = storage.Access();
  auto acc_after = storage.Access();

  for (int i = 0; i < 5; ++i) {
    auto vertex = CreateVertex(&acc);
    ASSERT_NO_ERROR(vertex.AddLabel(label1));
  }

  EXPECT_THAT(GetIds(acc.Vertices(label1, View::NEW), View::NEW),
              UnorderedElementsAre(0, 1, 2, 3, 4));
  EXPECT_THAT(GetIds(acc_before.Vertices(label1, View::NEW), View::NEW),
              IsEmpty());
  EXPECT_THAT(GetIds(acc_after.Vertices(label1, View::NEW), View::NEW),
              IsEmpty());

  ASSERT_NO_ERROR(acc.Commit());

  auto acc_after_commit = storage.Access();

  EXPECT_THAT(GetIds(acc_before.Vertices(label1, View::NEW), View::NEW),
              IsEmpty());
  EXPECT_THAT(GetIds(acc_after.Vertices(label1, View::NEW), View::NEW),
              IsEmpty());
  EXPECT_THAT(GetIds(acc_after_commit.Vertices(label1, View::NEW), View::NEW),
              UnorderedElementsAre(0, 1, 2, 3, 4));
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST_F(IndexTest, LabelIndexCountEstimate) {
  EXPECT_TRUE(storage.CreateIndex(label1));
  EXPECT_TRUE(storage.CreateIndex(label2));

  auto acc = storage.Access();
  for (int i = 0; i < 20; ++i) {
    auto vertex = CreateVertex(&acc);
    ASSERT_NO_ERROR(vertex.AddLabel(i % 3 ? label1 : label2));
  }

  EXPECT_EQ(acc.ApproximateVertexCount(label1), 13);
  EXPECT_EQ(acc.ApproximateVertexCount(label2), 7);
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST_F(IndexTest, LabelPropertyIndexCreateAndDrop) {
  EXPECT_EQ(storage.ListAllIndices().label_property.size(), 0);
  EXPECT_TRUE(storage.CreateIndex(label1, prop_id));
  {
    auto acc = storage.Access();
    EXPECT_TRUE(acc.LabelPropertyIndexExists(label1, prop_id));
  }
  EXPECT_THAT(storage.ListAllIndices().label_property,
              UnorderedElementsAre(std::make_pair(label1, prop_id)));
  {
    auto acc = storage.Access();
    EXPECT_FALSE(acc.LabelPropertyIndexExists(label2, prop_id));
  }
  EXPECT_FALSE(storage.CreateIndex(label1, prop_id));
  EXPECT_THAT(storage.ListAllIndices().label_property,
              UnorderedElementsAre(std::make_pair(label1, prop_id)));

  EXPECT_TRUE(storage.CreateIndex(label2, prop_id));
  {
    auto acc = storage.Access();
    EXPECT_TRUE(acc.LabelPropertyIndexExists(label2, prop_id));
  }
  EXPECT_THAT(storage.ListAllIndices().label_property,
              UnorderedElementsAre(std::make_pair(label1, prop_id),
                                   std::make_pair(label2, prop_id)));

  EXPECT_TRUE(storage.DropIndex(label1, prop_id));
  {
    auto acc = storage.Access();
    EXPECT_FALSE(acc.LabelPropertyIndexExists(label1, prop_id));
  }
  EXPECT_THAT(storage.ListAllIndices().label_property,
              UnorderedElementsAre(std::make_pair(label2, prop_id)));
  EXPECT_FALSE(storage.DropIndex(label1, prop_id));

  EXPECT_TRUE(storage.DropIndex(label2, prop_id));
  {
    auto acc = storage.Access();
    EXPECT_FALSE(acc.LabelPropertyIndexExists(label2, prop_id));
  }
  EXPECT_EQ(storage.ListAllIndices().label_property.size(), 0);
}

// The following three tests are almost an exact copy-paste of the corresponding
// label index tests. We request all vertices with given label and property from
// the index, without range filtering. Range filtering is tested in a separate
// test.

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST_F(IndexTest, LabelPropertyIndexBasic) {
  storage.CreateIndex(label1, prop_val);
  storage.CreateIndex(label2, prop_val);

  auto acc = storage.Access();
  EXPECT_THAT(GetIds(acc.Vertices(label1, prop_val, View::OLD), View::OLD),
              IsEmpty());

  for (int i = 0; i < 10; ++i) {
    auto vertex = CreateVertex(&acc);
    ASSERT_NO_ERROR(vertex.AddLabel(i % 2 ? label1 : label2));
    ASSERT_NO_ERROR(vertex.SetProperty(prop_val, PropertyValue(i)));
  }

  EXPECT_THAT(GetIds(acc.Vertices(label1, prop_val, View::OLD), View::OLD),
              IsEmpty());
  EXPECT_THAT(GetIds(acc.Vertices(label2, prop_val, View::OLD), View::OLD),
              IsEmpty());
  EXPECT_THAT(GetIds(acc.Vertices(label1, prop_val, View::NEW), View::NEW),
              UnorderedElementsAre(1, 3, 5, 7, 9));
  EXPECT_THAT(GetIds(acc.Vertices(label2, prop_val, View::NEW), View::NEW),
              UnorderedElementsAre(0, 2, 4, 6, 8));

  acc.AdvanceCommand();

  EXPECT_THAT(GetIds(acc.Vertices(label1, prop_val, View::OLD), View::OLD),
              UnorderedElementsAre(1, 3, 5, 7, 9));
  EXPECT_THAT(GetIds(acc.Vertices(label2, prop_val, View::OLD), View::OLD),
              UnorderedElementsAre(0, 2, 4, 6, 8));
  EXPECT_THAT(GetIds(acc.Vertices(label1, prop_val, View::NEW), View::NEW),
              UnorderedElementsAre(1, 3, 5, 7, 9));
  EXPECT_THAT(GetIds(acc.Vertices(label2, prop_val, View::NEW), View::NEW),
              UnorderedElementsAre(0, 2, 4, 6, 8));

  for (auto vertex : acc.Vertices(View::OLD)) {
    int64_t id = vertex.GetProperty(prop_id, View::OLD)->ValueInt();
    if (id % 2) {
      ASSERT_NO_ERROR(vertex.SetProperty(prop_val, PropertyValue()));
    } else {
      ASSERT_NO_ERROR(vertex.AddLabel(label1));
    }
  }

  EXPECT_THAT(GetIds(acc.Vertices(label1, prop_val, View::OLD), View::OLD),
              UnorderedElementsAre(1, 3, 5, 7, 9));
  EXPECT_THAT(GetIds(acc.Vertices(label2, prop_val, View::OLD), View::OLD),
              UnorderedElementsAre(0, 2, 4, 6, 8));
  EXPECT_THAT(GetIds(acc.Vertices(label1, prop_val, View::NEW), View::NEW),
              UnorderedElementsAre(0, 2, 4, 6, 8));
  EXPECT_THAT(GetIds(acc.Vertices(label2, prop_val, View::NEW), View::NEW),
              UnorderedElementsAre(0, 2, 4, 6, 8));

  for (auto vertex : acc.Vertices(View::OLD)) {
    int64_t id = vertex.GetProperty(prop_id, View::OLD)->ValueInt();
    if (id % 2 == 0) {
      ASSERT_NO_ERROR(acc.DeleteVertex(&vertex));
    }
  }

  EXPECT_THAT(GetIds(acc.Vertices(label1, prop_val, View::OLD), View::OLD),
              UnorderedElementsAre(1, 3, 5, 7, 9));
  EXPECT_THAT(GetIds(acc.Vertices(label2, prop_val, View::OLD), View::OLD),
              UnorderedElementsAre(0, 2, 4, 6, 8));
  EXPECT_THAT(GetIds(acc.Vertices(label1, prop_val, View::NEW), View::NEW),
              IsEmpty());
  EXPECT_THAT(GetIds(acc.Vertices(label2, prop_val, View::NEW), View::NEW),
              IsEmpty());

  acc.AdvanceCommand();

  EXPECT_THAT(GetIds(acc.Vertices(label1, prop_val, View::OLD), View::OLD),
              IsEmpty());
  EXPECT_THAT(GetIds(acc.Vertices(label2, prop_val, View::OLD), View::OLD),
              IsEmpty());
  EXPECT_THAT(GetIds(acc.Vertices(label1, prop_val, View::NEW), View::NEW),
              IsEmpty());
  EXPECT_THAT(GetIds(acc.Vertices(label2, prop_val, View::NEW), View::NEW),
              IsEmpty());
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST_F(IndexTest, LabelPropertyIndexDuplicateVersions) {
  storage.CreateIndex(label1, prop_val);
  {
    auto acc = storage.Access();
    for (int i = 0; i < 5; ++i) {
      auto vertex = CreateVertex(&acc);
      ASSERT_NO_ERROR(vertex.AddLabel(label1));
      ASSERT_NO_ERROR(vertex.SetProperty(prop_val, PropertyValue(i)));
    }

    EXPECT_THAT(GetIds(acc.Vertices(label1, prop_val, View::NEW), View::NEW),
                UnorderedElementsAre(0, 1, 2, 3, 4));

    ASSERT_NO_ERROR(acc.Commit());
  }

  {
    auto acc = storage.Access();
    EXPECT_THAT(GetIds(acc.Vertices(label1, prop_val, View::OLD), View::OLD),
                UnorderedElementsAre(0, 1, 2, 3, 4));

    for (auto vertex : acc.Vertices(View::OLD)) {
      ASSERT_NO_ERROR(vertex.SetProperty(prop_val, PropertyValue()));
    }

    EXPECT_THAT(GetIds(acc.Vertices(label1, prop_val, View::OLD), View::OLD),
                UnorderedElementsAre(0, 1, 2, 3, 4));
    EXPECT_THAT(GetIds(acc.Vertices(label1, prop_val, View::NEW), View::NEW),
                IsEmpty());

    for (auto vertex : acc.Vertices(View::OLD)) {
      ASSERT_NO_ERROR(vertex.SetProperty(prop_val, PropertyValue(42)));
    }
    EXPECT_THAT(GetIds(acc.Vertices(label1, prop_val, View::OLD), View::OLD),
                UnorderedElementsAre(0, 1, 2, 3, 4));
    EXPECT_THAT(GetIds(acc.Vertices(label1, prop_val, View::NEW), View::NEW),
                UnorderedElementsAre(0, 1, 2, 3, 4));
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST_F(IndexTest, LabelPropertyIndexTransactionalIsolation) {
  storage.CreateIndex(label1, prop_val);

  auto acc_before = storage.Access();
  auto acc = storage.Access();
  auto acc_after = storage.Access();

  for (int i = 0; i < 5; ++i) {
    auto vertex = CreateVertex(&acc);
    ASSERT_NO_ERROR(vertex.AddLabel(label1));
    ASSERT_NO_ERROR(vertex.SetProperty(prop_val, PropertyValue(i)));
  }

  EXPECT_THAT(GetIds(acc.Vertices(label1, prop_val, View::NEW), View::NEW),
              UnorderedElementsAre(0, 1, 2, 3, 4));
  EXPECT_THAT(
      GetIds(acc_before.Vertices(label1, prop_val, View::NEW), View::NEW),
      IsEmpty());
  EXPECT_THAT(
      GetIds(acc_after.Vertices(label1, prop_val, View::NEW), View::NEW),
      IsEmpty());

  ASSERT_NO_ERROR(acc.Commit());

  auto acc_after_commit = storage.Access();

  EXPECT_THAT(
      GetIds(acc_before.Vertices(label1, prop_val, View::NEW), View::NEW),
      IsEmpty());
  EXPECT_THAT(
      GetIds(acc_after.Vertices(label1, prop_val, View::NEW), View::NEW),
      IsEmpty());
  EXPECT_THAT(
      GetIds(acc_after_commit.Vertices(label1, prop_val, View::NEW), View::NEW),
      UnorderedElementsAre(0, 1, 2, 3, 4));
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST_F(IndexTest, LabelPropertyIndexFiltering) {
  // We insert vertices with values:
  // 0 0.0 1 1.0 2 2.0 3 3.0 4 4.0
  // Then we check all combinations of inclusive and exclusive bounds.
  // We also have a mix of doubles and integers to verify that they are sorted
  // properly.

  storage.CreateIndex(label1, prop_val);

  {
    auto acc = storage.Access();

    for (int i = 0; i < 10; ++i) {
      auto vertex = CreateVertex(&acc);
      ASSERT_NO_ERROR(vertex.AddLabel(label1));
      ASSERT_NO_ERROR(vertex.SetProperty(
          prop_val, i % 2 ? PropertyValue(i / 2) : PropertyValue(i / 2.0)));
    }
    ASSERT_NO_ERROR(acc.Commit());
  }
  {
    auto acc = storage.Access();
    for (int i = 0; i < 5; ++i) {
      EXPECT_THAT(
          GetIds(acc.Vertices(label1, prop_val, PropertyValue(i), View::OLD)),
          UnorderedElementsAre(2 * i, 2 * i + 1));
    }

    // [1, +inf>
    EXPECT_THAT(GetIds(acc.Vertices(label1, prop_val,
                                    utils::MakeBoundInclusive(PropertyValue(1)),
                                    std::nullopt, View::OLD)),
                UnorderedElementsAre(2, 3, 4, 5, 6, 7, 8, 9));
    // <1, +inf>
    EXPECT_THAT(GetIds(acc.Vertices(label1, prop_val,
                                    utils::MakeBoundExclusive(PropertyValue(1)),
                                    std::nullopt, View::OLD)),
                UnorderedElementsAre(4, 5, 6, 7, 8, 9));

    // <-inf, 3]
    EXPECT_THAT(GetIds(acc.Vertices(label1, prop_val, std::nullopt,
                                    utils::MakeBoundInclusive(PropertyValue(3)),
                                    View::OLD)),
                UnorderedElementsAre(0, 1, 2, 3, 4, 5, 6, 7));
    // <-inf, 3>
    EXPECT_THAT(GetIds(acc.Vertices(label1, prop_val, std::nullopt,
                                    utils::MakeBoundExclusive(PropertyValue(3)),
                                    View::OLD)),
                UnorderedElementsAre(0, 1, 2, 3, 4, 5));

    // [1, 3]
    EXPECT_THAT(
        GetIds(acc.Vertices(
            label1, prop_val, utils::MakeBoundInclusive(PropertyValue(1)),
            utils::MakeBoundInclusive(PropertyValue(3)), View::OLD)),
        UnorderedElementsAre(2, 3, 4, 5, 6, 7));
    // <1, 3]
    EXPECT_THAT(
        GetIds(acc.Vertices(
            label1, prop_val, utils::MakeBoundExclusive(PropertyValue(1)),
            utils::MakeBoundInclusive(PropertyValue(3)), View::OLD)),
        UnorderedElementsAre(4, 5, 6, 7));
    // [1, 3>
    EXPECT_THAT(
        GetIds(acc.Vertices(
            label1, prop_val, utils::MakeBoundInclusive(PropertyValue(1)),
            utils::MakeBoundExclusive(PropertyValue(3)), View::OLD)),
        UnorderedElementsAre(2, 3, 4, 5));
    // <1, 3>
    EXPECT_THAT(
        GetIds(acc.Vertices(
            label1, prop_val, utils::MakeBoundExclusive(PropertyValue(1)),
            utils::MakeBoundExclusive(PropertyValue(3)), View::OLD)),
        UnorderedElementsAre(4, 5));
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST_F(IndexTest, LabelPropertyIndexCountEstimate) {
  storage.CreateIndex(label1, prop_val);

  auto acc = storage.Access();
  for (int i = 1; i <= 10; ++i) {
    for (int j = 0; j < i; ++j) {
      auto vertex = CreateVertex(&acc);
      ASSERT_NO_ERROR(vertex.AddLabel(label1));
      ASSERT_NO_ERROR(vertex.SetProperty(prop_val, PropertyValue(i)));
    }
  }

  EXPECT_EQ(acc.ApproximateVertexCount(label1, prop_val), 55);
  for (int i = 1; i <= 10; ++i) {
    EXPECT_EQ(acc.ApproximateVertexCount(label1, prop_val, PropertyValue(i)),
              i);
  }

  EXPECT_EQ(acc.ApproximateVertexCount(
                label1, prop_val, utils::MakeBoundInclusive(PropertyValue(2)),
                utils::MakeBoundInclusive(PropertyValue(6))),
            2 + 3 + 4 + 5 + 6);
}
