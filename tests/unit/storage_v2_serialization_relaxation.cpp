#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "storage/v2/property_value.hpp"
#include "storage/v2/storage.hpp"
#include "storage/v2/vertex.hpp"
#include "storage/v2/vertex_accessor.hpp"
#include "storage/v2/view.hpp"

namespace {
class SerializationSetup : public ::testing::Test {
 protected:
  storage::Storage store;
  storage::Gid gid1;
  storage::Gid gid2;

  SerializationSetup() {}

  virtual ~SerializationSetup() {}

  virtual void SetUp() {
    auto acc1 = store.Access();
    auto acc2 = store.Access();

    auto vertex1 = acc1.CreateVertex();
    gid1 = vertex1.Gid();

    acc1.AdvanceCommand();
    acc2.AdvanceCommand();

    EXPECT_TRUE(acc1.FindVertex(gid1, storage::View::OLD));
    EXPECT_FALSE(acc2.FindVertex(gid1, storage::View::OLD));

    auto vertex2 = acc2.CreateVertex();
    gid2 = vertex2.Gid();

    acc1.AdvanceCommand();
    acc2.AdvanceCommand();

    EXPECT_FALSE(acc1.FindVertex(gid2, storage::View::OLD));
    EXPECT_TRUE(acc2.FindVertex(gid2, storage::View::OLD));

    ASSERT_FALSE(acc1.Commit().HasError());
    ASSERT_FALSE(acc2.Commit().HasError());
  }
};
}  // namespace

TEST_F(SerializationSetup, UseCase1) {
  auto acc1 = store.Access();
  auto acc2 = store.Access();

  auto vertex1_1 = acc1.FindVertex(gid1, storage::View::OLD);
  ASSERT_TRUE(vertex1_1);
  auto vertex1_2 = acc1.FindVertex(gid2, storage::View::OLD);
  ASSERT_TRUE(vertex1_2);

  auto vertex2_1 = acc2.FindVertex(gid1, storage::View::OLD);
  ASSERT_TRUE(vertex2_1);
  auto vertex2_2 = acc2.FindVertex(gid2, storage::View::OLD);
  ASSERT_TRUE(vertex2_2);

  ASSERT_TRUE(acc1.CreateEdge(&(*vertex1_1), &(*vertex1_2), acc1.NameToEdgeType("edge")).HasValue());
  ASSERT_TRUE(acc2.CreateEdge(&(*vertex2_1), &(*vertex2_2), acc2.NameToEdgeType("edge")).HasValue());

  ASSERT_FALSE(acc1.Commit().HasError());
  ASSERT_FALSE(acc2.Commit().HasError());
}

TEST_F(SerializationSetup, UseCase2) {
  auto acc1 = store.Access();
  auto acc2 = store.Access();

  auto vertex1_1 = acc1.FindVertex(gid1, storage::View::OLD);
  ASSERT_TRUE(vertex1_1);
  auto vertex1_2 = acc1.FindVertex(gid2, storage::View::OLD);
  ASSERT_TRUE(vertex1_2);

  auto vertex2_1 = acc2.FindVertex(gid1, storage::View::OLD);
  ASSERT_TRUE(vertex2_1);
  auto vertex2_2 = acc2.FindVertex(gid2, storage::View::OLD);
  ASSERT_TRUE(vertex2_2);

  ASSERT_TRUE(acc1.CreateEdge(&(*vertex1_1), &(*vertex1_2), acc1.NameToEdgeType("edge")).HasValue());
  ASSERT_TRUE(acc2.CreateEdge(&(*vertex2_1), &(*vertex2_2), acc2.NameToEdgeType("edge")).HasValue());

  ASSERT_FALSE(acc2.Commit().HasError());
  ASSERT_FALSE(acc1.Commit().HasError());
}

TEST_F(SerializationSetup, UseCase3) {
  auto acc1 = store.Access();
  auto acc2 = store.Access();
  auto acc3 = store.Access();

  auto vertex1_1 = acc1.FindVertex(gid1, storage::View::OLD);
  ASSERT_TRUE(vertex1_1);
  auto vertex1_2 = acc1.FindVertex(gid2, storage::View::OLD);
  ASSERT_TRUE(vertex1_2);

  auto vertex2_1 = acc2.FindVertex(gid1, storage::View::OLD);
  ASSERT_TRUE(vertex2_1);
  auto vertex2_2 = acc2.FindVertex(gid2, storage::View::OLD);
  ASSERT_TRUE(vertex2_2);

  auto vertex3_1 = acc3.FindVertex(gid1, storage::View::OLD);
  ASSERT_TRUE(vertex2_1);
  auto vertex3_2 = acc3.FindVertex(gid2, storage::View::OLD);
  ASSERT_TRUE(vertex2_2);

  ASSERT_TRUE(acc1.CreateEdge(&(*vertex1_1), &(*vertex1_2), acc1.NameToEdgeType("edge")).HasValue());
  ASSERT_TRUE(acc2.CreateEdge(&(*vertex2_1), &(*vertex2_2), acc2.NameToEdgeType("edge")).HasValue());

  ASSERT_FALSE(acc2.Commit().HasError());

  ASSERT_TRUE(acc3.CreateEdge(&(*vertex3_1), &(*vertex3_2), acc3.NameToEdgeType("edge")).HasValue());

  ASSERT_FALSE(acc3.Commit().HasError());
  ASSERT_FALSE(acc1.Commit().HasError());
}

TEST_F(SerializationSetup, SerializationConflict1) {
  auto acc1 = store.Access();
  auto acc2 = store.Access();

  auto vertex1_1 = acc1.FindVertex(gid1, storage::View::OLD);
  ASSERT_TRUE(vertex1_1);
  auto vertex1_2 = acc1.FindVertex(gid2, storage::View::OLD);
  ASSERT_TRUE(vertex1_2);

  auto vertex2_1 = acc2.FindVertex(gid1, storage::View::OLD);
  ASSERT_TRUE(vertex2_1);
  auto vertex2_2 = acc2.FindVertex(gid2, storage::View::OLD);
  ASSERT_TRUE(vertex2_2);

  ASSERT_TRUE(acc1.CreateEdge(&(*vertex1_1), &(*vertex1_2), acc1.NameToEdgeType("edge")).HasValue());
  auto property = store.NameToProperty("property2");
  vertex2_2->SetProperty(property, storage::PropertyValue(123));

  ASSERT_FALSE(acc1.Commit().HasError());
  EXPECT_DEATH(acc2.Commit().HasError(), "The transaction can't be committed!");
}

TEST_F(SerializationSetup, SerializationConflict2) {
  auto acc1 = store.Access();
  auto acc2 = store.Access();

  auto vertex1_1 = acc1.FindVertex(gid1, storage::View::OLD);
  ASSERT_TRUE(vertex1_1);
  auto vertex1_2 = acc1.FindVertex(gid2, storage::View::OLD);
  ASSERT_TRUE(vertex1_2);

  auto vertex2_1 = acc2.FindVertex(gid1, storage::View::OLD);
  ASSERT_TRUE(vertex2_1);
  auto vertex2_2 = acc2.FindVertex(gid2, storage::View::OLD);
  ASSERT_TRUE(vertex2_2);

  ASSERT_TRUE(acc1.CreateEdge(&(*vertex1_1), &(*vertex1_2), acc1.NameToEdgeType("edge")).HasValue());
  auto property = store.NameToProperty("property2");
  vertex2_2->SetProperty(property, storage::PropertyValue(123));

  ASSERT_DEATH(acc2.Commit().HasError(), "The transaction can't be committed!");
  ASSERT_FALSE(acc1.Commit().HasError());
}
