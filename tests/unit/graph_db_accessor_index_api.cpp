#include <experimental/optional>
#include <memory>
#include <thread>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "database/dbms.hpp"
#include "database/graph_db_accessor.hpp"
#include "utils/bound.hpp"

using testing::UnorderedElementsAreArray;

template <typename TIterable>
auto Count(TIterable iterable) {
  return std::distance(iterable.begin(), iterable.end());
}

/**
 * A test fixture that contains a database, accessor,
 * label, property and an edge_type.
 */
class GraphDbAccessorIndex : public testing::Test {
 protected:
  Dbms dbms;
  std::unique_ptr<GraphDbAccessor> dba = dbms.active();
  GraphDbTypes::Property property = dba->Property("property");
  GraphDbTypes::Label label = dba->Label("label");
  GraphDbTypes::EdgeType edge_type = dba->EdgeType("edge_type");

  auto AddVertex() {
    auto vertex = dba->InsertVertex();
    vertex.add_label(label);
    return vertex;
  }

  auto AddVertex(int property_value) {
    auto vertex = dba->InsertVertex();
    vertex.add_label(label);
    vertex.PropsSet(property, property_value);
    return vertex;
  }

  // commits the current dba, and replaces it with a new one
  void Commit() {
    dba->Commit();
    auto dba2 = dbms.active();
    dba.swap(dba2);
  }
};

TEST_F(GraphDbAccessorIndex, LabelIndexCount) {
  auto label2 = dba->Label("label2");
  EXPECT_EQ(dba->VerticesCount(label), 0);
  EXPECT_EQ(dba->VerticesCount(label2), 0);
  EXPECT_EQ(dba->VerticesCount(), 0);
  for (int i = 0; i < 11; ++i) dba->InsertVertex().add_label(label);
  for (int i = 0; i < 17; ++i) dba->InsertVertex().add_label(label2);
  // even though xxx_count functions in GraphDbAccessor can over-estaimate
  // in this situation they should be exact (nothing was ever deleted)
  EXPECT_EQ(dba->VerticesCount(label), 11);
  EXPECT_EQ(dba->VerticesCount(label2), 17);
  EXPECT_EQ(dba->VerticesCount(), 28);
}

TEST_F(GraphDbAccessorIndex, LabelIndexIteration) {
  // add 10 vertices, check visibility
  for (int i = 0; i < 10; i++) AddVertex();
  EXPECT_EQ(Count(dba->Vertices(label, false)), 0);
  EXPECT_EQ(Count(dba->Vertices(label, true)), 10);
  Commit();
  EXPECT_EQ(Count(dba->Vertices(label, false)), 10);
  EXPECT_EQ(Count(dba->Vertices(label, true)), 10);

  // remove 3 vertices, check visibility
  int deleted = 0;
  for (auto vertex : dba->Vertices(false)) {
    dba->RemoveVertex(vertex);
    if (++deleted >= 3) break;
  }
  EXPECT_EQ(Count(dba->Vertices(label, false)), 10);
  EXPECT_EQ(Count(dba->Vertices(label, true)), 7);
  Commit();
  EXPECT_EQ(Count(dba->Vertices(label, false)), 7);
  EXPECT_EQ(Count(dba->Vertices(label, true)), 7);
}

TEST_F(GraphDbAccessorIndex, EdgesCount) {
  auto edge_type2 = dba->EdgeType("edge_type2");
  EXPECT_EQ(dba->EdgesCount(), 0);

  auto v1 = AddVertex();
  auto v2 = AddVertex();
  for (int i = 0; i < 11; ++i) dba->InsertEdge(v1, v2, edge_type);
  for (int i = 0; i < 17; ++i) dba->InsertEdge(v1, v2, edge_type2);
  // even though xxx_count functions in GraphDbAccessor can over-estaimate
  // in this situation they should be exact (nothing was ever deleted)
  EXPECT_EQ(dba->EdgesCount(), 28);
}

TEST_F(GraphDbAccessorIndex, LabelPropertyIndexBuild) {
  AddVertex(0);

  Commit();
  dba->BuildIndex(label, property);
  Commit();

  EXPECT_EQ(dba->VerticesCount(label, property), 1);

  // confirm there is a differentiation of indexes based on (label, property)
  auto label2 = dba->Label("label2");
  auto property2 = dba->Property("property2");
  dba->BuildIndex(label2, property);
  dba->BuildIndex(label, property2);
  Commit();

  EXPECT_EQ(dba->VerticesCount(label, property), 1);
  EXPECT_EQ(dba->VerticesCount(label2, property), 0);
  EXPECT_EQ(dba->VerticesCount(label, property2), 0);
}

TEST_F(GraphDbAccessorIndex, LabelPropertyIndexBuildTwice) {
  dba->BuildIndex(label, property);
  EXPECT_THROW(dba->BuildIndex(label, property), utils::BasicException);
}

TEST_F(GraphDbAccessorIndex, LabelPropertyIndexCount) {
  dba->BuildIndex(label, property);
  EXPECT_EQ(dba->VerticesCount(label, property), 0);
  EXPECT_EQ(Count(dba->Vertices(label, property)), 0);
  for (int i = 0; i < 14; ++i) AddVertex(0);
  EXPECT_EQ(dba->VerticesCount(label, property), 14);
  EXPECT_EQ(Count(dba->Vertices(label, property)), 14);
}

TEST(GraphDbAccessorIndexApi, LabelPropertyBuildIndexConcurrent) {
  Dbms dbms;
  auto dba = dbms.active();

  // We need to build indices in other threads.
  auto build_index_async = [&dbms](int &success, int index) {
    std::thread([&dbms, &success, index]() {
      auto dba = dbms.active();
      try {
        dba->BuildIndex(dba->Label("l" + std::to_string(index)),
                        dba->Property("p" + std::to_string(index)));
        dba->Commit();
        success = 1;
      } catch (IndexBuildInProgressException &) {
        dba->Abort();
        success = 0;
      }
    }).detach();
  };

  int build_1_success = -1;
  build_index_async(build_1_success, 1);
  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  // First index build should now be inside the BuildIndex function waiting for
  // dba to commit. A second built attempt should fail.
  int build_2_success = -1;
  build_index_async(build_2_success, 2);
  std::this_thread::sleep_for(std::chrono::milliseconds(30));
  EXPECT_EQ(build_1_success, -1);
  EXPECT_EQ(build_2_success, 0);

  // End dba and expect that first build index finished successfully.
  dba->Commit();
  std::this_thread::sleep_for(std::chrono::milliseconds(30));
  EXPECT_EQ(build_1_success, 1);
}

#define EXPECT_WITH_MARGIN(x, center) \
  EXPECT_THAT(                        \
      x, testing::AllOf(testing::Ge(center - 2), testing::Le(center + 2)));

TEST_F(GraphDbAccessorIndex, LabelPropertyValueCount) {
  dba->BuildIndex(label, property);

  // add some vertices without the property
  for (int i = 0; i < 20; i++) AddVertex();

  // add vertices with prop values [0, 29), ten vertices for each value
  for (int i = 0; i < 300; i++) AddVertex(i / 10);
  // add verties in t he [30, 40) range, 100 vertices for each value
  for (int i = 0; i < 1000; i++) AddVertex(30 + i / 100);

  // test estimates for exact value count
  EXPECT_WITH_MARGIN(dba->VerticesCount(label, property, 10), 10);
  EXPECT_WITH_MARGIN(dba->VerticesCount(label, property, 14), 10);
  EXPECT_WITH_MARGIN(dba->VerticesCount(label, property, 30), 100);
  EXPECT_WITH_MARGIN(dba->VerticesCount(label, property, 39), 100);
  EXPECT_EQ(dba->VerticesCount(label, property, 40), 0);

  // helper functions
  auto Inclusive = [](int64_t value) {
    return std::experimental::make_optional(
        utils::MakeBoundInclusive(PropertyValue(value)));
  };
  auto Exclusive = [](int64_t value) {
    return std::experimental::make_optional(
        utils::MakeBoundExclusive(PropertyValue(value)));
  };
  auto VerticesCount = [this](auto lower, auto upper) {
    return dba->VerticesCount(label, property, lower, upper);
  };

  using std::experimental::nullopt;
  ::testing::FLAGS_gtest_death_test_style = "threadsafe";
  EXPECT_DEATH(VerticesCount(nullopt, nullopt), "bound must be provided");
  EXPECT_WITH_MARGIN(VerticesCount(nullopt, Exclusive(4)), 40);
  EXPECT_WITH_MARGIN(VerticesCount(nullopt, Inclusive(4)), 50);
  EXPECT_WITH_MARGIN(VerticesCount(Exclusive(13), nullopt), 160 + 1000);
  EXPECT_WITH_MARGIN(VerticesCount(Inclusive(13), nullopt), 170 + 1000);
  EXPECT_WITH_MARGIN(VerticesCount(Inclusive(13), Exclusive(14)), 10);
  EXPECT_WITH_MARGIN(VerticesCount(Exclusive(13), Inclusive(14)), 10);
  EXPECT_WITH_MARGIN(VerticesCount(Exclusive(13), Exclusive(13)), 0);
  EXPECT_WITH_MARGIN(VerticesCount(Inclusive(20), Exclusive(13)), 0);
}

#undef EXPECT_WITH_MARGIN

TEST_F(GraphDbAccessorIndex, LabelPropertyValueIteration) {
  dba->BuildIndex(label, property);
  Commit();

  // insert 10 verties and and check visibility
  for (int i = 0; i < 10; i++) AddVertex(12);
  EXPECT_EQ(Count(dba->Vertices(label, property, 12, false)), 0);
  EXPECT_EQ(Count(dba->Vertices(label, property, 12, true)), 10);
  Commit();
  EXPECT_EQ(Count(dba->Vertices(label, property, 12, false)), 10);
  EXPECT_EQ(Count(dba->Vertices(label, property, 12, true)), 10);
}

TEST_F(GraphDbAccessorIndex, LabelPropertyValueSorting) {
  dba->BuildIndex(label, property);
  Commit();

  std::vector<PropertyValue> expected_property_value(50, 0);

  // strings
  for (int i = 0; i < 10; ++i) {
    auto vertex_accessor = dba->InsertVertex();
    vertex_accessor.add_label(label);
    vertex_accessor.PropsSet(property,
                             static_cast<std::string>(std::to_string(i)));
    expected_property_value[i] = vertex_accessor.PropsAt(property);
  }
  // bools - insert in reverse to check for comparison between values.
  for (int i = 9; i >= 0; --i) {
    auto vertex_accessor = dba->InsertVertex();
    vertex_accessor.add_label(label);
    vertex_accessor.PropsSet(property, static_cast<bool>(i / 5));
    expected_property_value[10 + i] = vertex_accessor.PropsAt(property);
  }

  // integers
  for (int i = 0; i < 10; ++i) {
    auto vertex_accessor = dba->InsertVertex();
    vertex_accessor.add_label(label);
    vertex_accessor.PropsSet(property, i);
    expected_property_value[20 + 2 * i] = vertex_accessor.PropsAt(property);
  }
  // doubles
  for (int i = 0; i < 10; ++i) {
    auto vertex_accessor = dba->InsertVertex();
    vertex_accessor.add_label(label);
    vertex_accessor.PropsSet(property, static_cast<double>(i + 0.5));
    expected_property_value[20 + 2 * i + 1] = vertex_accessor.PropsAt(property);
  }

  // lists of ints - insert in reverse to check for comparision between
  // lists.
  for (int i = 9; i >= 0; --i) {
    auto vertex_accessor = dba->InsertVertex();
    vertex_accessor.add_label(label);
    std::vector<PropertyValue> value;
    value.push_back(PropertyValue(i));
    vertex_accessor.PropsSet(property, value);
    expected_property_value[40 + i] = vertex_accessor.PropsAt(property);
  }

  // Maps. Declare a vector in the expected order, then shuffle when setting on
  // vertices.
  std::vector<std::map<std::string, PropertyValue>> maps{
      {{"b", 12}},
      {{"b", 12}, {"a", 77}},
      {{"a", 77}, {"c", 0}},
      {{"a", 78}, {"b", 12}}};
  expected_property_value.insert(expected_property_value.end(), maps.begin(),
                                 maps.end());
  auto shuffled = maps;
  std::random_shuffle(shuffled.begin(), shuffled.end());
  for (const auto &map : shuffled) {
    auto vertex_accessor = dba->InsertVertex();
    vertex_accessor.add_label(label);
    vertex_accessor.PropsSet(property, map);
  }

  EXPECT_EQ(Count(dba->Vertices(label, property, false)), 0);
  EXPECT_EQ(Count(dba->Vertices(label, property, true)), 54);

  int cnt = 0;
  for (auto vertex : dba->Vertices(label, property, true)) {
    const PropertyValue &property_value = vertex.PropsAt(property);
    EXPECT_EQ(property_value.type(), expected_property_value[cnt].type());
    switch (property_value.type()) {
      case PropertyValue::Type::Bool:
        EXPECT_EQ(property_value.Value<bool>(),
                  expected_property_value[cnt].Value<bool>());
        break;
      case PropertyValue::Type::Double:
        EXPECT_EQ(property_value.Value<double>(),
                  expected_property_value[cnt].Value<double>());
        break;
      case PropertyValue::Type::Int:
        EXPECT_EQ(property_value.Value<int64_t>(),
                  expected_property_value[cnt].Value<int64_t>());
        break;
      case PropertyValue::Type::String:
        EXPECT_EQ(property_value.Value<std::string>(),
                  expected_property_value[cnt].Value<std::string>());
        break;
      case PropertyValue::Type::List: {
        auto received_value =
            property_value.Value<std::vector<PropertyValue>>();
        auto expected_value =
            expected_property_value[cnt].Value<std::vector<PropertyValue>>();
        EXPECT_EQ(received_value.size(), expected_value.size());
        EXPECT_EQ(received_value.size(), 1);
        EXPECT_EQ(received_value[0].Value<int64_t>(),
                  expected_value[0].Value<int64_t>());
        break;
      }
      case PropertyValue::Type::Map: {
        auto received_value =
            property_value.Value<std::map<std::string, PropertyValue>>();
        auto expected_value =
            expected_property_value[cnt]
                .Value<std::map<std::string, PropertyValue>>();
        EXPECT_EQ(received_value.size(), expected_value.size());
        for (const auto &kv : expected_value) {
          auto found = expected_value.find(kv.first);
          EXPECT_NE(found, expected_value.end());
          EXPECT_EQ(kv.second.Value<int64_t>(), found->second.Value<int64_t>());
        }
        break;
      }
      case PropertyValue::Type::Null:
        ASSERT_FALSE("Invalid value type.");
    }
    ++cnt;
  }
}

/**
 * A test fixture that contains a database, accessor,
 * (label, property) index and 100 vertices, 10 for
 * each of [0, 10) property values.
 */
class GraphDbAccesssorIndexRange : public GraphDbAccessorIndex {
 protected:
  void SetUp() override {
    dba->BuildIndex(label, property);
    for (int i = 0; i < 100; i++) AddVertex(i / 10);

    ASSERT_EQ(Count(dba->Vertices(false)), 0);
    ASSERT_EQ(Count(dba->Vertices(true)), 100);
    Commit();
    ASSERT_EQ(Count(dba->Vertices(false)), 100);
  }

  auto Vertices(std::experimental::optional<utils::Bound<PropertyValue>> lower,
                std::experimental::optional<utils::Bound<PropertyValue>> upper,
                bool current_state = false) {
    return dba->Vertices(label, property, lower, upper, current_state);
  }

  auto Inclusive(PropertyValue value) {
    return std::experimental::make_optional(
        utils::MakeBoundInclusive(PropertyValue(value)));
  }

  auto Exclusive(int value) {
    return std::experimental::make_optional(
        utils::MakeBoundExclusive(PropertyValue(value)));
  }
};

TEST_F(GraphDbAccesssorIndexRange, RangeIteration) {
  using std::experimental::nullopt;
  EXPECT_EQ(Count(Vertices(nullopt, Inclusive(7))), 80);
  EXPECT_EQ(Count(Vertices(nullopt, Exclusive(7))), 70);
  EXPECT_EQ(Count(Vertices(Inclusive(7), nullopt)), 30);
  EXPECT_EQ(Count(Vertices(Exclusive(7), nullopt)), 20);
  EXPECT_EQ(Count(Vertices(Exclusive(3), Exclusive(6))), 20);
  EXPECT_EQ(Count(Vertices(Inclusive(3), Inclusive(6))), 40);
  EXPECT_EQ(Count(Vertices(Inclusive(6), Inclusive(3))), 0);
  ::testing::FLAGS_gtest_death_test_style = "threadsafe";
  EXPECT_DEATH(Vertices(nullopt, nullopt), "bound must be provided");
}

TEST_F(GraphDbAccesssorIndexRange, RangeIterationCurrentState) {
  using std::experimental::nullopt;
  EXPECT_EQ(Count(Vertices(nullopt, Inclusive(7))), 80);
  for (int i = 0; i < 20; i++) AddVertex(2);
  EXPECT_EQ(Count(Vertices(nullopt, Inclusive(7))), 80);
  EXPECT_EQ(Count(Vertices(nullopt, Inclusive(7), true)), 100);
  Commit();
  EXPECT_EQ(Count(Vertices(nullopt, Inclusive(7))), 100);
}

TEST_F(GraphDbAccesssorIndexRange, RangeInterationIncompatibleTypes) {
  using std::experimental::nullopt;

  // using PropertyValue::Null as a bound fails with an assertion
  ::testing::FLAGS_gtest_death_test_style = "threadsafe";
  EXPECT_DEATH(Vertices(nullopt, Inclusive(PropertyValue::Null)),
               "not a valid index bound");
  EXPECT_DEATH(Vertices(Inclusive(PropertyValue::Null), nullopt),
               "not a valid index bound");
  std::vector<PropertyValue> incompatible_with_int{
      "string", true, std::vector<PropertyValue>{1}};

  // using incompatible upper and lower bounds yields no results
  EXPECT_EQ(Count(Vertices(Inclusive(2), Inclusive("string"))), 0);

  // for incomparable bound and stored data,
  // expect that no results are returned
  ASSERT_EQ(Count(Vertices(Inclusive(0), nullopt)), 100);
  for (PropertyValue value : incompatible_with_int) {
    ::testing::FLAGS_gtest_death_test_style = "threadsafe";
    EXPECT_EQ(Count(Vertices(nullopt, Inclusive(value))), 0)
        << "Found vertices of type int for predicate value type: "
        << value.type();
    EXPECT_EQ(Count(Vertices(Inclusive(value), nullopt)), 0)
        << "Found vertices of type int for predicate value type: "
        << value.type();
  }

  // we can compare int to double
  EXPECT_EQ(Count(Vertices(nullopt, Inclusive(1000.0))), 100);
  EXPECT_EQ(Count(Vertices(Inclusive(0.0), nullopt)), 100);
}
