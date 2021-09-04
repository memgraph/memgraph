#include <functional>
#include <memory>
#include <utility>

#include <gtest/gtest.h>

#include "query/procedure/mg_procedure_impl.hpp"

#include "test_utils.hpp"

TEST(CypherType, PresentableNameSimpleTypes) {
  EXPECT_EQ(EXPECT_MGP_NO_ERROR(const mgp_type *, mgp_type_any)->impl->GetPresentableName(), "ANY");
  EXPECT_EQ(EXPECT_MGP_NO_ERROR(const mgp_type *, mgp_type_bool)->impl->GetPresentableName(), "BOOLEAN");
  EXPECT_EQ(EXPECT_MGP_NO_ERROR(const mgp_type *, mgp_type_string)->impl->GetPresentableName(), "STRING");
  EXPECT_EQ(EXPECT_MGP_NO_ERROR(const mgp_type *, mgp_type_int)->impl->GetPresentableName(), "INTEGER");
  EXPECT_EQ(EXPECT_MGP_NO_ERROR(const mgp_type *, mgp_type_float)->impl->GetPresentableName(), "FLOAT");
  EXPECT_EQ(EXPECT_MGP_NO_ERROR(const mgp_type *, mgp_type_number)->impl->GetPresentableName(), "NUMBER");
  EXPECT_EQ(EXPECT_MGP_NO_ERROR(const mgp_type *, mgp_type_map)->impl->GetPresentableName(), "MAP");
  EXPECT_EQ(EXPECT_MGP_NO_ERROR(const mgp_type *, mgp_type_node)->impl->GetPresentableName(), "NODE");
  EXPECT_EQ(EXPECT_MGP_NO_ERROR(const mgp_type *, mgp_type_relationship)->impl->GetPresentableName(), "RELATIONSHIP");
  EXPECT_EQ(EXPECT_MGP_NO_ERROR(const mgp_type *, mgp_type_path)->impl->GetPresentableName(), "PATH");
}

TEST(CypherType, PresentableNameCompositeTypes) {
  const mgp_type *any_type = EXPECT_MGP_NO_ERROR(const mgp_type *, mgp_type_any);
  {
    const auto *nullable_any = EXPECT_MGP_NO_ERROR(const mgp_type *, mgp_type_nullable, any_type);
    EXPECT_EQ(nullable_any->impl->GetPresentableName(), "ANY?");
  }
  {
    const auto *nullable_any =
        EXPECT_MGP_NO_ERROR(const mgp_type *, mgp_type_nullable,
                            EXPECT_MGP_NO_ERROR(const mgp_type *, mgp_type_nullable,
                                                EXPECT_MGP_NO_ERROR(const mgp_type *, mgp_type_nullable, any_type)));
    EXPECT_EQ(nullable_any->impl->GetPresentableName(), "ANY?");
  }
  {
    const auto *nullable_list = EXPECT_MGP_NO_ERROR(const mgp_type *, mgp_type_nullable,
                                                    EXPECT_MGP_NO_ERROR(const mgp_type *, mgp_type_list, any_type));
    EXPECT_EQ(nullable_list->impl->GetPresentableName(), "LIST? OF ANY");
  }
  {
    const auto *list_of_int =
        EXPECT_MGP_NO_ERROR(const mgp_type *, mgp_type_list, EXPECT_MGP_NO_ERROR(const mgp_type *, mgp_type_int));
    EXPECT_EQ(list_of_int->impl->GetPresentableName(), "LIST OF INTEGER");
  }
  {
    const auto *list_of_nullable_path = EXPECT_MGP_NO_ERROR(
        const mgp_type *, mgp_type_list,
        EXPECT_MGP_NO_ERROR(const mgp_type *, mgp_type_nullable, EXPECT_MGP_NO_ERROR(const mgp_type *, mgp_type_path)));
    EXPECT_EQ(list_of_nullable_path->impl->GetPresentableName(), "LIST OF PATH?");
  }
  {
    const auto *list_of_list_of_map = EXPECT_MGP_NO_ERROR(
        const mgp_type *, mgp_type_list,
        EXPECT_MGP_NO_ERROR(const mgp_type *, mgp_type_list, EXPECT_MGP_NO_ERROR(const mgp_type *, mgp_type_map)));
    EXPECT_EQ(list_of_list_of_map->impl->GetPresentableName(), "LIST OF LIST OF MAP");
  }
  {
    const auto *nullable_list_of_nullable_list_of_nullable_string = EXPECT_MGP_NO_ERROR(
        const mgp_type *, mgp_type_nullable,
        EXPECT_MGP_NO_ERROR(
            const mgp_type *, mgp_type_list,
            EXPECT_MGP_NO_ERROR(
                const mgp_type *, mgp_type_nullable,
                EXPECT_MGP_NO_ERROR(const mgp_type *, mgp_type_list,
                                    EXPECT_MGP_NO_ERROR(const mgp_type *, mgp_type_nullable,
                                                        EXPECT_MGP_NO_ERROR(const mgp_type *, mgp_type_string))))));
    EXPECT_EQ(nullable_list_of_nullable_list_of_nullable_string->impl->GetPresentableName(),
              "LIST? OF LIST? OF STRING?");
  }
}

TEST(CypherType, NullSatisfiesType) {
  mgp_memory memory{utils::NewDeleteResource()};
  {
    auto *mgp_null = EXPECT_MGP_NO_ERROR(mgp_value *, mgp_value_make_null, &memory);
    const query::TypedValue tv_null;
    std::vector<const mgp_type *> primitive_types{EXPECT_MGP_NO_ERROR(const mgp_type *, mgp_type_any),
                                                  EXPECT_MGP_NO_ERROR(const mgp_type *, mgp_type_bool),
                                                  EXPECT_MGP_NO_ERROR(const mgp_type *, mgp_type_string),
                                                  EXPECT_MGP_NO_ERROR(const mgp_type *, mgp_type_int),
                                                  EXPECT_MGP_NO_ERROR(const mgp_type *, mgp_type_float),
                                                  EXPECT_MGP_NO_ERROR(const mgp_type *, mgp_type_number),
                                                  EXPECT_MGP_NO_ERROR(const mgp_type *, mgp_type_map),
                                                  EXPECT_MGP_NO_ERROR(const mgp_type *, mgp_type_node),
                                                  EXPECT_MGP_NO_ERROR(const mgp_type *, mgp_type_relationship),
                                                  EXPECT_MGP_NO_ERROR(const mgp_type *, mgp_type_path)};
    for (const auto *primitive_type : primitive_types) {
      for (const auto *type :
           {primitive_type, EXPECT_MGP_NO_ERROR(const mgp_type *, mgp_type_list, primitive_type),
            EXPECT_MGP_NO_ERROR(const mgp_type *, mgp_type_list,
                                EXPECT_MGP_NO_ERROR(const mgp_type *, mgp_type_nullable, primitive_type))}) {
        EXPECT_FALSE(type->impl->SatisfiesType(*mgp_null));
        EXPECT_FALSE(type->impl->SatisfiesType(tv_null));
        const auto *null_type = EXPECT_MGP_NO_ERROR(const mgp_type *, mgp_type_nullable, type);
        EXPECT_TRUE(null_type->impl->SatisfiesType(*mgp_null));
        EXPECT_TRUE(null_type->impl->SatisfiesType(tv_null));
      }
    }
    mgp_value_destroy(mgp_null);
  }
}

static void CheckSatisfiesTypesAndNullable(const mgp_value *mgp_val, const query::TypedValue &tv,
                                           const std::vector<const mgp_type *> &types) {
  for (const auto *type : types) {
    EXPECT_TRUE(type->impl->SatisfiesType(*mgp_val)) << type->impl->GetPresentableName();
    EXPECT_TRUE(type->impl->SatisfiesType(tv));
    const auto *null_type = EXPECT_MGP_NO_ERROR(const mgp_type *, mgp_type_nullable, type);
    EXPECT_TRUE(null_type->impl->SatisfiesType(*mgp_val)) << null_type->impl->GetPresentableName();
    EXPECT_TRUE(null_type->impl->SatisfiesType(tv));
  }
}

static void CheckNotSatisfiesTypesAndListAndNullable(const mgp_value *mgp_val, const query::TypedValue &tv,
                                                     const std::vector<const mgp_type *> &elem_types) {
  for (const auto *elem_type : elem_types) {
    for (const auto *type :
         {elem_type, EXPECT_MGP_NO_ERROR(const mgp_type *, mgp_type_list, elem_type),
          EXPECT_MGP_NO_ERROR(const mgp_type *, mgp_type_list,
                              EXPECT_MGP_NO_ERROR(const mgp_type *, mgp_type_nullable, elem_type))}) {
      EXPECT_FALSE(type->impl->SatisfiesType(*mgp_val)) << type->impl->GetPresentableName();
      EXPECT_FALSE(type->impl->SatisfiesType(tv));
      const auto *null_type = EXPECT_MGP_NO_ERROR(const mgp_type *, mgp_type_nullable, type);
      EXPECT_FALSE(null_type->impl->SatisfiesType(*mgp_val)) << null_type->impl->GetPresentableName();
      EXPECT_FALSE(null_type->impl->SatisfiesType(tv));
    }
  }
}

TEST(CypherType, BoolSatisfiesType) {
  mgp_memory memory{utils::NewDeleteResource()};
  auto *mgp_bool = EXPECT_MGP_NO_ERROR(mgp_value *, mgp_value_make_bool, 1, &memory);
  const query::TypedValue tv_bool(true);
  CheckSatisfiesTypesAndNullable(
      mgp_bool, tv_bool,
      {EXPECT_MGP_NO_ERROR(const mgp_type *, mgp_type_any), EXPECT_MGP_NO_ERROR(const mgp_type *, mgp_type_bool)});
  CheckNotSatisfiesTypesAndListAndNullable(
      mgp_bool, tv_bool,
      {EXPECT_MGP_NO_ERROR(const mgp_type *, mgp_type_string), EXPECT_MGP_NO_ERROR(const mgp_type *, mgp_type_int),
       EXPECT_MGP_NO_ERROR(const mgp_type *, mgp_type_float), EXPECT_MGP_NO_ERROR(const mgp_type *, mgp_type_number),
       EXPECT_MGP_NO_ERROR(const mgp_type *, mgp_type_map), EXPECT_MGP_NO_ERROR(const mgp_type *, mgp_type_node),
       EXPECT_MGP_NO_ERROR(const mgp_type *, mgp_type_relationship),
       EXPECT_MGP_NO_ERROR(const mgp_type *, mgp_type_path)});
  mgp_value_destroy(mgp_bool);
}

TEST(CypherType, IntSatisfiesType) {
  mgp_memory memory{utils::NewDeleteResource()};
  auto *mgp_int = EXPECT_MGP_NO_ERROR(mgp_value *, mgp_value_make_int, 42, &memory);
  const query::TypedValue tv_int(42);
  CheckSatisfiesTypesAndNullable(
      mgp_int, tv_int,
      {EXPECT_MGP_NO_ERROR(const mgp_type *, mgp_type_any), EXPECT_MGP_NO_ERROR(const mgp_type *, mgp_type_int),
       EXPECT_MGP_NO_ERROR(const mgp_type *, mgp_type_number)});
  CheckNotSatisfiesTypesAndListAndNullable(
      mgp_int, tv_int,
      {EXPECT_MGP_NO_ERROR(const mgp_type *, mgp_type_bool), EXPECT_MGP_NO_ERROR(const mgp_type *, mgp_type_string),
       EXPECT_MGP_NO_ERROR(const mgp_type *, mgp_type_float), EXPECT_MGP_NO_ERROR(const mgp_type *, mgp_type_map),
       EXPECT_MGP_NO_ERROR(const mgp_type *, mgp_type_node),
       EXPECT_MGP_NO_ERROR(const mgp_type *, mgp_type_relationship),
       EXPECT_MGP_NO_ERROR(const mgp_type *, mgp_type_path)});
  mgp_value_destroy(mgp_int);
}

TEST(CypherType, DoubleSatisfiesType) {
  mgp_memory memory{utils::NewDeleteResource()};
  auto *mgp_double = EXPECT_MGP_NO_ERROR(mgp_value *, mgp_value_make_double, 42, &memory);
  const query::TypedValue tv_double(42.0);
  CheckSatisfiesTypesAndNullable(
      mgp_double, tv_double,
      {EXPECT_MGP_NO_ERROR(const mgp_type *, mgp_type_any), EXPECT_MGP_NO_ERROR(const mgp_type *, mgp_type_float),
       EXPECT_MGP_NO_ERROR(const mgp_type *, mgp_type_number)});
  CheckNotSatisfiesTypesAndListAndNullable(
      mgp_double, tv_double,
      {EXPECT_MGP_NO_ERROR(const mgp_type *, mgp_type_bool), EXPECT_MGP_NO_ERROR(const mgp_type *, mgp_type_string),
       EXPECT_MGP_NO_ERROR(const mgp_type *, mgp_type_int), EXPECT_MGP_NO_ERROR(const mgp_type *, mgp_type_map),
       EXPECT_MGP_NO_ERROR(const mgp_type *, mgp_type_node),
       EXPECT_MGP_NO_ERROR(const mgp_type *, mgp_type_relationship),
       EXPECT_MGP_NO_ERROR(const mgp_type *, mgp_type_path)});
  mgp_value_destroy(mgp_double);
}

TEST(CypherType, StringSatisfiesType) {
  mgp_memory memory{utils::NewDeleteResource()};
  auto *mgp_string = EXPECT_MGP_NO_ERROR(mgp_value *, mgp_value_make_string, "text", &memory);
  const query::TypedValue tv_string("text");
  CheckSatisfiesTypesAndNullable(
      mgp_string, tv_string,
      {EXPECT_MGP_NO_ERROR(const mgp_type *, mgp_type_any), EXPECT_MGP_NO_ERROR(const mgp_type *, mgp_type_string)});
  CheckNotSatisfiesTypesAndListAndNullable(
      mgp_string, tv_string,
      {EXPECT_MGP_NO_ERROR(const mgp_type *, mgp_type_bool), EXPECT_MGP_NO_ERROR(const mgp_type *, mgp_type_int),
       EXPECT_MGP_NO_ERROR(const mgp_type *, mgp_type_float), EXPECT_MGP_NO_ERROR(const mgp_type *, mgp_type_number),
       EXPECT_MGP_NO_ERROR(const mgp_type *, mgp_type_map), EXPECT_MGP_NO_ERROR(const mgp_type *, mgp_type_node),
       EXPECT_MGP_NO_ERROR(const mgp_type *, mgp_type_relationship),
       EXPECT_MGP_NO_ERROR(const mgp_type *, mgp_type_path)});
  mgp_value_destroy(mgp_string);
}

TEST(CypherType, MapSatisfiesType) {
  mgp_memory memory{utils::NewDeleteResource()};
  auto *map = EXPECT_MGP_NO_ERROR(mgp_map *, mgp_map_make_empty, &memory);
  EXPECT_EQ(
      mgp_map_insert(
          map, "key",
          test_utils::CreateValueOwningPtr(EXPECT_MGP_NO_ERROR(mgp_value *, mgp_value_make_int, 42, &memory)).get()),
      MGP_ERROR_NO_ERROR);
  auto *mgp_map_v = EXPECT_MGP_NO_ERROR(mgp_value *, mgp_value_make_map, map);
  const query::TypedValue tv_map(std::map<std::string, query::TypedValue>{{"key", query::TypedValue(42)}});
  CheckSatisfiesTypesAndNullable(
      mgp_map_v, tv_map,
      {EXPECT_MGP_NO_ERROR(const mgp_type *, mgp_type_any), EXPECT_MGP_NO_ERROR(const mgp_type *, mgp_type_map)});
  CheckNotSatisfiesTypesAndListAndNullable(
      mgp_map_v, tv_map,
      {EXPECT_MGP_NO_ERROR(const mgp_type *, mgp_type_bool), EXPECT_MGP_NO_ERROR(const mgp_type *, mgp_type_string),
       EXPECT_MGP_NO_ERROR(const mgp_type *, mgp_type_int), EXPECT_MGP_NO_ERROR(const mgp_type *, mgp_type_float),
       EXPECT_MGP_NO_ERROR(const mgp_type *, mgp_type_number), EXPECT_MGP_NO_ERROR(const mgp_type *, mgp_type_node),
       EXPECT_MGP_NO_ERROR(const mgp_type *, mgp_type_relationship),
       EXPECT_MGP_NO_ERROR(const mgp_type *, mgp_type_path)});
  mgp_value_destroy(mgp_map_v);
}

TEST(CypherType, VertexSatisfiesType) {
  storage::Storage db;
  auto storage_dba = db.Access();
  query::DbAccessor dba(&storage_dba);
  auto vertex = dba.InsertVertex();
  mgp_memory memory{utils::NewDeleteResource()};
  utils::Allocator<mgp_vertex> alloc(memory.impl);
  mgp_graph graph{&dba, storage::View::NEW};
  auto *mgp_vertex_v =
      EXPECT_MGP_NO_ERROR(mgp_value *, mgp_value_make_vertex, alloc.new_object<mgp_vertex>(vertex, &graph));
  const query::TypedValue tv_vertex(vertex);
  CheckSatisfiesTypesAndNullable(
      mgp_vertex_v, tv_vertex,
      {EXPECT_MGP_NO_ERROR(const mgp_type *, mgp_type_any), EXPECT_MGP_NO_ERROR(const mgp_type *, mgp_type_node),
       EXPECT_MGP_NO_ERROR(const mgp_type *, mgp_type_map)});
  CheckNotSatisfiesTypesAndListAndNullable(
      mgp_vertex_v, tv_vertex,
      {EXPECT_MGP_NO_ERROR(const mgp_type *, mgp_type_bool), EXPECT_MGP_NO_ERROR(const mgp_type *, mgp_type_string),
       EXPECT_MGP_NO_ERROR(const mgp_type *, mgp_type_int), EXPECT_MGP_NO_ERROR(const mgp_type *, mgp_type_float),
       EXPECT_MGP_NO_ERROR(const mgp_type *, mgp_type_number),
       EXPECT_MGP_NO_ERROR(const mgp_type *, mgp_type_relationship),
       EXPECT_MGP_NO_ERROR(const mgp_type *, mgp_type_path)});
  mgp_value_destroy(mgp_vertex_v);
}

TEST(CypherType, EdgeSatisfiesType) {
  storage::Storage db;
  auto storage_dba = db.Access();
  query::DbAccessor dba(&storage_dba);
  auto v1 = dba.InsertVertex();
  auto v2 = dba.InsertVertex();
  auto edge = *dba.InsertEdge(&v1, &v2, dba.NameToEdgeType("edge_type"));
  mgp_memory memory{utils::NewDeleteResource()};
  utils::Allocator<mgp_edge> alloc(memory.impl);
  mgp_graph graph{&dba, storage::View::NEW};
  auto *mgp_edge_v = EXPECT_MGP_NO_ERROR(mgp_value *, mgp_value_make_edge, alloc.new_object<mgp_edge>(edge, &graph));
  const query::TypedValue tv_edge(edge);
  CheckSatisfiesTypesAndNullable(mgp_edge_v, tv_edge,
                                 {EXPECT_MGP_NO_ERROR(const mgp_type *, mgp_type_any),
                                  EXPECT_MGP_NO_ERROR(const mgp_type *, mgp_type_relationship),
                                  EXPECT_MGP_NO_ERROR(const mgp_type *, mgp_type_map)});
  CheckNotSatisfiesTypesAndListAndNullable(
      mgp_edge_v, tv_edge,
      {EXPECT_MGP_NO_ERROR(const mgp_type *, mgp_type_bool), EXPECT_MGP_NO_ERROR(const mgp_type *, mgp_type_string),
       EXPECT_MGP_NO_ERROR(const mgp_type *, mgp_type_int), EXPECT_MGP_NO_ERROR(const mgp_type *, mgp_type_float),
       EXPECT_MGP_NO_ERROR(const mgp_type *, mgp_type_number), EXPECT_MGP_NO_ERROR(const mgp_type *, mgp_type_node),
       EXPECT_MGP_NO_ERROR(const mgp_type *, mgp_type_path)});
  mgp_value_destroy(mgp_edge_v);
}

TEST(CypherType, PathSatisfiesType) {
  storage::Storage db;
  auto storage_dba = db.Access();
  query::DbAccessor dba(&storage_dba);
  auto v1 = dba.InsertVertex();
  auto v2 = dba.InsertVertex();
  auto edge = *dba.InsertEdge(&v1, &v2, dba.NameToEdgeType("edge_type"));
  mgp_memory memory{utils::NewDeleteResource()};
  utils::Allocator<mgp_path> alloc(memory.impl);
  mgp_graph graph{&dba, storage::View::NEW};
  auto *mgp_vertex_v = alloc.new_object<mgp_vertex>(v1, &graph);
  auto path = EXPECT_MGP_NO_ERROR(mgp_path *, mgp_path_make_with_start, mgp_vertex_v, &memory);
  ASSERT_TRUE(path);
  alloc.delete_object(mgp_vertex_v);
  auto mgp_edge_v = alloc.new_object<mgp_edge>(edge, &graph);
  ASSERT_EQ(mgp_path_expand(path, mgp_edge_v), MGP_ERROR_NO_ERROR);
  alloc.delete_object(mgp_edge_v);
  auto *mgp_path_v = EXPECT_MGP_NO_ERROR(mgp_value *, mgp_value_make_path, path);
  const query::TypedValue tv_path(query::Path(v1, edge, v2));
  CheckSatisfiesTypesAndNullable(
      mgp_path_v, tv_path,
      {EXPECT_MGP_NO_ERROR(const mgp_type *, mgp_type_any), EXPECT_MGP_NO_ERROR(const mgp_type *, mgp_type_path)});
  CheckNotSatisfiesTypesAndListAndNullable(
      mgp_path_v, tv_path,
      {EXPECT_MGP_NO_ERROR(const mgp_type *, mgp_type_bool), EXPECT_MGP_NO_ERROR(const mgp_type *, mgp_type_string),
       EXPECT_MGP_NO_ERROR(const mgp_type *, mgp_type_int), EXPECT_MGP_NO_ERROR(const mgp_type *, mgp_type_float),
       EXPECT_MGP_NO_ERROR(const mgp_type *, mgp_type_number), EXPECT_MGP_NO_ERROR(const mgp_type *, mgp_type_map),
       EXPECT_MGP_NO_ERROR(const mgp_type *, mgp_type_node),
       EXPECT_MGP_NO_ERROR(const mgp_type *, mgp_type_relationship)});
  mgp_value_destroy(mgp_path_v);
}

static std::vector<const mgp_type *> MakeListTypes(const std::vector<const mgp_type *> &element_types) {
  std::vector<const mgp_type *> list_types;
  list_types.reserve(2U * element_types.size());
  for (const auto *type : element_types) {
    list_types.push_back(EXPECT_MGP_NO_ERROR(const mgp_type *, mgp_type_list, type));
    list_types.push_back(EXPECT_MGP_NO_ERROR(const mgp_type *, mgp_type_list,
                                             EXPECT_MGP_NO_ERROR(const mgp_type *, mgp_type_nullable, type)));
  }
  return list_types;
}

TEST(CypherType, EmptyListSatisfiesType) {
  mgp_memory memory{utils::NewDeleteResource()};
  auto *list = EXPECT_MGP_NO_ERROR(mgp_list *, mgp_list_make_empty, 0, &memory);
  auto *mgp_list_v = EXPECT_MGP_NO_ERROR(mgp_value *, mgp_value_make_list, list);
  query::TypedValue tv_list(std::vector<query::TypedValue>{});
  // Empty List satisfies all list element types
  std::vector<const mgp_type *> primitive_types{EXPECT_MGP_NO_ERROR(const mgp_type *, mgp_type_any),
                                                EXPECT_MGP_NO_ERROR(const mgp_type *, mgp_type_bool),
                                                EXPECT_MGP_NO_ERROR(const mgp_type *, mgp_type_string),
                                                EXPECT_MGP_NO_ERROR(const mgp_type *, mgp_type_int),
                                                EXPECT_MGP_NO_ERROR(const mgp_type *, mgp_type_float),
                                                EXPECT_MGP_NO_ERROR(const mgp_type *, mgp_type_number),
                                                EXPECT_MGP_NO_ERROR(const mgp_type *, mgp_type_map),
                                                EXPECT_MGP_NO_ERROR(const mgp_type *, mgp_type_node),
                                                EXPECT_MGP_NO_ERROR(const mgp_type *, mgp_type_relationship),
                                                EXPECT_MGP_NO_ERROR(const mgp_type *, mgp_type_path)};
  auto all_types = MakeListTypes(primitive_types);
  all_types.push_back(EXPECT_MGP_NO_ERROR(const mgp_type *, mgp_type_any));
  CheckSatisfiesTypesAndNullable(mgp_list_v, tv_list, all_types);
  mgp_value_destroy(mgp_list_v);
}

TEST(CypherType, ListOfIntSatisfiesType) {
  mgp_memory memory{utils::NewDeleteResource()};
  constexpr int64_t elem_count = 3;
  auto *list = EXPECT_MGP_NO_ERROR(mgp_list *, mgp_list_make_empty, elem_count, &memory);
  auto *mgp_list_v = EXPECT_MGP_NO_ERROR(mgp_value *, mgp_value_make_list, list);
  query::TypedValue tv_list(std::vector<query::TypedValue>{});
  for (int64_t i = 0; i < elem_count; ++i) {
    ASSERT_EQ(
        mgp_list_append(
            list,
            test_utils::CreateValueOwningPtr(EXPECT_MGP_NO_ERROR(mgp_value *, mgp_value_make_int, i, &memory)).get()),
        MGP_ERROR_NO_ERROR);
    tv_list.ValueList().emplace_back(i);
    auto valid_types = MakeListTypes({EXPECT_MGP_NO_ERROR(const mgp_type *, mgp_type_any),
                                      EXPECT_MGP_NO_ERROR(const mgp_type *, mgp_type_int),
                                      EXPECT_MGP_NO_ERROR(const mgp_type *, mgp_type_number)});
    valid_types.push_back(EXPECT_MGP_NO_ERROR(const mgp_type *, mgp_type_any));
    CheckSatisfiesTypesAndNullable(mgp_list_v, tv_list, valid_types);
    CheckNotSatisfiesTypesAndListAndNullable(
        mgp_list_v, tv_list,
        {EXPECT_MGP_NO_ERROR(const mgp_type *, mgp_type_bool), EXPECT_MGP_NO_ERROR(const mgp_type *, mgp_type_string),
         EXPECT_MGP_NO_ERROR(const mgp_type *, mgp_type_float), EXPECT_MGP_NO_ERROR(const mgp_type *, mgp_type_map),
         EXPECT_MGP_NO_ERROR(const mgp_type *, mgp_type_node),
         EXPECT_MGP_NO_ERROR(const mgp_type *, mgp_type_relationship),
         EXPECT_MGP_NO_ERROR(const mgp_type *, mgp_type_path)});
  }
  mgp_value_destroy(mgp_list_v);
}

TEST(CypherType, ListOfIntAndBoolSatisfiesType) {
  mgp_memory memory{utils::NewDeleteResource()};
  constexpr int64_t elem_count = 2;
  auto *list = EXPECT_MGP_NO_ERROR(mgp_list *, mgp_list_make_empty, elem_count, &memory);
  auto *mgp_list_v = EXPECT_MGP_NO_ERROR(mgp_value *, mgp_value_make_list, list);
  query::TypedValue tv_list(std::vector<query::TypedValue>{});
  // Add an int
  ASSERT_EQ(
      mgp_list_append(
          list,
          test_utils::CreateValueOwningPtr(EXPECT_MGP_NO_ERROR(mgp_value *, mgp_value_make_int, 42, &memory)).get()),
      MGP_ERROR_NO_ERROR);
  tv_list.ValueList().emplace_back(42);
  // Add a boolean
  ASSERT_EQ(
      mgp_list_append(
          list,
          test_utils::CreateValueOwningPtr(EXPECT_MGP_NO_ERROR(mgp_value *, mgp_value_make_bool, 1, &memory)).get()),
      MGP_ERROR_NO_ERROR);
  tv_list.ValueList().emplace_back(true);
  auto valid_types = MakeListTypes({EXPECT_MGP_NO_ERROR(const mgp_type *, mgp_type_any)});
  valid_types.push_back(EXPECT_MGP_NO_ERROR(const mgp_type *, mgp_type_any));
  CheckSatisfiesTypesAndNullable(mgp_list_v, tv_list, valid_types);
  // All other types will not be satisfied
  CheckNotSatisfiesTypesAndListAndNullable(
      mgp_list_v, tv_list,
      {EXPECT_MGP_NO_ERROR(const mgp_type *, mgp_type_bool), EXPECT_MGP_NO_ERROR(const mgp_type *, mgp_type_string),
       EXPECT_MGP_NO_ERROR(const mgp_type *, mgp_type_int), EXPECT_MGP_NO_ERROR(const mgp_type *, mgp_type_float),
       EXPECT_MGP_NO_ERROR(const mgp_type *, mgp_type_number), EXPECT_MGP_NO_ERROR(const mgp_type *, mgp_type_map),
       EXPECT_MGP_NO_ERROR(const mgp_type *, mgp_type_node),
       EXPECT_MGP_NO_ERROR(const mgp_type *, mgp_type_relationship),
       EXPECT_MGP_NO_ERROR(const mgp_type *, mgp_type_path)});
  mgp_value_destroy(mgp_list_v);
}

TEST(CypherType, ListOfNullSatisfiesType) {
  mgp_memory memory{utils::NewDeleteResource()};
  auto *list = EXPECT_MGP_NO_ERROR(mgp_list *, mgp_list_make_empty, 1, &memory);
  auto *mgp_list_v = EXPECT_MGP_NO_ERROR(mgp_value *, mgp_value_make_list, list);
  query::TypedValue tv_list(std::vector<query::TypedValue>{});
  ASSERT_EQ(
      mgp_list_append(
          list, test_utils::CreateValueOwningPtr(EXPECT_MGP_NO_ERROR(mgp_value *, mgp_value_make_null, &memory)).get()),
      MGP_ERROR_NO_ERROR);
  tv_list.ValueList().emplace_back();
  // List with Null satisfies all nullable list element types
  std::vector<const mgp_type *> primitive_types{EXPECT_MGP_NO_ERROR(const mgp_type *, mgp_type_any),
                                                EXPECT_MGP_NO_ERROR(const mgp_type *, mgp_type_bool),
                                                EXPECT_MGP_NO_ERROR(const mgp_type *, mgp_type_string),
                                                EXPECT_MGP_NO_ERROR(const mgp_type *, mgp_type_int),
                                                EXPECT_MGP_NO_ERROR(const mgp_type *, mgp_type_float),
                                                EXPECT_MGP_NO_ERROR(const mgp_type *, mgp_type_number),
                                                EXPECT_MGP_NO_ERROR(const mgp_type *, mgp_type_map),
                                                EXPECT_MGP_NO_ERROR(const mgp_type *, mgp_type_node),
                                                EXPECT_MGP_NO_ERROR(const mgp_type *, mgp_type_relationship),
                                                EXPECT_MGP_NO_ERROR(const mgp_type *, mgp_type_path)};
  std::vector<const mgp_type *> valid_types{EXPECT_MGP_NO_ERROR(const mgp_type *, mgp_type_any)};
  valid_types.reserve(1U + primitive_types.size());
  for (const auto *elem_type : primitive_types) {
    valid_types.push_back(EXPECT_MGP_NO_ERROR(const mgp_type *, mgp_type_list,
                                              EXPECT_MGP_NO_ERROR(const mgp_type *, mgp_type_nullable, elem_type)));
  }
  CheckSatisfiesTypesAndNullable(mgp_list_v, tv_list, valid_types);
  std::vector<const mgp_type *> invalid_types;
  invalid_types.reserve(primitive_types.size());
  for (const auto *elem_type : primitive_types) {
    invalid_types.push_back(EXPECT_MGP_NO_ERROR(const mgp_type *, mgp_type_list, elem_type));
  }
  for (const auto *type : invalid_types) {
    EXPECT_FALSE(type->impl->SatisfiesType(*mgp_list_v)) << type->impl->GetPresentableName();
    EXPECT_FALSE(type->impl->SatisfiesType(tv_list));
    const auto *null_type = EXPECT_MGP_NO_ERROR(const mgp_type *, mgp_type_nullable, type);
    EXPECT_FALSE(null_type->impl->SatisfiesType(*mgp_list_v)) << null_type->impl->GetPresentableName();
    EXPECT_FALSE(null_type->impl->SatisfiesType(tv_list));
  }
  mgp_value_destroy(mgp_list_v);
}
