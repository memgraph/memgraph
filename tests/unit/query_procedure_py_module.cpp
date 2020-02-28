#include <gtest/gtest.h>

#include <string>

#include "query/procedure/mg_procedure_impl.hpp"
#include "query/procedure/py_module.hpp"

TEST(PyModule, MgpValueToPyObject) {
  mgp_memory memory{utils::NewDeleteResource()};
  auto *list = mgp_list_make_empty(42, &memory);
  {
    // Create a list: [null, false, true, 42, 0.1, "some text"]
    auto primitive_values = {mgp_value_make_null(&memory),
                             mgp_value_make_bool(0, &memory),
                             mgp_value_make_bool(1, &memory),
                             mgp_value_make_int(42, &memory),
                             mgp_value_make_double(0.1, &memory),
                             mgp_value_make_string("some text", &memory)};
    for (auto *val : primitive_values) {
      mgp_list_append(list, val);
      mgp_value_destroy(val);
    }
  }
  auto *list_val = mgp_value_make_list(list);
  auto *map = mgp_map_make_empty(&memory);
  mgp_map_insert(map, "list", list_val);
  mgp_value_destroy(list_val);
  auto *map_val = mgp_value_make_map(map);
  auto gil = py::EnsureGIL();
  py::Object py_graph(query::procedure::MakePyGraph(nullptr, &memory));
  auto py_dict = query::procedure::MgpValueToPyObject(
      *map_val, reinterpret_cast<query::procedure::PyGraph *>(
                    static_cast<PyObject *>(py_graph)));
  mgp_value_destroy(map_val);
  // We should now have in Python:
  // {"list": [None, False, True, 42, 0.1, "some text"]}
  ASSERT_TRUE(PyDict_Check(py_dict));
  EXPECT_EQ(PyDict_Size(py_dict), 1);
  PyObject *key = nullptr;
  PyObject *value = nullptr;
  Py_ssize_t pos = 0;
  while (PyDict_Next(py_dict, &pos, &key, &value)) {
    ASSERT_TRUE(PyUnicode_Check(key));
    EXPECT_EQ(std::string(PyUnicode_AsUTF8(key)), "list");
    ASSERT_TRUE(PyList_Check(value));
    ASSERT_EQ(PyList_Size(value), 6);
    EXPECT_EQ(PyList_GetItem(value, 0), Py_None);
    EXPECT_EQ(PyList_GetItem(value, 1), Py_False);
    EXPECT_EQ(PyList_GetItem(value, 2), Py_True);
    auto *py_long = PyList_GetItem(value, 3);
    ASSERT_TRUE(PyLong_Check(py_long));
    EXPECT_EQ(PyLong_AsLong(py_long), 42);
    auto *py_float = PyList_GetItem(value, 4);
    ASSERT_TRUE(PyFloat_Check(py_float));
    EXPECT_EQ(PyFloat_AsDouble(py_float), 0.1);
    auto *py_str = PyList_GetItem(value, 5);
    ASSERT_TRUE(PyUnicode_Check(py_str));
    EXPECT_EQ(std::string(PyUnicode_AsUTF8(py_str)), "some text");
  }
}

// Our _mgp types should not support (by default) pickling and copying.
static void AssertPickleAndCopyAreNotSupported(PyObject *py_obj) {
  py::Object pickle_mod(PyImport_ImportModule("pickle"));
  ASSERT_TRUE(pickle_mod);
  ASSERT_FALSE(py::FetchError());
  py::Object dumps_res(pickle_mod.CallMethod("dumps", py_obj));
  ASSERT_FALSE(dumps_res);
  ASSERT_TRUE(py::FetchError());
  py::Object copy_mod(PyImport_ImportModule("copy"));
  ASSERT_TRUE(pickle_mod);
  ASSERT_FALSE(py::FetchError());
  py::Object copy_res(pickle_mod.CallMethod("copy", py_obj));
  ASSERT_FALSE(copy_res);
  ASSERT_TRUE(py::FetchError());
  // We should have cleared the error state.
  ASSERT_FALSE(py::FetchError());
  py::Object deepcopy_res(pickle_mod.CallMethod("deepcopy", py_obj));
  ASSERT_FALSE(deepcopy_res);
  ASSERT_TRUE(py::FetchError());
}

// TODO: Test Vertex and Edge values
TEST(PyModule, PyPath) {
  storage::Storage db;
  {
    auto dba = db.Access();
    auto v1 = dba.CreateVertex();
    auto v2 = dba.CreateVertex();
    ASSERT_TRUE(
        dba.CreateEdge(&v1, &v2, dba.NameToEdgeType("type")).HasValue());
    ASSERT_FALSE(dba.Commit().HasError());
  }
  auto storage_dba = db.Access();
  query::DbAccessor dba(&storage_dba);
  mgp_memory memory{utils::NewDeleteResource()};
  mgp_graph graph{&dba, storage::View::OLD};
  auto *start_v = mgp_graph_get_vertex_by_id(&graph, mgp_vertex_id{0}, &memory);
  ASSERT_TRUE(start_v);
  auto *path = mgp_path_make_with_start(start_v, &memory);
  ASSERT_TRUE(path);
  auto *edges_it = mgp_vertex_iter_out_edges(start_v, &memory);
  ASSERT_TRUE(edges_it);
  for (const auto *edge = mgp_edges_iterator_get(edges_it); edge;
       edge = mgp_edges_iterator_next(edges_it)) {
    ASSERT_TRUE(mgp_path_expand(path, edge));
  }
  ASSERT_EQ(mgp_path_size(path), 1);
  mgp_edges_iterator_destroy(edges_it);
  mgp_vertex_destroy(start_v);
  auto *path_value = mgp_value_make_path(path);
  ASSERT_TRUE(path_value);
  auto gil = py::EnsureGIL();
  py::Object py_graph(query::procedure::MakePyGraph(&graph, &memory));
  ASSERT_TRUE(py_graph);
  // We have setup the C structs, so create convert to PyObject.
  py::Object py_path_value(
      query::procedure::MgpValueToPyObject(*path_value, py_graph));
  ASSERT_TRUE(py_path_value);
  AssertPickleAndCopyAreNotSupported(py_path_value);
  // Convert back to C struct and check equality.
  auto *new_path_value =
      query::procedure::PyObjectToMgpValue(py_path_value, &memory);
  ASSERT_TRUE(new_path_value);
  ASSERT_NE(new_path_value, path_value);  // Pointer compare.
  ASSERT_TRUE(mgp_value_is_path(new_path_value));
  ASSERT_TRUE(mgp_path_equal(mgp_value_get_path(path_value),
                             mgp_value_get_path(new_path_value)));
  mgp_value_destroy(new_path_value);
  mgp_value_destroy(path_value);
  ASSERT_FALSE(dba.Commit().HasError());
}

TEST(PyModule, PyObjectToMgpValue) {
  mgp_memory memory{utils::NewDeleteResource()};
  auto gil = py::EnsureGIL();
  py::Object py_value{Py_BuildValue("[i f s (i f s) {s i s f}]", 1, 1.0, "one",
                                    2, 2.0, "two", "three", 3, "four", 4.0)};
  mgp_value *value = query::procedure::PyObjectToMgpValue(py_value, &memory);

  ASSERT_TRUE(mgp_value_is_list(value));
  const mgp_list *list1 = mgp_value_get_list(value);
  EXPECT_EQ(mgp_list_size(list1), 5);
  ASSERT_TRUE(mgp_value_is_int(mgp_list_at(list1, 0)));
  EXPECT_EQ(mgp_value_get_int(mgp_list_at(list1, 0)), 1);
  ASSERT_TRUE(mgp_value_is_double(mgp_list_at(list1, 1)));
  EXPECT_EQ(mgp_value_get_double(mgp_list_at(list1, 1)), 1.0);
  ASSERT_TRUE(mgp_value_is_string(mgp_list_at(list1, 2)));
  EXPECT_STREQ(mgp_value_get_string(mgp_list_at(list1, 2)), "one");
  ASSERT_TRUE(mgp_value_is_list(mgp_list_at(list1, 3)));
  const mgp_list *list2 = mgp_value_get_list(mgp_list_at(list1, 3));
  EXPECT_EQ(mgp_list_size(list2), 3);
  ASSERT_TRUE(mgp_value_is_int(mgp_list_at(list2, 0)));
  EXPECT_EQ(mgp_value_get_int(mgp_list_at(list2, 0)), 2);
  ASSERT_TRUE(mgp_value_is_double(mgp_list_at(list2, 1)));
  EXPECT_EQ(mgp_value_get_double(mgp_list_at(list2, 1)), 2.0);
  ASSERT_TRUE(mgp_value_is_string(mgp_list_at(list2, 2)));
  EXPECT_STREQ(mgp_value_get_string(mgp_list_at(list2, 2)), "two");
  ASSERT_TRUE(mgp_value_is_map(mgp_list_at(list1, 4)));
  const mgp_map *map = mgp_value_get_map(mgp_list_at(list1, 4));
  EXPECT_EQ(mgp_map_size(map), 2);
  const mgp_value *v1 = mgp_map_at(map, "three");
  ASSERT_TRUE(mgp_value_is_int(v1));
  EXPECT_EQ(mgp_value_get_int(v1), 3);
  const mgp_value *v2 = mgp_map_at(map, "four");
  ASSERT_TRUE(mgp_value_is_double(v2));
  EXPECT_EQ(mgp_value_get_double(v2), 4.0);
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  // Initialize Python
  auto *program_name = Py_DecodeLocale(argv[0], nullptr);
  CHECK(program_name);
  // Set program name, so Python can find its way to runtime libraries relative
  // to executable.
  Py_SetProgramName(program_name);
  PyImport_AppendInittab("_mgp", &query::procedure::PyInitMgpModule);
  Py_InitializeEx(0 /* = initsigs */);
  PyEval_InitThreads();
  int test_result;
  {
    py::Object mgp(PyImport_ImportModule("_mgp"));
    Py_BEGIN_ALLOW_THREADS;
    test_result = RUN_ALL_TESTS();
    Py_END_ALLOW_THREADS;
  }
  // Shutdown Python
  Py_Finalize();
  PyMem_RawFree(program_name);
  return test_result;
}
