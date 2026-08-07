set(export_module_src
    export_module.cpp
    algorithm/export.cpp
    algorithm/json_writer.cpp)

# `export` is a C++ keyword but the target name and .so filename are just strings; Memgraph derives the Cypher
# namespace from the filename, giving `CALL export.json_data(...)`.
mage_add_query_module(export 1 "${export_module_src}")

target_include_directories(export PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})
target_link_libraries(export PRIVATE nlohmann_json::nlohmann_json fmt::fmt)
