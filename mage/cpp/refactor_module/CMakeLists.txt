set(refactor_module_src
    refactor_module.cpp
    algorithm/refactor.cpp)

mage_add_query_module(refactor 1 "${refactor_module_src}")

target_link_libraries(refactor PRIVATE fmt::fmt)
