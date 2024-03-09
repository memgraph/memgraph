set(mg_custom_cursors
  once.cpp
  scanall.cpp
)
add_library(mg_custom_cursors STATIC ${mg_custom_cursors})
# TODO(gitbuda): Having mg-storage-v2 here is broken.
target_link_libraries(mg_custom_cursors mg-utils mg-storage-v2)
