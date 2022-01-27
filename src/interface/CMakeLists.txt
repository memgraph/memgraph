include(MgThrift)

set(MG_INTERFACE_PATH ${CMAKE_CURRENT_SOURCE_DIR})
set(MG_INTERFACE_OUTPUT_PATH ${CMAKE_CURRENT_BINARY_DIR})

mg_thrift_library(
  "storage" #file_name
  "Storage" #services
  "${MG_INTERFACE_PATH}" #file_path
  "${MG_INTERFACE_PATH}" #output_path
  "interface" #include_prefix
)
