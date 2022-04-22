include(MgThrift)

set(MG_INTERFACE_PATH ${CMAKE_CURRENT_SOURCE_DIR})

mg_thrift_library(
  "storage" #file_name
  "Storage" #services
  "${MG_INTERFACE_PATH}" #file_path
  "${MG_INTERFACE_PATH}" #output_path
  "interface" #include_prefix
)

mg_thrift_library(
  "meta" #file_name
  "Meta" #services
  "${MG_INTERFACE_PATH}" #file_path
  "${MG_INTERFACE_PATH}" #output_path
  "interface" #include_prefix
)
