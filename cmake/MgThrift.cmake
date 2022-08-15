find_package(Gflags)
find_package(Folly)
find_package(wangle)
find_package(fizz)
find_package(proxygen)
find_package(FBThrift)
set(THRIFTCPP2 "FBThrift::thriftcpp2")
set(THRIFT1 ${FBTHRIFT_COMPILER})

include(${FBTHRIFT_INCLUDE_DIR}/thrift/ThriftLibrary.cmake)

set(MG_INTERFACE_TARGET_NAME_PREFIX "mg-interface")

function(_mg_thrift_generate
  file_name
  services
  language
  options
  file_path
  output_path
  include_prefix
)
  cmake_parse_arguments(THRIFT_GENERATE # Prefix
    "" # Options
    "" # One Value args
    "THRIFT_INCLUDE_DIRECTORIES" # Multi-value args
    "${ARGN}")

  set(thrift_include_directories)

  foreach(dir ${THRIFT_GENERATE_THRIFT_INCLUDE_DIRECTORIES})
    list(APPEND thrift_include_directories "-I" "${dir}")
  endforeach()

  set("${file_name}-${language}-HEADERS"
    ${output_path}/gen-${language}/${file_name}_constants.h
    ${output_path}/gen-${language}/${file_name}_data.h
    ${output_path}/gen-${language}/${file_name}_metadata.h
    ${output_path}/gen-${language}/${file_name}_types.h
    ${output_path}/gen-${language}/${file_name}_types.tcc
  )
  set("${file_name}-${language}-SOURCES"
    ${output_path}/gen-${language}/${file_name}_constants.cpp
    ${output_path}/gen-${language}/${file_name}_data.cpp
    ${output_path}/gen-${language}/${file_name}_types.cpp
  )

  if(NOT "${options}" MATCHES "no_metadata")
    set("${file_name}-${language}-SOURCES"
      ${${file_name}-${language}-SOURCES}
      ${output_path}/gen-${language}/${file_name}_metadata.cpp
    )
  endif()

  foreach(service ${services})
    set("${file_name}-${language}-HEADERS"
      ${${file_name}-${language}-HEADERS}
      ${output_path}/gen-${language}/${service}.h
      ${output_path}/gen-${language}/${service}.tcc
      ${output_path}/gen-${language}/${service}AsyncClient.h
      ${output_path}/gen-${language}/${service}_custom_protocol.h
    )
    set("${file_name}-${language}-SOURCES"
      ${${file_name}-${language}-SOURCES}
      ${output_path}/gen-${language}/${service}.cpp
      ${output_path}/gen-${language}/${service}AsyncClient.cpp
    )
  endforeach()

  if("${include_prefix}" STREQUAL "")
    set(include_prefix_text "")
  else()
    set(include_prefix_text "include_prefix=${include_prefix}")

    if(NOT "${options}" STREQUAL "")
      set(include_prefix_text ",${include_prefix_text}")
    endif()
  endif()

  set(gen_language ${language})

  if("${language}" STREQUAL "cpp2")
    set(gen_language "mstch_cpp2")
  elseif("${language}" STREQUAL "py3")
    set(gen_language "mstch_py3")
    file(WRITE "${output_path}/gen-${language}/${file_name}/__init__.py")
  endif()

  add_custom_command(
    OUTPUT ${${file_name}-${language}-HEADERS}
    ${${file_name}-${language}-SOURCES}
    COMMAND ${THRIFT1}
    --gen "${gen_language}:${options}${include_prefix_text}"
    -o ${output_path}
    ${thrift_include_directories}
    "${file_path}/${file_name}.thrift"
    DEPENDS
    ${THRIFT1}
    "${file_path}/${file_name}.thrift"
    COMMENT "Generating ${MG_INTERFACE_TARGET_NAME_PREFIX}-${file_name}-${language} files. Output: ${output_path}"
  )
  add_custom_target(
    ${MG_INTERFACE_TARGET_NAME_PREFIX}-${file_name}-${language}-target ALL
    DEPENDS ${${language}-${language}-HEADERS}
    ${${file_name}-${language}-SOURCES}
  )

  set("${file_name}-${language}-SOURCES" ${${file_name}-${language}-SOURCES} PARENT_SCOPE)
  install(
    DIRECTORY gen-${language}
    DESTINATION include/${include_prefix}
    FILES_MATCHING PATTERN "*.h")
  install(
    DIRECTORY gen-${language}
    DESTINATION include/${include_prefix}
    FILES_MATCHING PATTERN "*.tcc")
endfunction()

function(_mg_thrift_object
  file_name
  services
  language
  options
  file_path
  output_path
  include_prefix
)
  _mg_thrift_generate(
    "${file_name}"
    "${services}"
    "${language}"
    "${options}"
    "${file_path}"
    "${output_path}"
    "${include_prefix}"
    "${ARGN}"
  )
  bypass_source_check(${${file_name}-${language}-SOURCES})
  add_library(
    "${MG_INTERFACE_TARGET_NAME_PREFIX}-${file_name}-${language}-obj"
    OBJECT
    ${${file_name}-${language}-SOURCES}
  )
  add_dependencies(
    "${MG_INTERFACE_TARGET_NAME_PREFIX}-${file_name}-${language}-obj"
    "${MG_INTERFACE_TARGET_NAME_PREFIX}-${file_name}-${language}-target"
  )
  target_include_directories(${MG_INTERFACE_TARGET_NAME_PREFIX}-${file_name}-${language}-obj PUBLIC ${output_path})
  message(STATUS "MgThrift will create the Object file : ${MG_INTERFACE_TARGET_NAME_PREFIX}-${file_name}-${language}-obj")
endfunction()

function(mg_thrift_library
  file_name
  services
  file_path
  output_path
  include_prefix
)
  _mg_thrift_object(
    "${file_name}"
    "${services}"
    "cpp2"
    "stack_arguments" # options
    "${file_path}"
    "${output_path}"
    "${include_prefix}"
    THRIFT_INCLUDE_DIRECTORIES "${FBTHRIFT_INCLUDE_DIR}"
  )
  set(LIBRARY_NAME "${MG_INTERFACE_TARGET_NAME_PREFIX}-${file_name}-cpp2")
  add_library(
    "${LIBRARY_NAME}"
    $<TARGET_OBJECTS:${LIBRARY_NAME}-obj>
  )
  target_link_libraries("${LIBRARY_NAME}" ${THRIFTCPP2})
  message("MgThrift will create the library file : ${LIBRARY_NAME}")
endfunction()
