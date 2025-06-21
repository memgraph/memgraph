set(CMAKE_SYSTEM_NAME Linux)
execute_process(
    COMMAND uname -m
    OUTPUT_VARIABLE uname_result
    OUTPUT_STRIP_TRAILING_WHITESPACE
)
set(CMAKE_SYSTEM_PROCESSOR "${uname_result}")

set(tools "/opt/toolchain-v7")


#set(CMAKE_SYSROOT "${tools}")
set(CMAKE_PREFIX_PATH "${tools}")
set(MG_TOOLCHAIN_ROOT "${tools}")
message(STATUS "Using toolchain file at: ${CMAKE_TOOLCHAIN_FILE}" )

set(CMAKE_C_COMPILER "${tools}/bin/clang")
set(CMAKE_CXX_COMPILER "${tools}/bin/clang++")
#set(CMAKE_C_FLAGS "-isystem '${tools}/include'")
#set(CMAKE_CXX_FLAGS "-isystem '${tools}/include'")

#set(CMAKE_FIND_ROOT_PATH_MODE_PROGRAM NEVER)
#set(CMAKE_FIND_ROOT_PATH_MODE_LIBRARY ONLY)
#set(CMAKE_FIND_ROOT_PATH_MODE_INCLUDE ONLY)
#set(CMAKE_FIND_ROOT_PATH_MODE_PACKAGE ONLY)
