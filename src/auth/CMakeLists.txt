add_library(mg-auth STATIC)
target_sources(mg-auth PRIVATE
    auth.cpp
    crypto.cpp
    models.cpp
    module.cpp
    rpc.cpp
    replication_handlers.cpp
    profiles/user_profiles.cpp
)

find_package(Seccomp REQUIRED)
find_package(fmt REQUIRED)
find_package(gflags REQUIRED)

target_link_libraries(mg-auth
    PUBLIC mg-kvstore mg-license mg::system mg-replication
    PRIVATE nlohmann_json::nlohmann_json libbcrypt::libbcrypt gflags fmt::fmt
    mg-utils ${Seccomp_LIBRARIES} openssl::openssl)
target_include_directories(mg-auth SYSTEM PRIVATE ${Seccomp_INCLUDE_DIRS})

# Install reference auth modules and their configuration files.
install(PROGRAMS ${CMAKE_CURRENT_SOURCE_DIR}/reference_modules/common.py
        DESTINATION lib/memgraph/auth_module)
install(PROGRAMS ${CMAKE_CURRENT_SOURCE_DIR}/reference_modules/example.py
        DESTINATION lib/memgraph/auth_module)
install(PROGRAMS ${CMAKE_CURRENT_SOURCE_DIR}/reference_modules/ldap.py
        DESTINATION lib/memgraph/auth_module)
install(PROGRAMS ${CMAKE_CURRENT_SOURCE_DIR}/reference_modules/saml.py
        DESTINATION lib/memgraph/auth_module)
# TODO: This absolute path is inconsistent with sibling files which use relative paths.
# Consider changing to: DESTINATION lib/memgraph/auth_module
install(FILES ${CMAKE_CURRENT_SOURCE_DIR}/reference_modules/ldap.example.yaml
        DESTINATION /etc/memgraph/auth_module)
install(PROGRAMS ${CMAKE_CURRENT_SOURCE_DIR}/reference_modules/requirements.txt
        DESTINATION lib/memgraph/auth_module)
install(PROGRAMS ${CMAKE_CURRENT_SOURCE_DIR}/reference_modules/oidc.py
        DESTINATION lib/memgraph/auth_module)
