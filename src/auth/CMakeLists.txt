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

target_link_libraries(mg-auth nlohmann_json::nlohmann_json libbcrypt gflags fmt::fmt)
target_link_libraries(mg-auth mg-utils mg-kvstore mg-license mg::system mg-replication)

target_link_libraries(mg-auth ${Seccomp_LIBRARIES})
target_include_directories(mg-auth SYSTEM PRIVATE ${Seccomp_INCLUDE_DIRS})

target_link_libraries(mg-auth openssl::openssl)

# Install reference auth modules and their configuration files.
install(PROGRAMS ${CMAKE_CURRENT_SOURCE_DIR}/reference_modules/example.py
        DESTINATION lib/memgraph/auth_module)
install(PROGRAMS ${CMAKE_CURRENT_SOURCE_DIR}/reference_modules/ldap.py
        DESTINATION lib/memgraph/auth_module)
install(PROGRAMS ${CMAKE_CURRENT_SOURCE_DIR}/reference_modules/saml.py
        DESTINATION lib/memgraph/auth_module)
install(FILES ${CMAKE_CURRENT_SOURCE_DIR}/reference_modules/ldap.example.yaml
        DESTINATION /etc/memgraph/auth_module)
install(PROGRAMS ${CMAKE_CURRENT_SOURCE_DIR}/reference_modules/requirements.txt
        DESTINATION lib/memgraph/auth_module)
install(PROGRAMS ${CMAKE_CURRENT_SOURCE_DIR}/reference_modules/oidc.py
        DESTINATION lib/memgraph/auth_module)
