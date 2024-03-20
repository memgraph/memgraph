set(auth_src_files
    auth.cpp
    crypto.cpp
    models.cpp
    module.cpp
    rpc.cpp
    replication_handlers.cpp)

find_package(Seccomp REQUIRED)
find_package(fmt REQUIRED)
find_package(gflags REQUIRED)


add_library(mg-auth STATIC ${auth_src_files})
target_link_libraries(mg-auth json libbcrypt gflags fmt::fmt)
target_link_libraries(mg-auth mg-utils mg-kvstore mg-license mg::system mg-replication)

target_link_libraries(mg-auth ${Seccomp_LIBRARIES})
target_include_directories(mg-auth SYSTEM PRIVATE ${Seccomp_INCLUDE_DIRS})

find_package(OpenSSL REQUIRED)
target_link_libraries(mg-auth ${OPENSSL_LIBRARIES})
target_include_directories(mg-auth SYSTEM PUBLIC ${OPENSSL_INCLUDE_DIR})

# Install reference auth modules and their configuration files.
install(PROGRAMS ${CMAKE_CURRENT_SOURCE_DIR}/reference_modules/example.py
        DESTINATION lib/memgraph/auth_module)
install(PROGRAMS ${CMAKE_CURRENT_SOURCE_DIR}/reference_modules/ldap.py
        DESTINATION lib/memgraph/auth_module)
install(FILES ${CMAKE_CURRENT_SOURCE_DIR}/reference_modules/ldap.example.yaml
        DESTINATION /etc/memgraph/auth_module)
install(PROGRAMS ${CMAKE_CURRENT_SOURCE_DIR}/reference_modules/saml/saml.py
        DESTINATION lib/memgraph/auth_module/saml)
install(FILES ${CMAKE_CURRENT_SOURCE_DIR}/reference_modules/saml/settings.json
        DESTINATION /etc/memgraph/auth_module/saml)
