add_library(mg-auth STATIC)
target_sources(mg-auth PRIVATE
    auth.cpp
    crypto.cpp
    models.cpp
    module.cpp
    rpc.cpp
    profiles/user_profiles.cpp
)

find_package(libseccomp REQUIRED)
find_package(fmt REQUIRED)
find_package(gflags REQUIRED)

target_link_libraries(mg-auth
    PUBLIC mg-kvstore mg-license
    PRIVATE nlohmann_json::nlohmann_json libbcrypt::libbcrypt gflags fmt::fmt
    mg-utils mg::system mg-slk libseccomp::libseccomp openssl::openssl)

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
install(PROGRAMS ${CMAKE_CURRENT_SOURCE_DIR}/reference_modules/oidc.py
        DESTINATION lib/memgraph/auth_module)

# kerberos.py is left out of a FIPS build. Ubuntu's MIT krb5 is compiled with
# its own builtin crypto backend - libk5crypto3 defines krb5int_aes_encrypt,
# krb5int_hash_sha256 and friends and imports no OpenSSL symbols at all - so
# every Kerberos operation would run outside the validated module. Rebuilding
# krb5 with --with-crypto-impl=openssl is the way back in; until then the
# feature is absent rather than silently non-compliant. The module itself fails
# closed without gssapi, which the requirements filter below also drops.
if(NOT MG_FIPS)
    install(PROGRAMS ${CMAKE_CURRENT_SOURCE_DIR}/reference_modules/kerberos.py
            DESTINATION lib/memgraph/auth_module)
endif()

# One canonical requirements.txt for both builds, so the pinned versions cannot
# drift. The FIPS build installs it with the "# fips-exclude:" lines stripped -
# the dependencies only the omitted modules need.
set(AUTH_REQUIREMENTS ${CMAKE_CURRENT_SOURCE_DIR}/reference_modules/requirements.txt)
if(MG_FIPS)
    file(READ ${AUTH_REQUIREMENTS} AUTH_REQUIREMENTS_TEXT)
    # A requirements line may legally contain ';' (an environment marker), which
    # CMake would otherwise treat as a list separator and split the line on.
    string(REPLACE ";" "@SEMI@" AUTH_REQUIREMENTS_TEXT "${AUTH_REQUIREMENTS_TEXT}")
    string(REPLACE "\n" ";" AUTH_REQUIREMENTS_LINES "${AUTH_REQUIREMENTS_TEXT}")
    list(FILTER AUTH_REQUIREMENTS_LINES EXCLUDE REGEX "#[ \t]*fips-exclude:")
    string(REPLACE ";" "\n" AUTH_REQUIREMENTS_TEXT "${AUTH_REQUIREMENTS_LINES}")
    string(REPLACE "@SEMI@" ";" AUTH_REQUIREMENTS_TEXT "${AUTH_REQUIREMENTS_TEXT}")
    set(AUTH_REQUIREMENTS ${CMAKE_CURRENT_BINARY_DIR}/requirements.txt)
    file(WRITE ${AUTH_REQUIREMENTS} "${AUTH_REQUIREMENTS_TEXT}")
endif()
install(FILES ${AUTH_REQUIREMENTS}
        DESTINATION lib/memgraph/auth_module)
