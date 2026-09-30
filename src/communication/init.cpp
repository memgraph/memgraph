// Copyright 2026 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include "init.hpp"

#include "context.hpp"

#include <openssl/err.h>
#include <openssl/opensslv.h>
#include <openssl/ssl.h>

#include <openssl/core_names.h>
#include <openssl/evp.h>
#include <openssl/params.h>
#include <openssl/provider.h>

#include <array>

#include <fmt/format.h>

#include "utils/exit_codes.hpp"
#include "utils/fips.hpp"
#include "utils/logging.hpp"
#include "utils/signals.hpp"
#include "utils/startup_failure.hpp"

namespace memgraph::communication {

namespace {
// OpenSSL before 1.1 did not have a out-of-the-box multithreading support
// You need to manually define locks, locking callbacks and id function.
// https://stackoverflow.com/a/42856544
// https://wiki.openssl.org/index.php/Library_Initialization#libssl_Initialization
// https://www.openssl.org/docs/man1.0.2/man3/CRYPTO_num_locks.html
#if OPENSSL_VERSION_NUMBER < 0x10100000L
std::vector<utils::SpinLock> crypto_locks;

void LockingFunction(int mode, int n, const char *file, int line) {
  if (mode & CRYPTO_LOCK) {
    crypto_locks[n].lock();
  } else {
    crypto_locks[n].unlock();
  }
}

unsigned long IdFunction() { return (unsigned long)std::hash<std::thread::id>()(std::this_thread::get_id()); }

void SetupThreading() {
  crypto_locks.resize(CRYPTO_num_locks());
  CRYPTO_set_id_callback(IdFunction);
  CRYPTO_set_locking_callback(LockingFunction);
}

void Cleanup() {
  CRYPTO_set_id_callback(nullptr);
  CRYPTO_set_locking_callback(nullptr);
  crypto_locks.clear();
}
#else
void SetupThreading() {}

void Cleanup() {}
#endif
}  // namespace

#ifdef MG_ENTERPRISE
void EnableFipsMode() {
  // Activates the provider if openssl.cnf configured one, so this both proves
  // the module is present and forces its power-on self-tests to run.
  if (OSSL_PROVIDER_available(nullptr, "fips") != 1) {
    utils::FailStartup(
        utils::ExitCode::FipsModeUnavailable,
        "--fips-mode=true but the OpenSSL FIPS provider is not available. Check that fips.so is installed and that "
        "OPENSSL_CONF and OPENSSL_MODULES point at it.");
  }

  if (EVP_default_properties_enable_fips(nullptr, 1) != 1) {
    utils::FailStartup(utils::ExitCode::FipsModeUnavailable,
                       "--fips-mode=true but the FIPS default property query could not be enabled.");
  }
  if (EVP_default_properties_is_fips_enabled(nullptr) != 1) {
    utils::FailStartup(
        utils::ExitCode::FipsModeUnavailable,
        "--fips-mode=true and enabling the FIPS default property query reported success, but it did not take effect.");
  }

  // retain_fallbacks=1: we only want a handle to read the module's identity.
  // OSSL_PROVIDER_load() would additionally disable the fallback providers as
  // a side effect, which is the config file's decision to make, not ours.
  auto *provider = OSSL_PROVIDER_try_load(nullptr, "fips", 1);
  if (provider == nullptr) {
    utils::FailStartup(utils::ExitCode::FipsModeUnavailable,
                       "--fips-mode=true but the OpenSSL FIPS provider reported as available could not be loaded.");
  }

  char *name = nullptr;
  char *version = nullptr;
  int status = 0;
  auto params = std::array{
      OSSL_PARAM_construct_utf8_ptr(OSSL_PROV_PARAM_NAME, &name, 0),
      OSSL_PARAM_construct_utf8_ptr(OSSL_PROV_PARAM_VERSION, &version, 0),
      OSSL_PARAM_construct_int(OSSL_PROV_PARAM_STATUS, &status),
      OSSL_PARAM_construct_end(),
  };
  if (OSSL_PROVIDER_get_params(provider, params.data()) != 1) {
    OSSL_PROVIDER_unload(provider);
    utils::FailStartup(utils::ExitCode::FipsModeUnavailable,
                       "--fips-mode=true but the OpenSSL FIPS provider parameters could not be read.");
  }

  // A provider that loaded but failed a self-test is non-operational, and every
  // cryptographic operation through it would fail. Refuse to serve traffic.
  if (status != 1) {
    OSSL_PROVIDER_unload(provider);
    utils::FailStartup(
        utils::ExitCode::FipsModeUnavailable,
        fmt::format("--fips-mode=true but the OpenSSL FIPS provider is not operational (status {}).", status));
  }

  if (name == nullptr || version == nullptr) {
    OSSL_PROVIDER_unload(provider);
    utils::FailStartup(utils::ExitCode::FipsModeUnavailable,
                       "--fips-mode=true but the OpenSSL FIPS provider did not report its name and version.");
  }

  // Copy the identity out before unloading: `name` and `version` point into
  // storage owned by the provider.
  auto fips_status = utils::FipsStatus{.enabled = true,
                                       .provider_name = name,
                                       .provider_version = version,
                                       .tls_min_version = std::string{kFipsMinTlsVersionName}};

  OSSL_PROVIDER_unload(provider);

  // Module identity belongs in the log as well as in SHOW FIPS INFO.
  spdlog::info(
      "FIPS mode enabled (OpenSSL provider '{}' version {}).", fips_status.provider_name, fips_status.provider_version);

  // Published last, so an enabled status implies the provider was actually
  // verified rather than merely requested.
  utils::SetFipsStatus(std::move(fips_status));
}
#endif

SSLInit::SSLInit() {
  // Initialize the OpenSSL library.
  SSL_library_init();
  OpenSSL_add_ssl_algorithms();
  SSL_load_error_strings();
  ERR_load_crypto_strings();

  // Ignore SIGPIPE.
  MG_ASSERT(utils::SignalIgnore(utils::Signal::Pipe), "Couldn't ignore SIGPIPE!");

  SetupThreading();
}

SSLInit::~SSLInit() { Cleanup(); }
}  // namespace memgraph::communication
