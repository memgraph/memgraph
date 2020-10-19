#include "init.hpp"

#include <glog/logging.h>

#include <openssl/bio.h>
#include <openssl/err.h>
#include <openssl/ssl.h>
#include <thread>

#include "utils/signals.hpp"
#include "utils/spin_lock.hpp"

namespace communication {

namespace {
// OpenSSL before 1.1 did not have a out-of-the-box multithreading support
// You need to manually define locks, locking callbacks and id function.
// https://stackoverflow.com/a/42856544
// https://wiki.openssl.org/index.php/Library_Initialization#libssl_Initialization
#if OPENSSL_VERSION_NUMBER < 0x10100000L
std::vector<utils::SpinLock> mutex;

void LockingFunction(int mode, int n, const char *file, int line) {
  if (mode & CRYPTO_LOCK) {
    mutex[n].lock();
  } else {
    mutex[n].unlock();
  }
}

unsigned long IdFunction() {
  return (unsigned long)std::hash<std::thread::id>()(
      std::this_thread::get_id());
}

void SetupThreading() {
  mutex.resize(CRYPTO_num_locks());
  CRYPTO_set_id_callback(IdFunction);
  CRYPTO_set_locking_callback(LockingFunction);
}

void Cleanup() {
  CRYPTO_set_id_callback(nullptr);
  CRYPTO_set_locking_callback(nullptr);
  mutex.clear();
}
#else
void SetupThreading() {}
void Cleanup() {}
#endif
}  // namespace

SSLInit::SSLInit() {
  // Initialize the OpenSSL library.
  SSL_library_init();
  OpenSSL_add_ssl_algorithms();
  SSL_load_error_strings();
  ERR_load_crypto_strings();

  // Ignore SIGPIPE.
  CHECK(utils::SignalIgnore(utils::Signal::Pipe)) << "Couldn't ignore SIGPIPE!";

  SetupThreading();
}

SSLInit::~SSLInit() { Cleanup(); }
}  // namespace communication
