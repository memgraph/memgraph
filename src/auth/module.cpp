// Copyright 2024 Memgraph Ltd.
//
// Licensed as a Memgraph Enterprise file under the Memgraph Enterprise
// License (the "License"); by using this file, you agree to be bound by the terms of the License, and you may not use
// this file except in compliance with the License. You may obtain a copy of the License at https://memgraph.com/legal.
//
//

#include "auth/module.hpp"

#include <cerrno>
#include <chrono>
#include <csignal>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <thread>

#include <fcntl.h>
#include <libgen.h>
#include <linux/limits.h>
#include <poll.h>
#include <pwd.h>
#include <sched.h>
#include <seccomp.h>
#include <sys/resource.h>
#include <sys/syscall.h>
#include <sys/time.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

#include <fmt/format.h>
#include <gflags/gflags.h>

#include "utils/logging.hpp"

#include <mutex>

namespace {

/////////////////////////////////////////////////////////////////////////
// Constants used for starting and communicating with the target process.
/////////////////////////////////////////////////////////////////////////

const int kPipeReadEnd = 0;
const int kPipeWriteEnd = 1;

const int kCommunicationToModuleFd = 1000;
const int kCommunicationFromModuleFd = 1001;

const int kTerminateTimeoutSec = 5;

///////////////////////////////////////////
// char** wrapper used for C library calls.
///////////////////////////////////////////

const int kCharppMaxElements = 4096;

class CharPP final {
 public:
  CharPP() { memset(data_, 0, sizeof(char *) * kCharppMaxElements); }

  ~CharPP() {
    for (size_t i = 0; i < size_; ++i) {
      free(data_[i]);
    }
  }

  CharPP(const CharPP &) = delete;
  CharPP(CharPP &&) = delete;
  CharPP &operator=(const CharPP &) = delete;
  CharPP &operator=(CharPP &&) = delete;

  void Add(const char *value) {
    if (size_ == kCharppMaxElements) return;
    int len = strlen(value);
    char *item = static_cast<char *>(malloc(sizeof(char) * (len + 1)));
    if (item == nullptr) return;
    memcpy(item, value, len);
    item[len] = 0;
    data_[size_++] = item;
  }

  void Add(const std::string &value) { Add(value.c_str()); }

  char **Get() { return data_; }

 private:
  char *data_[kCharppMaxElements];
  size_t size_{0};
};

////////////////////////////////////
// Security functions and constants.
////////////////////////////////////

const std::vector<int> kSeccompSyscallsBlacklist = {
    SCMP_SYS(mknod),         SCMP_SYS(mount),        SCMP_SYS(setuid),
    SCMP_SYS(stime),         SCMP_SYS(ptrace),       SCMP_SYS(setgid),
    SCMP_SYS(acct),          SCMP_SYS(umount),       SCMP_SYS(setpgid),
    SCMP_SYS(chroot),        SCMP_SYS(setreuid),     SCMP_SYS(setregid),
    SCMP_SYS(sethostname),   SCMP_SYS(settimeofday), SCMP_SYS(setgroups),
    SCMP_SYS(swapon),        SCMP_SYS(reboot),       SCMP_SYS(setpriority),
    SCMP_SYS(ioperm),        SCMP_SYS(syslog),       SCMP_SYS(iopl),
    SCMP_SYS(vhangup),       SCMP_SYS(vm86old),      SCMP_SYS(swapoff),
    SCMP_SYS(setdomainname), SCMP_SYS(adjtimex),     SCMP_SYS(init_module),
    SCMP_SYS(delete_module), SCMP_SYS(setfsuid),     SCMP_SYS(setfsgid),
    SCMP_SYS(setresuid),     SCMP_SYS(vm86),         SCMP_SYS(setresgid),
    SCMP_SYS(capset),        SCMP_SYS(setreuid),     SCMP_SYS(setregid),
    SCMP_SYS(setgroups),     SCMP_SYS(setresuid),    SCMP_SYS(setresgid),
    SCMP_SYS(setuid),        SCMP_SYS(setgid),       SCMP_SYS(setfsuid),
    SCMP_SYS(setfsgid),      SCMP_SYS(pivot_root),   SCMP_SYS(sched_setaffinity),
    SCMP_SYS(clock_settime), SCMP_SYS(kexec_load),   SCMP_SYS(mknodat),
    SCMP_SYS(unshare),
#ifdef SYS_seccomp
    SCMP_SYS(seccomp),
#endif
};

bool SetupSeccomp() {
  // Initialize the seccomp context.
  scmp_filter_ctx ctx;
  ctx = seccomp_init(SCMP_ACT_ALLOW);
  if (ctx == nullptr) return false;

  // Add all general blacklist rules.
  for (auto syscall_num : kSeccompSyscallsBlacklist) {
    if (seccomp_rule_add(ctx, SCMP_ACT_KILL, syscall_num, 0) != 0) {
      seccomp_release(ctx);
      return false;
    }
  }

  // Load the context for the current process.
  auto ret = seccomp_load(ctx);

  // Free the context and return success/failure.
  seccomp_release(ctx);
  return ret == 0;
}

bool SetLimit(int resource, rlim_t n) {
  struct rlimit limit;
  limit.rlim_cur = limit.rlim_max = n;
  return setrlimit(resource, &limit) == 0;
}

////////////////////////////////////////////////////
// Target function used to start the module process.
////////////////////////////////////////////////////

int Target(void *arg) {
  // NOTE: (D)LOG shouldn't be used here because it wasn't initialized in this
  // process and something really bad could happen.

  // Get a pointer to the passed arguments.
  auto *ta = reinterpret_cast<memgraph::auth::TargetArguments *>(arg);

  // Redirect `stdin` to `/dev/null`.
  int fd = open("/dev/null", O_RDONLY | O_CLOEXEC);
  if (fd == -1) {
    std::cerr << "Couldn't open \"/dev/null\" for auth module stdin because of: " << strerror(errno) << " (" << errno
              << ")!" << std::endl;
    return EXIT_FAILURE;
  }
  if (dup2(fd, STDIN_FILENO) != STDIN_FILENO) {
    std::cerr << "Couldn't attach \"/dev/null\" to auth module stdin because of: " << strerror(errno) << " (" << errno
              << ")!" << std::endl;
    return EXIT_FAILURE;
  }

  // Change the current directory to the module directory.
  if (chdir(ta->module_executable_path.parent_path().c_str()) != 0) {
    std::cerr << "Couldn't change directory to " << ta->module_executable_path.parent_path()
              << " for auth module stdin because of: " << strerror(errno) << " (" << errno << ")!" << std::endl;
    return EXIT_FAILURE;
  }

  // Create the executable CharPP object.
  CharPP exe;
  exe.Add(ta->module_executable_path);

  // Create the environment CharPP object.
  CharPP env;
  for (uint64_t i = 0; environ[i] != nullptr; ++i) {
    env.Add(environ[i]);
  }

  // Connect the communication input pipe.
  if (dup2(ta->pipe_to_module, kCommunicationToModuleFd) != kCommunicationToModuleFd) {
    std::cerr << "Couldn't attach communication to module pipe to auth module "
                 "because of: "
              << strerror(errno) << " (" << errno << ")!" << std::endl;
    return EXIT_FAILURE;
  }

  // Connect the communication output pipe.
  if (dup2(ta->pipe_from_module, kCommunicationFromModuleFd) != kCommunicationFromModuleFd) {
    std::cerr << "Couldn't attach communication from module pipe to auth "
                 "module because of: "
              << strerror(errno) << " (" << errno << ")!" << std::endl;
    return EXIT_FAILURE;
  }

  // Disable core dumps.
  if (!SetLimit(RLIMIT_CORE, 0)) {
    std::cerr << "Couldn't disable core dumps for auth module!" << std::endl;
    // This isn't a fatal error.
  }

  // Ignore SIGINT.
  struct sigaction action;
  // `sa_sigaction` must be cleared before `sa_handler` is set because on some
  // platforms the two are a union.
  action.sa_sigaction = nullptr;
  action.sa_handler = SIG_IGN;
  sigemptyset(&action.sa_mask);
  action.sa_flags = 0;
  if (sigaction(SIGINT, &action, nullptr) != 0) {
    std::cerr << "Couldn't ignore SIGINT for auth module because of: " << strerror(errno) << " (" << errno << ")!"
              << std::endl;
    return EXIT_FAILURE;
  }

  // Setup seccomp.
  if (!SetupSeccomp()) {
    std::cerr << "Couldn't enable seccomp for auth module!" << std::endl;
    // This isn't a fatal error.
  }

  execve(*exe.Get(), exe.Get(), env.Get());

  // If the `execve` call succeeded then the process will exit from that call
  // and won't reach this piece of code ever.
  std::cerr << "Couldn't start auth module because of: " << strerror(errno) << " (" << errno << ")!" << std::endl;

  return EXIT_FAILURE;
}

/////////////////////////////////////////////////////
// Function used to send data to the started process.
/////////////////////////////////////////////////////

/// The data that is being sent to the module process is always a newline
/// terminated JSON encoded string.

bool PutData(int fd, const nlohmann::json &data, int timeout_millisec) {
  std::string encoded;
  try {
    encoded = data.dump();
  } catch (const nlohmann::json::type_error &) {
    return false;
  }

  if (encoded.empty()) return false;
  if (*encoded.rbegin() != '\n') {
    encoded.push_back('\n');
  }

  size_t put = 0;
  while (put < encoded.size()) {
    struct pollfd desc;
    desc.fd = fd;
    desc.events = POLLOUT;
    desc.revents = 0;
    if (poll(&desc, 1, timeout_millisec) <= 0) {
      return false;
    }
    int ret = write(fd, encoded.data() + put, encoded.size() - put);
    if (ret > 0) {
      put += ret;
    } else if (ret == 0 || errno != EINTR) {
      return false;
    }
  }
  return true;
}

//////////////////////////////////////////////////////
// Function used to get data from the started process.
//////////////////////////////////////////////////////

/// The data that is being received from the module process is always a newline
/// terminated JSON encoded string. The JSON encoded string must be in a single
/// line and all newline characters may only appear encoded as a part of a
/// character string.

nlohmann::json GetData(int fd, int timeout_millisec) {
  std::string data;
  while (true) {
    struct pollfd desc;
    desc.fd = fd;
    desc.events = POLLIN;
    desc.revents = 0;
    if (poll(&desc, 1, timeout_millisec) <= 0) {
      return {};
    }
    char ch;
    int ret = read(fd, &ch, 1);
    if (ret > 0) {
      data += ch;
      if (ch == '\n') break;
    } else if (ret == 0 || errno != EINTR) {
      return {};
    }
  }
  try {
    return nlohmann::json::parse(data);
  } catch (const nlohmann::json::parse_error &) {
    return {};
  }
}

}  // namespace

namespace memgraph::auth {
Module::Module(const std::filesystem::path &module_executable_path) {
  if (!module_executable_path.empty()) {
    module_executable_path_ = std::filesystem::absolute(module_executable_path);
  }
}

bool Module::Startup() {
  // Check whether the process is alive.
  if (pid_ != -1 && waitpid(pid_, &status_, WNOHANG | WUNTRACED) == 0) {
    return true;
  }

  // Cleanup leftover state.
  Shutdown();

  // Setup communication pipes.
  if (pipe2(pipe_to_module_, O_CLOEXEC) != 0) {
    spdlog::error(
        "Couldn't create communication pipe from the database to "
        "the auth module!");
    return false;
  }
  if (pipe2(pipe_from_module_, O_CLOEXEC) != 0) {
    spdlog::error(
        "Couldn't create communication pipe from the auth module to "
        "the database!");
    close(pipe_to_module_[kPipeReadEnd]);
    close(pipe_to_module_[kPipeWriteEnd]);
    return false;
  }

  // Find the top of the stack.
  uint8_t *stack_top = stack_.get() + kStackSizeBytes;

  // Set the target arguments.
  target_arguments_->module_executable_path = module_executable_path_;
  target_arguments_->pipe_to_module = pipe_to_module_[kPipeReadEnd];
  target_arguments_->pipe_from_module = pipe_from_module_[kPipeWriteEnd];

  // Create the process.
  pid_ = clone(Target, stack_top, CLONE_VFORK, target_arguments_.get());
  if (pid_ == -1) {
    spdlog::error("Couldn't start the auth module process!");
    close(pipe_to_module_[kPipeReadEnd]);
    close(pipe_to_module_[kPipeWriteEnd]);
    close(pipe_from_module_[kPipeReadEnd]);
    close(pipe_from_module_[kPipeWriteEnd]);
    return false;
  }

  // Check whether the process is still running.
  if (waitpid(pid_, &status_, WNOHANG | WUNTRACED) != 0) {
    spdlog::error("The auth module process couldn't be started!");
    return false;
  }

  // Close pipes that won't be used from the master process.
  close(pipe_to_module_[kPipeReadEnd]);
  close(pipe_from_module_[kPipeWriteEnd]);

  return true;
}

nlohmann::json Module::Call(const nlohmann::json &params, int timeout_millisec) {
  auto guard = std::lock_guard{lock_};

  if (!params.is_object()) return {};

  // Ensure that the module is up and running.
  if (!Startup()) return {};

  // Put the request to the module process.
  if (!PutData(pipe_to_module_[kPipeWriteEnd], params, timeout_millisec)) {
    spdlog::error("Couldn't send data to the auth module process!");
    return {};
  }

  // Get the response from the module process.
  auto ret = GetData(pipe_from_module_[kPipeReadEnd], timeout_millisec);
  if (ret.is_null()) {
    spdlog::error("Couldn't receive data from the auth module process!");
    return {};
  }
  if (!ret.is_object()) {
    spdlog::error("Data received from the auth module is of wrong type!");
    return {};
  }
  return ret;
}

bool Module::IsUsed() const { return !module_executable_path_.empty(); }

void Module::Shutdown() {
  if (pid_ == -1) return;

  // Try to terminate the process gracefully in `kTerminateTimeoutSec`.
  std::this_thread::sleep_for(std::chrono::milliseconds(100));
  for (int i = 0; i < kTerminateTimeoutSec * 10; ++i) {
    spdlog::info("Terminating the auth module process with pid {}", pid_);
    kill(pid_, SIGTERM);
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    int ret = waitpid(pid_, &status_, WNOHANG | WUNTRACED);
    if (ret == pid_ || ret == -1) {
      break;
    }
  }

  // If the process is still alive, kill it and wait for it to die.
  if (waitpid(pid_, &status_, WNOHANG | WUNTRACED) == 0) {
    spdlog::warn("Killing the auth module process with pid {}", pid_);
    kill(pid_, SIGKILL);
    waitpid(pid_, &status_, 0);
  }

  // Close leftover open pipes.
  // We have to be careful to close only the leftover open pipes (the
  // pipe_to_module WriteEnd and pipe_from_module ReadEnd), the other two ends
  // were closed in the function that created them because they aren't used from
  // the master process (they are only used from the module process).
  close(pipe_to_module_[kPipeWriteEnd]);
  close(pipe_from_module_[kPipeReadEnd]);

  // Reset variables.
  pid_ = -1;
  status_ = 0;
  pipe_to_module_[kPipeReadEnd] = -1;
  pipe_to_module_[kPipeWriteEnd] = -1;
  pipe_from_module_[kPipeReadEnd] = -1;
  pipe_from_module_[kPipeWriteEnd] = -1;
}

Module::~Module() { Shutdown(); }

}  // namespace memgraph::auth
