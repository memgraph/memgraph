#include "integrations/kafka/transform.hpp"

#include <chrono>
#include <cstdlib>
#include <cstring>
#include <thread>

#include <errno.h>
#include <fcntl.h>
#include <libgen.h>
#include <linux/limits.h>
#include <pwd.h>
#include <sched.h>
#include <seccomp.h>
#include <signal.h>
#include <sys/resource.h>
#include <sys/time.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

#include <fmt/format.h>
#include <gflags/gflags.h>
#include <glog/logging.h>

#include "communication/bolt/v1/value.hpp"
#include "integrations/kafka/exceptions.hpp"
#include "utils/file.hpp"

DEFINE_string(python_interpreter, "/usr/bin/python3",
              "Path to the Python 3.x interpreter that should be used");

namespace {

///////////////////////
// Namespace shortcuts.
///////////////////////

using communication::bolt::Value;
using integrations::kafka::TargetArguments;
using integrations::kafka::TransformExecutionException;
namespace fs = std::experimental::filesystem;

/////////////////////////////////////////////////////////////////////////
// Constants used for starting and communicating with the target process.
/////////////////////////////////////////////////////////////////////////

const int kPipeReadEnd = 0;
const int kPipeWriteEnd = 1;

const int kCommunicationToPythonFd = 1000;
const int kCommunicationFromPythonFd = 1002;

const int kTerminateTimeoutSec = 5;

const std::string kHelperScriptName = "kafka.py";
const std::string kTransformScriptName = "transform.py";

////////////////////
// Helper functions.
////////////////////

fs::path GetTemporaryPath(pid_t pid) {
  return fs::temp_directory_path() / "memgraph" /
         fmt::format("transform_{}", pid);
}

fs::path GetHelperScriptPath() {
  char path[PATH_MAX];
  memset(path, 0, PATH_MAX);
  auto ret = readlink("/proc/self/exe", path, PATH_MAX);
  if (ret < 0) return "";
  return fs::path() / std::string(dirname(path)) / kHelperScriptName;
}

std::string GetEnvironmentVariable(const std::string &name) {
  char *value = secure_getenv(name.c_str());
  if (value == nullptr) return "";
  return {value};
}

///////////////////////////////////////////
// char** wrapper used for C library calls.
///////////////////////////////////////////

const int kCharppMaxElements = 20;

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
    char *item = (char *)malloc(sizeof(char) * (len + 1));
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

const std::vector<int> seccomp_syscalls_allowed = {
    SCMP_SYS(read),
    SCMP_SYS(write),
    SCMP_SYS(close),
    SCMP_SYS(stat),
    SCMP_SYS(fstat),
    SCMP_SYS(lstat),
    SCMP_SYS(poll),
    SCMP_SYS(lseek),
    SCMP_SYS(mmap),
    SCMP_SYS(mprotect),
    SCMP_SYS(munmap),
    SCMP_SYS(brk),
    SCMP_SYS(rt_sigaction),
    SCMP_SYS(rt_sigprocmask),
    SCMP_SYS(rt_sigreturn),
    SCMP_SYS(ioctl),
    SCMP_SYS(pread64),
    SCMP_SYS(pwrite64),
    SCMP_SYS(readv),
    SCMP_SYS(writev),
    SCMP_SYS(access),
    SCMP_SYS(select),
    SCMP_SYS(mremap),
    SCMP_SYS(msync),
    SCMP_SYS(mincore),
    SCMP_SYS(madvise),
    SCMP_SYS(dup),
    SCMP_SYS(dup2),
    SCMP_SYS(pause),
    SCMP_SYS(nanosleep),
    SCMP_SYS(getpid),
    SCMP_SYS(sendfile),
    SCMP_SYS(execve),
    SCMP_SYS(exit),
    SCMP_SYS(uname),
    SCMP_SYS(fcntl),
    SCMP_SYS(fsync),
    SCMP_SYS(fdatasync),
    SCMP_SYS(getdents),
    SCMP_SYS(getcwd),
    SCMP_SYS(readlink),
    SCMP_SYS(gettimeofday),
    SCMP_SYS(getrlimit),
    SCMP_SYS(getrusage),
    SCMP_SYS(getuid),
    SCMP_SYS(getgid),
    SCMP_SYS(geteuid),
    SCMP_SYS(getegid),
    SCMP_SYS(getppid),
    SCMP_SYS(getpgrp),
    SCMP_SYS(rt_sigpending),
    SCMP_SYS(rt_sigtimedwait),
    SCMP_SYS(rt_sigsuspend),
    SCMP_SYS(sched_setparam),
    SCMP_SYS(mlock),
    SCMP_SYS(munlock),
    SCMP_SYS(mlockall),
    SCMP_SYS(munlockall),
    SCMP_SYS(arch_prctl),
    SCMP_SYS(ioperm),
    SCMP_SYS(time),
    SCMP_SYS(futex),
    SCMP_SYS(set_tid_address),
    SCMP_SYS(clock_gettime),
    SCMP_SYS(clock_getres),
    SCMP_SYS(clock_nanosleep),
    SCMP_SYS(exit_group),
    SCMP_SYS(mbind),
    SCMP_SYS(set_mempolicy),
    SCMP_SYS(get_mempolicy),
    SCMP_SYS(migrate_pages),
    SCMP_SYS(openat),
    SCMP_SYS(pselect6),
    SCMP_SYS(ppoll),
    SCMP_SYS(set_robust_list),
    SCMP_SYS(get_robust_list),
    SCMP_SYS(tee),
    SCMP_SYS(move_pages),
    SCMP_SYS(dup3),
    SCMP_SYS(preadv),
    SCMP_SYS(pwritev),
    SCMP_SYS(getrandom),
    SCMP_SYS(sigaltstack),
    SCMP_SYS(gettid),
    SCMP_SYS(tgkill),
    SCMP_SYS(sysinfo),
};

bool SetupSeccomp() {
  // Initialize the seccomp context.
  scmp_filter_ctx ctx;
  ctx = seccomp_init(SCMP_ACT_TRAP);
  if (ctx == NULL) return false;

  // First we deny access to the `open` system call called with `O_WRONLY`,
  // `O_RDWR` and `O_CREAT`.
  if (seccomp_rule_add(ctx, SCMP_ACT_KILL, SCMP_SYS(open), 1,
                       SCMP_A1(SCMP_CMP_MASKED_EQ, O_WRONLY, O_WRONLY)) != 0) {
    seccomp_release(ctx);
    return false;
  }
  if (seccomp_rule_add(ctx, SCMP_ACT_KILL, SCMP_SYS(open), 1,
                       SCMP_A1(SCMP_CMP_MASKED_EQ, O_RDWR, O_RDWR)) != 0) {
    seccomp_release(ctx);
    return false;
  }
  if (seccomp_rule_add(ctx, SCMP_ACT_KILL, SCMP_SYS(open), 1,
                       SCMP_A1(SCMP_CMP_MASKED_EQ, O_CREAT, O_CREAT)) != 0) {
    seccomp_release(ctx);
    return false;
  }
  // Now we allow the `open` system call without the blocked flags.
  if (seccomp_rule_add(
          ctx, SCMP_ACT_ALLOW, SCMP_SYS(open), 1,
          SCMP_A1(SCMP_CMP_MASKED_EQ, O_WRONLY | O_RDWR | O_CREAT, 0)) != 0) {
    seccomp_release(ctx);
    return false;
  }

  // Add all general allow rules.
  for (auto syscall_num : seccomp_syscalls_allowed) {
    if (seccomp_rule_add(ctx, SCMP_ACT_ALLOW, syscall_num, 0) != 0) {
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

///////////////////////////////////////////////////////
// Target function used to start the transform process.
///////////////////////////////////////////////////////

int Target(void *arg) {
  // NOTE: (D)LOG shouldn't be used here because it wasn't initialized in this
  // process and something really bad could happen.

  // Get a pointer to the passed arguments.
  TargetArguments *ta = reinterpret_cast<TargetArguments *>(arg);

  // Redirect `stdin` to `/dev/null`.
  int fd = open("/dev/null", O_RDONLY | O_CLOEXEC);
  if (fd == -1) {
    return EXIT_FAILURE;
  }
  if (dup2(fd, STDIN_FILENO) != STDIN_FILENO) {
    return EXIT_FAILURE;
  }

  // Redirect `stdout` to `/dev/null`.
  fd = open("/dev/null", O_WRONLY | O_CLOEXEC);
  if (fd == -1) {
    return EXIT_FAILURE;
  }
  if (dup2(fd, STDOUT_FILENO) != STDOUT_FILENO) {
    return EXIT_FAILURE;
  }

  // Redirect `stderr` to `/dev/null`.
  fd = open("/dev/null", O_WRONLY | O_CLOEXEC);
  if (fd == -1) {
    return EXIT_FAILURE;
  }
  if (dup2(fd, STDERR_FILENO) != STDERR_FILENO) {
    return EXIT_FAILURE;
  }

  // Create the working directory.
  fs::path working_path = GetTemporaryPath(getpid());
  utils::DeleteDir(working_path);
  if (!utils::EnsureDir(working_path)) {
    return EXIT_FAILURE;
  }

  // Copy all scripts to the working directory.
  if (!utils::CopyFile(GetHelperScriptPath(),
                       working_path / kHelperScriptName)) {
    return EXIT_FAILURE;
  }
  if (!utils::CopyFile(ta->transform_script_path,
                       working_path / kTransformScriptName)) {
    return EXIT_FAILURE;
  }

  // Change the current directory to the working directory.
  if (chdir(working_path.c_str()) != 0) {
    return EXIT_FAILURE;
  }

  // Create the executable CharPP object.
  CharPP exe;
  exe.Add(FLAGS_python_interpreter);
  exe.Add(kHelperScriptName);

  // Create the environment CharPP object.
  CharPP env;
  env.Add(fmt::format("PATH={}", GetEnvironmentVariable("PATH")));
  // TODO (mferencevic): Change this to the effective user.
  env.Add(fmt::format("USER={}", GetEnvironmentVariable("USER")));
  env.Add(fmt::format("HOME={}", working_path));
  env.Add("LANG=en_US.utf8");
  env.Add("LANGUAGE=en_US:en");
  env.Add("PYTHONUNBUFFERED=1");
  env.Add("PYTHONIOENCODING=utf-8");
  env.Add("PYTHONDONTWRITEBYTECODE=1");

  // Connect the communication input pipe.
  if (dup2(ta->pipe_to_python, kCommunicationToPythonFd) !=
      kCommunicationToPythonFd) {
    return EXIT_FAILURE;
  }

  // Connect the communication output pipe.
  if (dup2(ta->pipe_from_python, kCommunicationFromPythonFd) !=
      kCommunicationFromPythonFd) {
    return EXIT_FAILURE;
  }

  // Set process limits.
  // Disable core dumps.
  if (!SetLimit(RLIMIT_CORE, 0)) {
    return EXIT_FAILURE;
  }
  // Disable file creation.
  if (!SetLimit(RLIMIT_FSIZE, 0)) {
    return EXIT_FAILURE;
  }
  // Set process number limit.
  if (!SetLimit(RLIMIT_NPROC, 0)) {
    return EXIT_FAILURE;
  }

  // TODO (mferencevic): Change the user to `nobody`.

  // Setup seccomp.
  if (!SetupSeccomp()) {
    return EXIT_FAILURE;
  }

  execve(*exe.Get(), exe.Get(), env.Get());

  // TODO (mferencevic): Log an error with `errno` about what failed.

  return EXIT_FAILURE;
}

/////////////////////////////////////////////////////////////
// Functions used to send data to the started Python process.
/////////////////////////////////////////////////////////////

/// The data that is being sent to the Python process is always a
/// `std::vector<uint8_t[]>` of data. It is sent in the following way:
///
/// uint32_t number of elements being sent
/// uint32_t element 0 size
/// uint8_t[] element 0 data
/// uint32_t element 1 size
/// uint8_t[] element 1 data
/// ...
///
/// The receiving end of the protocol is implemented in `kafka.py`.

void PutData(int fd, const uint8_t *data, uint32_t size) {
  int ret = 0;
  uint32_t put = 0;
  while (put < size) {
    ret = write(fd, data + put, size - put);
    if (ret > 0) {
      put += ret;
    } else if (ret == 0) {
      throw TransformExecutionException(
          "The communication pipe to the transform process was closed!");
    } else if (errno != EINTR) {
      throw TransformExecutionException(
          "Couldn't put data to the transfrom process!");
    }
  }
}

void PutSize(int fd, uint32_t size) {
  PutData(fd, reinterpret_cast<uint8_t *>(&size), sizeof(size));
}

//////////////////////////////////////////////////////////////
// Functions used to get data from the started Python process.
//////////////////////////////////////////////////////////////

/// The data that is being sent from the Python process is always a
/// `std::vector<std::pair<std::string, Value>>>` of data (array of pairs of
/// query and params). It is sent in the following way:
///
/// uint32_t number of elements being sent
/// uint32_t element 0 query size
/// char[] element 0 query data
/// data[] element 0 params
/// uint32_t element 1 query size
/// char[] element 1 query data
/// data[] element 1 params
/// ...
///
/// When sending the query parameters they have to be further encoded to enable
/// sending of None, Bool, Int, Float, Str, List and Dict objects. The encoding
/// is as follows:
///
/// None: uint8_t type (kTypeNone)
/// Bool: uint8_t type (kTypeBoolFalse or kTypeBoolTrue)
/// Int: uint8_t type (kTypeInt), int64_t value
/// Float: uint8_t type (kTypeFloat), double value
/// Str: uint8_t type (kTypeStr), uint32_t size, char[] data
/// List: uint8_t type (kTypeList), uint32_t size, data[] element 0,
///       data[] element 1, ...
/// Dict: uint8_t type (kTypeDict), uint32_t size, uint32_t element 0 key size,
///       char[] element 0 key data, data[] element 0 value,
///       uint32_t element 1 key size, char[] element 1 key data,
///       data[] element 1 value, ...
///
/// The sending end of the protocol is implemented in `kafka.py`.

const uint8_t kTypeNone = 0x10;
const uint8_t kTypeBoolFalse = 0x20;
const uint8_t kTypeBoolTrue = 0x21;
const uint8_t kTypeInt = 0x30;
const uint8_t kTypeFloat = 0x40;
const uint8_t kTypeStr = 0x50;
const uint8_t kTypeList = 0x60;
const uint8_t kTypeDict = 0x70;

void GetData(int fd, uint8_t *data, uint32_t size) {
  int ret = 0;
  uint32_t got = 0;
  while (got < size) {
    ret = read(fd, data + got, size - got);
    if (ret > 0) {
      got += ret;
    } else if (ret == 0) {
      throw TransformExecutionException(
          "The communication pipe from the transform process was closed!");
    } else if (errno != EINTR) {
      throw TransformExecutionException(
          "Couldn't get data from the transform process!");
    }
  }
}

uint32_t GetSize(int fd) {
  uint32_t size = 0;
  GetData(fd, reinterpret_cast<uint8_t *>(&size), sizeof(size));
  return size;
}

void GetString(int fd, std::string *value) {
  const int kMaxStackBuffer = 8192;
  uint8_t buffer[kMaxStackBuffer];
  uint32_t size = GetSize(fd);
  if (size < kMaxStackBuffer) {
    GetData(fd, buffer, size);
    *value = std::string(reinterpret_cast<char *>(buffer), size);
  } else {
    std::unique_ptr<uint8_t[]> tmp(new uint8_t[size]);
    GetData(fd, tmp.get(), size);
    *value = std::string(reinterpret_cast<char *>(tmp.get()), size);
  }
}

void GetValue(int fd, Value *value) {
  uint8_t type = 0;
  GetData(fd, &type, sizeof(type));
  if (type == kTypeNone) {
    *value = Value();
  } else if (type == kTypeBoolFalse) {
    *value = Value(false);
  } else if (type == kTypeBoolTrue) {
    *value = Value(true);
  } else if (type == kTypeInt) {
    int64_t tmp = 0;
    GetData(fd, reinterpret_cast<uint8_t *>(&tmp), sizeof(tmp));
    *value = Value(tmp);
  } else if (type == kTypeFloat) {
    double tmp = 0.0;
    GetData(fd, reinterpret_cast<uint8_t *>(&tmp), sizeof(tmp));
    *value = Value(tmp);
  } else if (type == kTypeStr) {
    std::string tmp;
    GetString(fd, &tmp);
    *value = Value(tmp);
  } else if (type == kTypeList) {
    std::vector<Value> tmp_vec;
    uint32_t size = GetSize(fd);
    tmp_vec.reserve(size);
    for (uint32_t i = 0; i < size; ++i) {
      Value tmp_value;
      GetValue(fd, &tmp_value);
      tmp_vec.push_back(tmp_value);
    }
    *value = Value(tmp_vec);
  } else if (type == kTypeDict) {
    std::map<std::string, Value> tmp_map;
    uint32_t size = GetSize(fd);
    for (uint32_t i = 0; i < size; ++i) {
      std::string tmp_key;
      Value tmp_value;
      GetString(fd, &tmp_key);
      GetValue(fd, &tmp_value);
      tmp_map.insert({tmp_key, tmp_value});
    }
    *value = Value(tmp_map);
  } else {
    throw TransformExecutionException(
        fmt::format("Couldn't get value of unsupported type 0x{:02x}!", type));
  }
}

}  // namespace

namespace integrations::kafka {

Transform::Transform(const std::string &transform_script_path)
    : transform_script_path_(transform_script_path) {}

bool Transform::Start() {
  // Setup communication pipes.
  if (pipe2(pipe_to_python_, O_CLOEXEC) != 0) {
    DLOG(ERROR) << "Couldn't create communication pipe from cpp to python!";
    return false;
  }
  if (pipe2(pipe_from_python_, O_CLOEXEC) != 0) {
    DLOG(ERROR) << "Couldn't create communication pipe from python to cpp!";
    return false;
  }

  // Find the top of the stack.
  uint8_t *stack_top = stack_.get() + kStackSizeBytes;

  // Set the target arguments.
  target_arguments_->transform_script_path = transform_script_path_;
  target_arguments_->pipe_to_python = pipe_to_python_[kPipeReadEnd];
  target_arguments_->pipe_from_python = pipe_from_python_[kPipeWriteEnd];

  // Create the process.
  pid_ = clone(Target, stack_top, CLONE_VFORK, target_arguments_.get());
  if (pid_ == -1) {
    DLOG(ERROR) << "Couldn't create the communication process!";
    return false;
  }

  // Close pipes that won't be used from the master process.
  close(pipe_to_python_[kPipeReadEnd]);
  close(pipe_from_python_[kPipeWriteEnd]);

  return true;
}

void Transform::Apply(
    const std::vector<std::unique_ptr<RdKafka::Message>> &batch,
    std::function<void(const std::string &,
                       const std::map<std::string, Value> &)>
        query_function) {
  // Check that the process is alive.
  if (waitpid(pid_, &status_, WNOHANG | WUNTRACED) != 0) {
    throw TransformExecutionException("The transform process has died!");
  }

  // Put the `batch` data to the transform process.
  PutSize(pipe_to_python_[kPipeWriteEnd], batch.size());
  for (const auto &item : batch) {
    PutSize(pipe_to_python_[kPipeWriteEnd], item->len());
    PutData(pipe_to_python_[kPipeWriteEnd],
            reinterpret_cast<const uint8_t *>(item->payload()), item->len());
  }

  // Get `query` and `params` data from the transfrom process.
  uint32_t size = GetSize(pipe_from_python_[kPipeReadEnd]);
  for (uint32_t i = 0; i < size; ++i) {
    std::string query;
    Value params;
    GetString(pipe_from_python_[kPipeReadEnd], &query);
    GetValue(pipe_from_python_[kPipeReadEnd], &params);
    query_function(query, params.ValueMap());
  }
}

Transform::~Transform() {
  // Try to terminate the process gracefully in `kTerminateTimeoutSec`.
  std::this_thread::sleep_for(std::chrono::milliseconds(100));
  for (int i = 0; i < kTerminateTimeoutSec * 10; ++i) {
    DLOG(INFO) << "Terminating the transform process with pid " << pid_;
    kill(pid_, SIGTERM);
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    int ret = waitpid(pid_, &status_, WNOHANG | WUNTRACED);
    if (ret == pid_ || ret == -1) {
      break;
    }
  }

  // If the process is still alive, kill it and wait for it to die.
  if (waitpid(pid_, &status_, WNOHANG | WUNTRACED) == 0) {
    DLOG(WARNING) << "Killing the transform process with pid " << pid_;
    kill(pid_, SIGKILL);
    waitpid(pid_, &status_, 0);
  }

  // Delete the working directory.
  if (pid_ != -1) {
    utils::DeleteDir(GetTemporaryPath(pid_));
  }

  // Close leftover open pipes.
  // We have to be careful to close only the leftover open pipes (the
  // pipe_to_python WriteEnd and pipe_from_python ReadEnd), the other two ends
  // were closed in the function that created them because they aren't used from
  // the master process (they are only used from the Python process).
  close(pipe_to_python_[kPipeWriteEnd]);
  close(pipe_from_python_[kPipeReadEnd]);
}

}  // namespace integrations::kafka
