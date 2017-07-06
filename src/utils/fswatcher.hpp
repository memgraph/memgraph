/**
 * Memgraph Ltd
 *
 * File System Watcher
 *
 * @author Marko Budiselic
 */
#pragma once

#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <sys/inotify.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <mutex>
#include <thread>
#include <vector>

#include <experimental/filesystem>
namespace fs = std::experimental::filesystem;

#include <glog/logging.h>

#include "utils/algorithm.hpp"
#include "utils/assert.hpp"
#include "utils/exceptions.hpp"
#include "utils/likely.hpp"
#include "utils/underlying_cast.hpp"

namespace utils {
namespace linux_os {
void set_non_blocking(int fd) {
  auto flags = fcntl(fd, F_GETFL, 0);

  if (UNLIKELY(flags == -1))
    throw BasicException("Cannot read flags from file descriptor.");

  flags |= O_NONBLOCK;

  auto status = fcntl(fd, F_SETFL, flags);

  if (UNLIKELY(status == -1))
    throw BasicException("Can't set NON_BLOCK flag to file descriptor");
}
}

/**
 * Goes from first to last item in a container, if an element satisfying the
 * predicate then the action is going to be executed and the element is going
 * to be shifted to the end of the container.
 *
 * @tparam ForwardIt type of forward iterator
 * @tparam UnaryPredicate type of predicate
 * @tparam Action type of action
 *
 * @return a past-the-end iterator for the new end of the range
 */
template <class ForwardIt, class UnaryPredicate, class Action>
ForwardIt action_remove_if(ForwardIt first, ForwardIt last, UnaryPredicate p,
                           Action a) {
  auto it = std::remove_if(first, last, p);
  if (it == last) return it;
  std::for_each(it, last, a);
  return it;
}

using ms = std::chrono::milliseconds;

/**
 * watch descriptor type, on linux it is int
 */
using os_wd_t = int;
/**
 * machine event mask type, on linux it is inotify mask type which is int
 */
using os_mask_t = int;
// NOTE: for now the types are linux dependent, once when the code base will be
// compiled for another OS figure out the most appropriate solution

/**
 * inotify buffer length (10 events)
 */
using in_event_t = struct inotify_event;
constexpr uint64_t IN_HEADER_SIZE = sizeof(struct inotify_event);
/**
 * The reason why here is 10 is because the memory space for the data
 * has to be upfront reserved (C API). I've picked up 10 because it seems like
 * a reasonable size and doesn't have to be configurable before compile or run
 * time.
 */
constexpr uint64_t IN_BUFF_SLOT_LEN = IN_HEADER_SIZE + NAME_MAX + 1;
constexpr uint64_t IN_BUFF_LEN = 10 * IN_BUFF_SLOT_LEN;

/**
 * File System Event Type - abstraction for underlying event types
 */
enum class FSEventType : os_mask_t {
  Accessed = IN_ACCESS,
  MetadataChanged = IN_ATTRIB,
  CloseWrite = IN_CLOSE_WRITE,
  CloseNowrite = IN_CLOSE_NOWRITE,
  Created = IN_CREATE,
  Deleted = IN_DELETE,
  DeletedSelf = IN_DELETE_SELF,
  Modified = IN_MODIFY,
  Renamed = IN_MOVE_SELF,
  MovedFrom = IN_MOVED_FROM,
  MovedTo = IN_MOVED_TO,
  Close = IN_CLOSE,
  Opened = IN_OPEN,
  Ignored = IN_IGNORED,
  All = Accessed | MetadataChanged | CloseWrite | CloseNowrite | Created |
        Deleted | DeletedSelf | Modified | Renamed | MovedFrom | MovedTo |
        Close | Opened | Ignored
};

inline FSEventType operator|(FSEventType lhs, FSEventType rhs) {
  return (FSEventType)(underlying_cast(lhs) | underlying_cast(rhs));
}

/**
 * @struct FSEventBase
 *
 * Base DTO object.
 *
 * In derived classes the path can represent eather directory or path to a file.
 * In derived classes the type can represent a set of event types or specific
 * event.
 */
struct FSEventBase {
  FSEventBase(const fs::path &path, const FSEventType type)
      : path(path), type(type) {}

  fs::path path;
  FSEventType type;

  operator os_mask_t() { return underlying_cast(type); }
};

/**
 * @struct WatchDescriptor
 *
 * The purpose of this struct is to register new watchers for specific
 * directory and event type.
 */
struct WatchDescriptor : public FSEventBase {
  WatchDescriptor(const fs::path &directory, const FSEventType type)
      : FSEventBase(directory, type) {
    debug_assert(fs::is_directory(path),
                 "The path parameter should be directory");
  }
};

/**
 * @struct FSEvent
 *
 * The purpose of this struct is to carry information about a new fs event.
 * In this case path is a path to the affected file and type is the event type.
 */
struct FSEvent : public FSEventBase {
  FSEvent(const fs::path &directory, const fs::path &filename,
          const FSEventType type)
      : FSEventBase(directory / filename, type) {}
};

/**
 * Custom FSWatcher Exception
 */
class FSWatcherException : public StacktraceException {
 public:
  using StacktraceException::StacktraceException;
};

/**
 * File System Watcher
 *
 * The idea is to create wrapper for inotify or any other file system
 * notification system.
 *
 * The object is not thread safe.
 *
 * parameters:
 *     * interval - time between two checks for the new file system events
 */
class FSWatcher {
  /**
   * callback type (the code that will be notified will be notified
   * through callback of this type
   */
  using callback_t = std::function<void(FSEvent event)>;

 public:
  /**
   * Initialize underlying notification system.
   */
  FSWatcher(ms check_interval = ms(100)) : check_interval_(check_interval) {
    DLOG(INFO) << fmt::format("Inotify header length: {}", IN_HEADER_SIZE);
    DLOG(INFO) << fmt::format("Inotify buffer length: {}", IN_BUFF_LEN);
    inotify_fd_ = inotify_init();
    if (inotify_fd_ == -1)
      throw FSWatcherException("Unable to initialize inotify\n");
    linux_os::set_non_blocking(inotify_fd_);
  }

  ~FSWatcher() {
    DLOG(INFO) << "destructor call";
    unwatchAll();
  }

  /*
   * copy and move constructors and assignemnt operators are deleted because
   * std::atomic can't be copied or moved
   */
  FSWatcher(const FSWatcher &other) = delete;
  FSWatcher(FSWatcher &&other) = delete;
  FSWatcher &operator=(const FSWatcher &) = delete;
  FSWatcher &operator=(FSWatcher &&) = delete;

  /**
   * Add Watcher
   */
  void watch(WatchDescriptor descriptor, callback_t callback) {
    stop();
    os_wd_t wd =
        inotify_add_watch(inotify_fd_, descriptor.path.c_str(), descriptor);
    if (wd == -1) {
      switch (errno) {
        case EACCES:
          throw FSWatcherException(
              "Unable to add watcher. Read access "
              "to the given file is not permitted.");
        case EBADF:
          throw FSWatcherException(
              "Unable to add watcher. The given "
              "file descriptor is not valid.");
        case EFAULT:
          throw FSWatcherException(
              "Unable to add watcher. pathname "
              "points outside of the process's "
              "accessible address space");
        case EINVAL:
          throw FSWatcherException(
              "Unable to add watcher. The given event mask contains no "
              "valid events; or fd is not an inotify file descriptor.");
        case ENAMETOOLONG:
          throw FSWatcherException(
              "Unable to add watcher. pathname is too long.");
        case ENOENT:
          throw FSWatcherException(
              "Unable to add watcher. A directory "
              "component in pathname does not exist "
              "or is a dangling symbolic link.");
        case ENOMEM:
          throw FSWatcherException(
              "Unable to add watcher. Insufficient "
              "kernel memory was available.");
        case ENOSPC:
          throw FSWatcherException(
              "Unable to add watcher. The user limit on the total number "
              "of inotify watches was reached or the kernel failed to "
              "allocate a needed resource.");
        default:
          throw FSWatcherException(
              "Unable to add watcher. Unknown Linux API error.");
      }
    }

    // update existing
    auto it = std::find_if(entries_.begin(), entries_.end(),
                           [wd](Entry &entry) { return wd == entry.os_wd; });
    if (it != entries_.end()) {
      it->descriptor = descriptor;
      it->callback = callback;
    } else {
      entries_.emplace_back(Entry(wd, descriptor, callback));
    }

    DLOG(INFO) << fmt::format("REGISTERED: wd({}) for path({}) and mask ({})",
                              wd, descriptor.path.c_str(),
                              (os_mask_t)(descriptor));
    start();
  }

  /**
   * Remove subscriber on specified path and type.
   *
   * Time complexity: O(n) (where n is number of entries)
   */
  void unwatch(WatchDescriptor descriptor) {
    stop();

    auto it = action_remove_if(
        entries_.begin(), entries_.end(),
        [&descriptor](Entry entry) {
          auto stored_descriptor = entry.descriptor;
          if (stored_descriptor.path != descriptor.path) return false;
          if (stored_descriptor.type != descriptor.type) return false;
          return true;
        },
        [this](Entry entry) { remove_underlaying_watcher(entry); });

    if (it != entries_.end()) entries_.erase(it);

    if (entries_.size() > 0) start();
  }

  /**
   * Removes all subscribers and stops the watching process.
   */
  void unwatchAll() {
    stop();

    if (entries_.size() <= 0) return;

    entries_.erase(action_remove_if(
        entries_.begin(), entries_.end(), [](Entry) { return true; },
        [this](Entry &entry) { remove_underlaying_watcher(entry); }));
  }

  /**
   * Start the watching process.
   */
  void start() {
    if (is_running_.load()) return;

    is_running_.store(true);

    // run separate thread
    dispatch_thread_ = std::thread([this]() {

      DLOG(INFO) << "dispatch thread - start";

      while (is_running_.load()) {
        std::this_thread::sleep_for(check_interval_);

        // read file descriptor and process new events
        // the read call should be non blocking otherwise
        // this thread can easily be blocked forever
        auto n = ::read(inotify_fd_, buffer_, IN_BUFF_LEN);
        if (n == 0) throw FSWatcherException("read() -> 0.");
        if (n == -1) continue;

        DLOG(INFO) << fmt::format("Read {} bytes from inotify fd", (long)n);

        // process all of the events in buffer returned by read()
        for (auto p = buffer_; p < buffer_ + n;) {
          // get in_event
          auto in_event = reinterpret_cast<in_event_t *>(p);
          auto in_event_length = IN_HEADER_SIZE + in_event->len;

          // sometimes inotify system returns event->len that is
          // longer then the length of the buffer
          // TODO: figure out why (it is not easy)
          if (((p - buffer_) + in_event_length) > IN_BUFF_LEN) break;
          // here should be an assertion
          // debug_assert(in_event_length <= IN_BUFF_SLOT_LEN,
          //                "Inotify event length cannot be bigger
          //                than "
          //                "Inotify slot length");

          // skip if in_event is undefined OR is equal to IN_IGNORED
          if ((in_event->len == 0 && in_event->mask == 0) ||
              in_event->mask == IN_IGNORED ||
              in_event_length == IN_HEADER_SIZE)  // skip empty paths
          {
            p += in_event_length;
            continue;
          }

          DLOG(INFO) << fmt::format("LEN: {}, MASK: {}, NAME: {}",
                                    in_event_length, in_event->mask,
                                    in_event->name);

          // find our watch descriptor
          auto entry = find_if(
              entries_.begin(), entries_.end(),
              [in_event](Entry &entry) { return entry.os_wd == in_event->wd; });
          auto &descriptor = entry->descriptor;

          // call user's callback
          entry->callback(FSEvent(descriptor.path, fs::path(in_event->name),
                                  static_cast<FSEventType>(in_event->mask)));

          // move on
          p += in_event_length;
        }
      }

      DLOG(INFO) << "dispatch thread - finish";
    });
  }

  /**
   * Stop the watching process.
   */
  void stop() {
    if (is_running_.load()) {
      is_running_.store(false);
      dispatch_thread_.join();
    }
  }

  /**
   * @return check interval - time between two underlaying check calls
   */
  ms check_interval() const { return check_interval_; }

  /**
   * @return number of entries
   */
  size_t size() const { return entries_.size(); }

 private:
  /**
   * Internal storage for all subscribers.
   *
   * <os watch descriptor, API watch descriptor, callback>
   */
  struct Entry {
    Entry(os_wd_t os_wd, WatchDescriptor descriptor, callback_t callback)
        : os_wd(os_wd), descriptor(descriptor), callback(callback) {}

    os_wd_t os_wd;
    WatchDescriptor descriptor;
    callback_t callback;
  };

  /**
   * Removes the os specific watch descriptor.
   */
  void remove_underlaying_watcher(Entry entry) {
    auto status = inotify_rm_watch(inotify_fd_, entry.os_wd);
    if (status == -1)
      throw FSWatcherException("Unable to remove underlaying watch.");
    else
      DLOG(INFO) << fmt::format("UNREGISTER: fd({}), wd({}), status({})",
                                inotify_fd_, entry.os_wd, status);
  }

  /**
   * inotify file descriptor
   */
  int inotify_fd_;

  /**
   * running flag, has to be atomic because two threads can update and read
   * the value
   */
  std::atomic<bool> is_running_;

  /**
   * interval between the end of events processing and next start of
   * processing
   */
  ms check_interval_;

  /**
   */
  std::vector<Entry> entries_;

  /**
   * thread for events processing
   */
  std::thread dispatch_thread_;

  /**
   * buffer for underlying events (inotify dependent)
   */
  char *buffer_[IN_BUFF_LEN];
};
}
