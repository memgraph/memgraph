#pragma once

#include <vector>
#include <atomic>
#include <sys/inotify.h>
#include <chrono>
// TODO: remove experimental from here once that becomes possible
#include <experimental/filesystem>

#include "utils/exceptions/basic_exception.hpp"
#include "utils/exceptions/not_yet_implemented.hpp"

// TODO: remove experimental from here once it becomes possible
namespace fs = std::experimental::filesystem;

namespace utils
{

using ms = std::chrono::milliseconds;

enum class FSEvent : int
{
    Created  = 0x1,
    Modified = 0x2,
    Deleted  = 0x4
};

class FSWatcherException : public BasicException
{
public:
    using BasicException::BasicException;
};

/*
 * File System Watcher
 *
 * parameters:
 *     * interval
 */
class FSWatcher
{
    using wd_t = int;
    using callback_t = std::function<void(fs::path &path, FSEvent event)>;
    // <path, watch descriptor, callback>
    using entry_t = std::tuple<fs::path, wd_t, callback_t>;

public:
    FSWatcher(ms interval = 100ms) : interval_(interval) { init(); }
    ~FSWatcher() = default;

    // copy and move constructors and assignemnt operators are deleted because
    // std::atomic can't be copied or moved
    FSWatcher(const FSWatcher &other) = delete;
    FSWatcher(FSWatcher &&other)      = delete;
    FSWatcher &operator=(const FSWatcher &) = delete;
    FSWatcher &operator=(FSWatcher &&) = delete;

    void init()
    {
        inotify_fd_ = inotify_init();
        if (inotify_fd_ == -1)
            throw FSWatcherException("Unable to initialize inotify");
    }

    void watch(const fs::path &path, callback_t callback)
    {
        int wd = inotify_add_watch(inotify_fd_, path.c_str(), IN_ALL_EVENTS);
        if (wd == -1)
            throw FSWatcherException("Unable to add watch");
        entries_.emplace_back(std::make_tuple(path, wd, callback));
    }

    void unwatch(const fs::path &path)
    {
        // iterate through all entries and remove specified watch descriptor
        for (auto &entry : entries_)
        {
            // if paths are not equal pass
            if (std::get<fs::path>(entry) != path)
                continue;
            
            // get watch descriptor and remove it from the watching
            auto status = inotify_rm_watch(inotify_fd_, std::get<wd_t>(entry));
            if (status == -1)
                throw FSWatcherException("Unable to remove watch");
        }
    }

    void unwatchAll()
    {
        // iterate through all entries and remove all watch descriptors
        for (auto &entry : entries_)
        {
            auto status = inotify_rm_watch(inotify_fd_, std::get<wd_t>(entry));
            if (status == -1)
                throw FSWatcherException("Unable to remove watch");
        }
    }

    /*
     * Start watching
     */
    void start()
    {
        is_running_.store(true);

        // TODO: run on a separate thread

        while (is_running_.load())
        {
            // TODO: read
        }

        throw NotYetImplemented("FSWatch::start");
    }

    /*
     * Stop watching
     */
    void stop()
    {
        is_running_.store(false);
    }

private:
    int inotify_fd_;
    std::atomic<bool> is_running_;
    ms interval_;
    std::vector<entry_t> entries_;
};
}
