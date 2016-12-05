#pragma once

#include <vector>
#include <atomic>
#include <sys/inotify.h>
#include <chrono>
#include <thread>
// TODO: remove experimental from here once that becomes possible
#include <experimental/filesystem>

#include "utils/exceptions/basic_exception.hpp"
#include "utils/exceptions/not_yet_implemented.hpp"

// TODO: remove experimental from here once it becomes possible
namespace fs = std::experimental::filesystem;

namespace utils
{

using ms = std::chrono::milliseconds;

/*
 * File System Event Types
 */
enum class FSEvent : int
{
    Created  = 0x1,
    Modified = 0x2,
    Deleted  = 0x4
};

/*
 * Custom exception 
 */
class FSWatcherException : public BasicException
{
public:
    using BasicException::BasicException;
};

/*
 * File System Watcher
 *
 * The idea is to create wrapper of inotify or any other file system
 * notificatino system.
 *
 * parameters:
 *     * interval - time between two check for the new file system events
 */
class FSWatcher
{
    // watch descriptor type
    using wd_t = int;

    // callback type (the code that will be notified will be notified
    // through callback of this type
    using callback_t = std::function<void(fs::path &path, FSEvent event)>;

    // storage type for all subscribers
    // <path, watch descriptor, callback>
    using entry_t = std::tuple<fs::path, wd_t, callback_t>;

public:
    FSWatcher(ms interval = ms(100)) : interval_(interval) { init(); }
    ~FSWatcher() = default;

    // copy and move constructors and assignemnt operators are deleted because
    // std::atomic can't be copied or moved
    FSWatcher(const FSWatcher &other) = delete;
    FSWatcher(FSWatcher &&other)      = delete;
    FSWatcher &operator=(const FSWatcher &) = delete;
    FSWatcher &operator=(FSWatcher &&) = delete;

    /*
     * Initialize file system notification system.
     */
    void init()
    {
        inotify_fd_ = inotify_init();
        if (inotify_fd_ == -1)
            throw FSWatcherException("Unable to initialize inotify");
    }

    /*
     * Add subscriber
     *
     * parameters:
     *     * path: path to a file which will be monitored
     *     * type: type of events
     *     * callback: callback that will be called on specified event type
     */
    void watch(const fs::path &path, FSEvent, callback_t callback)
    {
        // TODO: instead IN_ALL_EVENTS pass FSEvent
        int wd = inotify_add_watch(inotify_fd_, path.c_str(), IN_ALL_EVENTS);
        if (wd == -1)
            throw FSWatcherException("Unable to add watch");
        entries_.emplace_back(std::make_tuple(path, wd, callback));
    }

    /*
     * Remove subscriber on specified path and type
     */
    void unwatch(const fs::path &path, FSEvent)
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

    /*
     * Remove all subscribers.
     */
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
        throw NotYetImplemented("FSWatch::start");

        is_running_.store(true);

        dispatch_thread_ = std::thread([this]() {
            while (is_running_.load()) {
                std::this_thread::sleep_for(interval_);
                // TODO implement file system event processing
            }
        });
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
    std::thread dispatch_thread_;
};
}
