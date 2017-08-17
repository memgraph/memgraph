#pragma once

#include <algorithm>
#include <mutex>
#include <vector>

#include "utils/scheduler.hpp"

/**
 * @brief - Provides execution of jobs in job queue on one thread with 'pause'
 * time between two consecutives starts.
 */
class Executioner {
 public:
  template <typename TRep, typename TPeriod>
  Executioner(const std::chrono::duration<TRep, TPeriod> pause) {
    if (pause != pause.zero())
      scheduler_.Run(pause, std::bind(&Executioner::Execute, this));
  }

  ~Executioner() {
    // Be sure to first stop scheduler because otherwise we might destroy the
    // mutex before the scheduler and that might cause problems since mutex is
    // used in Execute method passed to scheduler along with jobs vector.
    scheduler_.Stop();
  }

  /**
   * @brief - Add function to job queue.
   */
  int64_t RegisterJob(const std::function<void()> &f) {
    {
      std::unique_lock<std::mutex> lock(update_mutex_);
      id_job_pairs_.emplace_back(std::make_pair(++count_, f));
      return id_job_pairs_.back().first;
    }
  }

  /**
   * @brief - Remove id from job queue.
   */
  void UnRegisterJob(const int64_t id) {
    {
      // First wait for execute lock and then for the update lock because
      // execute lock will be unavailable for longer and there is no point in
      // blocking other threads with update lock.
      std::unique_lock<std::mutex> execute_lock(execute_mutex_);
      std::unique_lock<std::mutex> update_lock(update_mutex_);

      for (auto id_job_pair_it = id_job_pairs_.begin();
           id_job_pair_it != id_job_pairs_.end(); ++id_job_pair_it) {
        if (id_job_pair_it->first == id) {
          id_job_pairs_.erase(id_job_pair_it);
          return;
        }
      }
    }
  }

 private:
  /**
   * @brief - Execute method executes jobs from id_job_pairs vector.
   * The reason for doing double locking is the following: we don't want to
   * block creation of new jobs since that will slow down all of memgraph so we
   * use a special lock for job update. Execute lock is here so that we can
   * guarantee that after some job is unregistered it's also stopped.
   */
  void Execute() {
    std::unique_lock<std::mutex> execute_lock(execute_mutex_);
    std::vector<std::pair<int, std::function<void()>>> id_job_pairs;

    // Acquire newest current version of jobs but being careful not to access
    // the vector in corrupt state.
    {
      std::unique_lock<std::mutex> update_lock(update_mutex_);
      id_job_pairs = id_job_pairs_;
    }

    for (auto id_job_pair : id_job_pairs) {
      id_job_pair.second();
    }
  }

  int64_t count_{0};
  std::mutex execute_mutex_;
  std::mutex update_mutex_;
  Scheduler scheduler_;
  std::vector<std::pair<int, std::function<void()>>> id_job_pairs_;
};
