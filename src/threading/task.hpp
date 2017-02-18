#pragma once

#include <iostream>

#include "io/uv/uvloop.hpp"
#include "pool.hpp"
#include "utils/placeholder.hpp"

class Task {
  template <class T>
  using work_t = std::function<T(void)>;

  template <class T>
  using after_work_t = std::function<void(T)>;

  template <class T>
  struct Work {
    Work(work_t<T> work, after_work_t<T> after_work)
        : work(std::move(work)), after_work(std::move(after_work)) {
      req.data = static_cast<void*>(this);
    }

    void operator()() { result.set(std::move(work())); }

    work_t<T> work;
    after_work_t<T> after_work;

    Placeholder<T> result;

    uv_work_t req;

    static void work_cb(uv_work_t* req) {
      auto& work = *static_cast<Work<T>*>(req->data);
      work();
    }

    static void after_work_cb(uv_work_t* req, int) {
      auto work = static_cast<Work<T>*>(req->data);

      work->after_work(std::move(work->result.get()));

      delete work;
    }
  };

 public:
  using sptr = std::shared_ptr<Task>;

  Task(uv::UvLoop::sptr loop) : loop(loop) {}

  Task(Task&) = delete;
  Task(Task&&) = delete;

  template <class F1, class F2>
  void run(F1&& work, F2&& after_work) {
    using T = decltype(work());

    auto w = new Work<T>(std::forward<F1>(work), std::forward<F2>(after_work));

    uv_queue_work(*loop, &w->req, Work<T>::work_cb, Work<T>::after_work_cb);
  }

 private:
  uv::UvLoop::sptr loop;
};
