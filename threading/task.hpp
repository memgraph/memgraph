#ifndef MEMGRAPH_THREADING_TASK_HPP
#define MEMGRAPH_THREADING_TASK_HPP

#include <iostream>

#include "pool.hpp"
#include "io/uv/uvloop.hpp"
#include "utils/placeholder.hpp"

class Task
{
    template <class T>
    using work_t = std::function<T(void)>;

    template <class T>
    using after_work_t = std::function<void(T)>;

    template <class T>
    struct Work
    {

        Work(uv::UvLoop& loop, work_t<T> work, after_work_t<T> after_work)
            : work(std::move(work)), after_work(std::move(after_work))
        {
            uv_async_init(loop, &this->async, async_cb);
        }

        void operator()()
        {
            result.set(std::move(work()));

            async.data = static_cast<void*>(this);
            uv_async_send(&this->async);
        }

        work_t<T> work;
        after_work_t<T> after_work;

        Placeholder<T> result;

        uv_async_t async;

    private:
        static void async_cb(uv_async_t* handle)
        {
            auto work = static_cast<Work<T>*>(handle->data);

            work->after_work(std::move(work->result.get()));

            auto async_as_handle = reinterpret_cast<uv_handle_t*>(handle);

            uv_close(async_as_handle, [](uv_handle_t* handle) {
                auto work = static_cast<Work<T>*>(handle->data);
                delete work;
            });
        }
    };

public:
    using sptr = std::shared_ptr<Task>;

    Task(uv::UvLoop::sptr loop, Pool::sptr pool) : loop(loop), pool(pool) {}

    Task(Task&) = delete;
    Task(Task&&) = delete;

    template <class F1, class F2>
    void run(F1&& work, F2&& after_work)
    {
        using T = decltype(work());

        auto w = new Work<T>(*loop, std::forward<F1>(work),
                             std::forward<F2>(after_work));

        pool->run([w]() { w->operator()(); });
    }

private:
    uv::UvLoop::sptr loop;
    Pool::sptr pool;
};

#endif
