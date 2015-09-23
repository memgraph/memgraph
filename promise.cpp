#include <iostream>
#include <atomic>

#include "server/uv/uv.hpp"
#include "server/http/http.hpp"

template <class T, class... Args>
class Promise
{
public:
    Promise(std::function<T(Args...)> f, Args&&... args)
    {
        result = f(std::forward<Args>(args)...);
    }

    template <class R>
    Promise<R, T> then(std::function<R(T)> f)
    {
        return Promise(f, std::forward<T>(result));
    }

    T get()
    {
        return result;
    }

private:
    

    T result;
    std::atomic<bool> completed;
};

class TaskPool
{
    template <class R, class... Args>
    class Task
    {
    public:
        using task_t = Task<R, Args...>;
        using work_f = std::function<R(Args...)>;
        using after_work_f = std::function<void(R)>;

        Task(work_f work, after_work_f callback)
            : work(work), callback(callback)
        {
            req.data = this;
        }

        void launch(uv::UvLoop& loop)
        {
            uv_queue_work(loop, &req, work_cb, after_work_cb);
        }

    private:
        std::function<R(Args...)> work;
        std::function<void(R&)> callback;

        R result;

        uv_work_t req;

        static void work_cb(uv_work_t* req)
        {
            auto& task = *reinterpret_cast<task_t*>(req->data);
        }

        static void after_work_cb(uv_work_t* req, int)
        {
            auto task = reinterpret_cast<task_t>(req->data);
            delete task;
        }
    };

public:
    TaskPool(uv::UvLoop& loop) : loop(loop) {}

    template <class R, class...Args>
    void launch(std::function<R(Args...)> func,
             std::function<void(R&)> callback)
    {
        auto task = new Task<R, Args...>(func, callback);
        task->launch(loop);
    }

private:
    uv::UvLoop& loop;
};



int main(void)
{
    uv::UvLoop loop;
    TaskPool tp(loop);
    
    tp.launch([](void) -> int { return 3 }, 
              [](int x) -> void { std::cout << x << std::endl; });

//    http::HttpServer server(loop);
//
//    http::Ipv4 ip("0.0.0.0", 3400);
//
//    server.listen(ip, [](http::Request& req, http::Response& res) {
//
//
//        
//        res.send(req.url);
//    });
//
//    loop.run(uv::UvLoop::Mode::Default);


    return 0;
}
