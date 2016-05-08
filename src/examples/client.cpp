#include <functional>
#include <algorithm>
#include <iostream>
#include <random>
#include <vector>
#include <thread>
#include <chrono>
#include <atomic>
#include <future>

#include <unistd.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>

#include "debug/log.hpp"

#include "io/network/epoll.hpp"
#include "io/network/socket.hpp"
#include "io/network/tcp/stream.hpp"
#include "io/network/stream_reader.hpp"

#include "memory/literals.hpp"

using namespace memory::literals;


class RandomString
{
    static constexpr char charset[] =
        "0123456789"
        "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
        "abcdefghijklmnopqrstuvwxyz";

public:
    template <class Rg>
    std::string operator()(Rg&& gen, size_t len)
    {
        auto str = std::string();
        str.reserve(len + 2);
        str.push_back('\'');

        while(str.size() < len)
            str.push_back(charset[rnd(std::forward<Rg>(gen))]);

        str.push_back('\'');
        return str;
    }

private:
    std::uniform_int_distribution<> rnd {0, sizeof(charset) - 1};
};

constexpr char RandomString::charset[];

static std::mt19937 mt {std::random_device{}()};

class CypherPost
{
    static std::string templ;

public:
    CypherPost()
    {
        request.reserve(64_kB);
    }

    void set(const std::string& query)
    {
        request.clear();

        request += "POST /db/data/transaction/commit HTTP/1.1\r\n" \
                   "Host: localhost:7474\r\n" \
                   "Authorization: Basic bmVvNGo6cGFzcw==\r\n" \
                   "Accept: application/json; charset=UTF-8\r\n" \
                   "Content-Type: application/json\r\n" \
                   "Content-Length: ";
        request += std::to_string(query.size() + templ.size() + 4);
        request += "\r\n\r\n";
        request += templ;
        request += query;
        request += "\"}]}";
    }

    operator const std::string&() const { return request; }

private:
    std::string request;
};

std::string CypherPost::templ = "{\"statements\":[{\"statement\":\"";

struct Result
{
    std::chrono::high_resolution_clock::time_point start, end;
    uint64_t requests;
};

class Worker : public io::StreamReader<Worker, io::tcp::Stream>
{
    char buf[65535];
    CypherPost post;

    std::uniform_int_distribution<> random_int;
    RandomString random_string;
    Replacer replacer;

public:
    Worker()
    {
        replacer.replace("#", [&]() { return std::to_string(random_int(mt)); })
                .replace("^", [&]() { return random_string(mt, 15); });
    }

    io::tcp::Stream& on_connect(io::Socket&&)
    {
        // DUMMY, refactor StreamReader to be more generic
        return *streams.back();
    }

    bool connect(const char* name, const char* port)
    {
        auto socket = io::Socket::connect(name, port);

        if(!socket.is_open())
            return false;

        socket.set_non_blocking();

        streams.push_back(std::make_unique<io::tcp::Stream>(std::move(socket)));
        auto& stream = streams.back();

        stream->event.events = EPOLLIN | EPOLLET | EPOLLRDHUP;
        this->add(*stream);

        return true;
    }

    void on_error(io::tcp::Stream& conn)
    {
        LOG_DEBUG("error on socket " << conn.id());
        (void)conn;

        LOG_DEBUG((errno == EBADF));

        std::this_thread::sleep_for(std::chrono::milliseconds(10));
        std::abort();
    }

    void on_wait_timeout() {}

    Buffer on_alloc(io::tcp::Stream&)
    {
        return Buffer { buf, sizeof buf };
    }

    void on_read(io::tcp::Stream& stream, Buffer& buf)
    {
        /* std::cout << "RESPONSE" << std::endl; */
        /* std::cout << std::string(buf.ptr, buf.len) << std::endl; */

        requests++;

        LOG_DEBUG("on_read");
        sendreq(stream.socket);
    }

    void on_close(io::tcp::Stream&) {}

    void sendreq(io::Socket& socket)
    {
        /* auto query = std::string("CREATE (n:Person {id: #, name: ^}) RETURN n"); */
        auto query = std::string("MATCH (n:Person {id: #}) RETURN n");

        post.set(replacer(query));

        /* std::cout << "REQUEST" << std::endl; */
        /* std::cout << static_cast<const std::string&>(post) << std::endl; */
        /* std::cout << "SIZE = " << static_cast<const std::string&>(post).size() << std::endl; */

        auto n = socket.write(static_cast<const std::string&>(post));

        /* std::cout << "Written N = " << n << " bytes." << std::endl; */

        LOG_DEBUG("sent.");
    }

    Result run_benchmark(std::chrono::duration<double> duration)
    {
        LOG_DEBUG("run_benchmark");
        using clock = std::chrono::high_resolution_clock;
        clock::time_point end, start = clock::now();

        for(auto& stream : streams)
            sendreq(stream->socket);

        LOG_DEBUG("sent req to all streams");

        while(true)
        {
            LOG_DEBUG("WAIT AND PROCESS");
            this->wait_and_process_events();

            if((end = clock::now()) - start > duration)
                break;
        }

        return {start, end, requests};
    }

private:
    uint64_t requests {0};
    std::vector<std::unique_ptr<io::tcp::Stream>> streams;
};

class WorkerRunner
{
public:
    WorkerRunner() : worker(std::make_unique<Worker>()) {}

    Worker* operator->() { return worker.get(); }
    const Worker* operator->() const { return worker.get(); }

    void operator()(std::chrono::duration<double> duration)
    {
        std::packaged_task<Result()> task([this, duration]() {
            return this->worker->run_benchmark(duration);
        });

        result = std::move(task.get_future());
        std::thread(std::move(task)).detach();
    }

    std::unique_ptr<Worker> worker;
    std::future<Result> result;
};

std::atomic<bool> alive {true};

int main(int argc, const char* argv[])
{
    using clock = std::chrono::high_resolution_clock;
    using namespace std::chrono;

    if(argc < 4)
        std::abort();

    auto threads = std::stoi(argv[1]);
    auto connections = std::stoi(argv[2]);
    auto duration = std::stoi(argv[3]);

    std::vector<WorkerRunner> workers;

    for(int i = 0; i < threads; ++i)
        workers.emplace_back();

    for(int i = 0; i < connections; ++i)
        workers[i % threads]->connect("localhost", "7474");

    std::vector<Result> results;

    std::cout << "Running queries on " << connections << " connections "
              << "using " << threads << " threads "
              << "for " << duration << " seconds." << std::endl
              << "..." << std::endl;

    for(auto& worker : workers)
        worker(std::chrono::seconds(duration));

    for(auto& worker : workers)
    {
        worker.result.wait();
        results.push_back(worker.result.get());
    }

    auto start = std::min_element(results.begin(), results.end(),
        [](auto a, auto b) { return a.start < b.start; })->start;

    auto end = std::max_element(results.begin(), results.end(),
        [](auto a, auto b) { return a.end < b.end; })->end;

    auto requests = std::accumulate(results.begin() + 1, results.end(),
        results[0].requests, [](auto acc, auto r) { return acc + r.requests; });

    auto elapsed = (end - start).count() / 1.0e9;

    std::cout << "Total of " << requests << " requests in "
              << elapsed  << "s." << std::endl
              << "Requests/sec: " << int(requests / elapsed)
              << "." << std::endl;

    return 0;
}
