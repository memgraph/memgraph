#pragma once

#include <vector>
#include <memory>
#include <chrono>
#include <future>

#include "worker.hpp"

template <class W>
class WorkerRunner
{
public:
    WorkerRunner(const std::string& query)
        : worker(std::make_unique<W>(query)) {}

    W* operator->() { return worker.get(); }
    const W* operator->() const { return worker.get(); }

    void operator()(std::chrono::duration<double> duration)
    {
        std::packaged_task<WorkerResult()> task([this, duration]() {
            return this->worker->benchmark(duration);
        });

        result = std::move(task.get_future());
        std::thread(std::move(task)).detach();
    }

    std::unique_ptr<W> worker;
    std::future<WorkerResult> result;
};

struct Result
{
    std::chrono::duration<double> elapsed;
    std::vector<uint64_t> requests;
};

Result benchmark(const std::string& host, const std::string& port,
                 int threads, int connections,
                 std::chrono::duration<double> duration,
                 const std::vector<std::string>& queries)
{
    std::vector<WorkerRunner<CypherWorker>> workers;

    for(int i = 0; i < threads; ++i)
        workers.emplace_back(queries[i]);

    for(int i = 0; i < threads * connections; ++i)
        workers[i % threads]->connect(host, port);

    for(auto& worker : workers)
        worker(duration);

    std::vector<WorkerResult> results;

    for(auto& worker : workers)
    {
        worker.result.wait();
        results.push_back(worker.result.get());
    }

    auto start = std::min_element(results.begin(), results.end(),
        [](auto a, auto b) { return a.start < b.start; })->start;

    auto end = std::max_element(results.begin(), results.end(),
        [](auto a, auto b) { return a.end < b.end; })->end;

    std::vector<uint64_t> qps;

    for(auto& result : results)
        qps.push_back(result.requests);

    return {end - start, qps};
}
