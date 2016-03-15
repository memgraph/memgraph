#pragma once

#include <functional>
#include <algorithm>
#include <iostream>
#include <random>
#include <vector>
#include <thread>
#include <chrono>
#include <atomic>
#include <future>

#include "simple_client.hpp"

#include "io/network/tcp/stream.hpp"
#include "cypher.hpp"

struct WorkerResult
{
    std::chrono::high_resolution_clock::time_point start, end;
    uint64_t requests;
};

class CypherWorker : public SimpleClient<CypherWorker, io::tcp::Stream>
{
public:
    CypherWorker(const std::string& query)
        : query(query), requests(0) {}

    io::tcp::Stream& on_connect(io::Socket&& socket)
    {
        streams.emplace_back(std::make_unique<io::tcp::Stream>(
            std::forward<io::Socket>(socket)
        ));

        return *streams.back();
    }

    void on_read(io::tcp::Stream& stream, Buffer& buf)
    {
        /* std::cout << "------------------- RESPONSE ------------------" << std::endl; */
        /* std::cout << std::string(buf.ptr, buf.len) << std::endl; */
        /* std::cout << "-----------------------------------------------" << std::endl; */
        /* std::cout << std::endl; */

        requests++;
        send(stream.socket);
    }

    void send(io::Socket& socket)
    {
        // cypherize and send the request
        //socket.write(cypher(queries[idx]));
        auto req = cypher(query);

/*         std::cout << "-------------------- REQUEST ------------------" << std::endl; */
/*         std::cout << req << std::endl; */

        socket.write(req);
    }

    WorkerResult benchmark(std::chrono::duration<double> duration)
    {
        using clock = std::chrono::high_resolution_clock;
        clock::time_point end, start = clock::now();

        for(auto& stream : streams)
            send(stream->socket);

        while(true)
        {
            this->wait_and_process_events();

            if((end = clock::now()) - start > duration)
                break;
        }

        return {start, end, requests};
    }

private:
    std::uniform_int_distribution<> random_int;
    Cypher cypher;

    std::vector<std::unique_ptr<io::tcp::Stream>> streams;
    std::string query;
    uint64_t requests;
};
