#include <iostream>
#include <signal.h>
#include <thread>
#include <vector>

#include "http/request.hpp"
#include "http/response.hpp"

#include "http/worker.hpp"
#include "socket.hpp"

#ifndef NDEBUG
#define LOG_DEBUG(x) std::cout << x << std::endl;
#else
#define LOG_DEBUG(x)
#endif

std::hash<std::thread::id> hash;

constexpr unsigned K = 128;

std::array<http::Parser<http::Request, http::Response>, K> workers;
std::array<std::thread, K> threads;

std::atomic<bool> alive{true};

void exiting() { LOG_DEBUG("Exiting..."); }

void sigint_handler(int)
{

    exiting();
    std::exit(0);
}

#define MAXEVENTS 64

int main(void)
{
    // std::atexit(exiting);
    signal(SIGINT, sigint_handler);

    for (size_t i = 0; i < workers.size(); ++i)
    {
        auto &w = workers[i];

        threads[i] = std::thread([i, &w]() {
            while (alive)
            {
                LOG_DEBUG("waiting for events on thread " << i);
                w.wait_and_process_events();
            }
        });
    }

    /* size_t WORKERS = std::thread::hardware_concurrency(); */

    /* std::vector<io::Worker> workers; */
    /* workers.resize(WORKERS); */

    /* for(size_t i = 0; i < WORKERS; ++i) */
    /* { */
    /*     workers.push_back(std::move(io::Worker())); */
    /*     workers.back().start(); */
    /* } */

    int idx = 0;

    auto socket = io::Socket::bind("0.0.0.0", "7474");
    socket.set_non_blocking();
    socket.listen(1024);

    int efd, s;
    struct epoll_event event;
    struct epoll_event *events;

    efd = epoll_create1(0);
    if (efd == -1)
    {
        perror("epoll_create");
        abort();
    }

    event.data.fd = socket;
    event.events  = EPOLLIN | EPOLLET;
    s             = epoll_ctl(efd, EPOLL_CTL_ADD, socket, &event);
    if (s == -1)
    {
        perror("epoll_ctl");
        abort();
    }

    /* Buffer where events are returned */
    events = static_cast<struct epoll_event *>(calloc(MAXEVENTS, sizeof event));

    /* The event loop */
    while (1)
    {
        int n, i;

        LOG_DEBUG("acceptor waiting for events");
        n = epoll_wait(efd, events, MAXEVENTS, -1);

        LOG_DEBUG("acceptor recieved " << n << " connection requests");

        for (i = 0; i < n; i++)
        {
            if ((events[i].events & EPOLLERR) ||
                (events[i].events & EPOLLHUP) ||
                (!(events[i].events & EPOLLIN)))
            {
                /* An error has occured on this fd, or the socket is not
                   ready for reading (why were we notified then?) */
                fprintf(stderr, "epoll error\n");
                close(events[i].data.fd);
                continue;
            }

            else if (socket == events[i].data.fd)
            {
                /* We have a notification on the listening socket, which
                   means one or more incoming connections. */
                while (true)
                {
                    LOG_DEBUG("trying to accept connection on thread " << idx);
                    if (!workers[idx].accept(socket)) break;

                    LOG_DEBUG("Accepted a new connection on thread " << idx);
                    idx = (idx + 1) % workers.size();
                    break;
                }
            }
        }
    }

    free(events);

    return 0;
}
