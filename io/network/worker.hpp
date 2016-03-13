#pragma once

#include "listener.hpp"
#include "tcp_stream.hpp"

namespace io
{
  const char* response = "HTTP/1.1 200 OK\r\nContent-Length:0\r\nConnection:Keep-Alive\r\n\r\n";

  size_t len = strlen(response);

class Worker : public Listener<Worker>
{
    char buf[64_kB];

public:
    using Listener::Listener;

    bool accept(Socket& socket)
    {
        auto s = socket.accept(nullptr, nullptr);

        if(!s.is_open())
            return false;

        this->add(s);

        return true;
    }

    void on_error(TcpStream* stream)
    {
        delete stream;
    }

    std::atomic<int> requests {0};

    void on_read(TcpStream* stream)
    {
        int done = 0;

        while (1)
        {
            ssize_t count;

            count = read(stream->socket, buf, sizeof buf);
            if (count == -1)
              {
                /* If errno == EAGAIN, that means we have read all
                   data. So go back to the main loop. */
                if (errno != EAGAIN)
                  {
                    perror ("read");
                    done = 1;
                  }
                break;
              }
            else if (count == 0)
              {
                /* End of file. The remote has closed the
                   connection. */
                done = 1;
                break;
              }

            size_t sum = 0;
            char* resp = (char*)response;

            while(sum < len)
            {
                int k = write(stream->socket, resp, len - sum);
                sum += k;
                resp += k;
            }

            requests.fetch_add(1, std::memory_order_relaxed);

          }

        if (done)
          {
            LOG_DEBUG("Closing TCP stream at " << stream->socket.id())

            /* Closing the descriptor will make epoll remove it
               from the set of descriptors which are monitored. */
            delete stream;
          }
    }
};

}
