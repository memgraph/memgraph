#include <iostream>
#include <array>

#include "debug/log.hpp"

#include "socket.hpp"
#include "worker.hpp"

#define MAXEVENTS 64

static int
make_socket_non_blocking (int sfd)
{
  int flags, s;

  flags = fcntl (sfd, F_GETFL, 0);
  if (flags == -1)
    {
      perror ("fcntl");
      return -1;
    }

  flags |= O_NONBLOCK;
  s = fcntl (sfd, F_SETFL, flags);
  if (s == -1)
    {
      perror ("fcntl");
      return -1;
    }

  return 0;
}

int main(void)
{
    std::array<io::Worker, 8> workers;
    int idx = 0;

    auto socket = io::Socket::create("7474");
    socket.set_non_blocking();
    socket.listen(1024);

  int efd, s;
  struct epoll_event event;
  struct epoll_event *events;

  efd = epoll_create1 (0);
  if (efd == -1)
    {
      perror ("epoll_create");
      abort ();
    }

  event.data.fd = socket;
  event.events = EPOLLIN | EPOLLET;
  s = epoll_ctl (efd, EPOLL_CTL_ADD, socket, &event);
  if (s == -1)
    {
      perror ("epoll_ctl");
      abort ();
    }

  /* Buffer where events are returned */
  events = static_cast<struct epoll_event*>(calloc (MAXEVENTS, sizeof event));

  /* The event loop */
  while (1)
    {
      int n, i;

      n = epoll_wait (efd, events, MAXEVENTS, -1);
      for (i = 0; i < n; i++)
	{
	  if ((events[i].events & EPOLLERR) ||
              (events[i].events & EPOLLHUP) ||
              (!(events[i].events & EPOLLIN)))
	    {
              /* An error has occured on this fd, or the socket is not
                 ready for reading (why were we notified then?) */
	      fprintf (stderr, "epoll error\n");
	      close (events[i].data.fd);
	      continue;
	    }

	  else if (socket == events[i].data.fd)
	    {
              /* We have a notification on the listening socket, which
                 means one or more incoming connections. */
              while (true)
              {
                  idx = (idx + 1) % workers.size();

                  auto& worker = workers[i];

                  if(!worker.accept(socket))
                      break;
              }
                    
                  /* struct sockaddr in_addr; */
                  /* socklen_t in_len; */
                  /* int infd; */
                  /* char hbuf[NI_MAXHOST], sbuf[NI_MAXSERV]; */

                  /* in_len = sizeof in_addr; */
                  /* infd = accept (socket, &in_addr, &in_len); */
                  /* if (infd == -1) */
                  /*   { */
                  /*     if ((errno == EAGAIN) || */
                  /*         (errno == EWOULDBLOCK)) */
                  /*       { */
                  /*         /1* We have processed all incoming */
                  /*            connections. *1/ */
                  /*         break; */
                  /*       } */
                  /*     else */
                  /*       { */
                  /*         perror ("accept"); */
                  /*         break; */
                  /*       } */
                  /*   } */

                  /* s = getnameinfo (&in_addr, in_len, */
                  /*                  hbuf, sizeof hbuf, */
                  /*                  sbuf, sizeof sbuf, */
                  /*                  NI_NUMERICHOST | NI_NUMERICSERV); */
                  /* if (s == 0) */
                  /*   { */
                  /*     printf("Accepted connection on descriptor %d " */
                  /*            "(host=%s, port=%s)\n", infd, hbuf, sbuf); */
                  /*   } */

                  /* /1* Make the incoming socket non-blocking and add it to the */
                  /*    list of fds to monitor. *1/ */
                  /* s = make_socket_non_blocking (infd); */
                  /* if (s == -1) */
                  /*   abort (); */

                  /* event.data.fd = infd; */
                  /* event.events = EPOLLIN | EPOLLET; */
                  /* s = epoll_ctl (efd, EPOLL_CTL_ADD, infd, &event); */
                  /* if (s == -1) */
                  /*   { */
                  /*     perror ("epoll_ctl"); */
                  /*     abort (); */
                  /*   } */
            }
          /* else */
          /*   { */
          /*     /1* We have data on the fd waiting to be read. Read and */
          /*        display it. We must read whatever data is available */
          /*        completely, as we are running in edge-triggered mode */
          /*        and won't get a notification again for the same */
          /*        data. *1/ */
          /*     int done = 0; */

          /*     while (1) */
          /*       { */
          /*         ssize_t count; */
          /*         char buf[512]; */

          /*         count = read (events[i].data.fd, buf, sizeof buf); */
          /*         if (count == -1) */
          /*           { */
          /*             /1* If errno == EAGAIN, that means we have read all */
          /*                data. So go back to the main loop. *1/ */
          /*             if (errno != EAGAIN) */
          /*               { */
          /*                 perror ("read"); */
          /*                 done = 1; */
          /*               } */
          /*             break; */
          /*           } */
          /*         else if (count == 0) */
          /*           { */
          /*             /1* End of file. The remote has closed the */
          /*                connection. *1/ */
          /*             done = 1; */
          /*             break; */
          /*           } */

          /*         /1* Write the buffer to standard output *1/ */
          /*         s = write (1, buf, count); */
          /*         if (s == -1) */
          /*           { */
          /*             perror ("write"); */
          /*             abort (); */
          /*           } */
          /*       } */

          /*     if (done) */
          /*       { */
          /*         printf ("Closed connection on descriptor %d\n", */
          /*                 events[i].data.fd); */

          /*         /1* Closing the descriptor will make epoll remove it */
          /*            from the set of descriptors which are monitored. *1/ */
          /*         close (events[i].data.fd); */
          /*       } */
          /*   } */
        }
    }

  free (events);


    return 0;
}
