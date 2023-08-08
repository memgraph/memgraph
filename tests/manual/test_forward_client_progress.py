#!/usr/bin/env python3

"""
This script can be pointed at a memgraph server that
you wish to assert "forward progress" for concurrent
clients on, for instance, for testing replication or
durability in the face of machine failures.

Of particular note are the arguments:
    --host              the first connection is made to this host:port
    --port
    --reconnect-host    after any disconnects, reconnection is attempted to reconnect-host:reconnect-port
    --reconnect-port

For any testing, it is likely that you will want to set the server argument:
    --storage-wal-file-flush-every-n-tx=1
which will ensure that the client will receive no success responses unless the
data is fsynced on disk.

For example, this will let you keep running a server container from the terminal in
a loop that is fairly kill friendly, also mounting an external directory into
/var/lib/memgraph to retain storage files across restarts:

terminal 1:
    run this script
terminal 2: (server running in a restart loop if it gets killed)
    while true; do docker run -v some/local/storage/directory:/var/lib/memgraph -it -p 7687:7687 -p 3000:3000 memgraph/memgraph --storage-wal-file-flush-every-n-tx=1; done
terminal 3: (killer that will restart it every few seconds)
    while true; do sleep `shuf -i 1-5 -n1`; pkill 'docker' -u `id -u`; done

If you are testing with replication, it makes the most sense
to configure SYNC replication, and to set the fall-back
reconnect-host and reconnect-port to the replica that will
take over after you kill main.

If you set the --ops argument to 0, it will run forever,
which may be useful for long-running correctness tests.

It works by having each concurrent thread (--concurrency argument)
create a unique vertex that has a "counter" attribute. This counter
attribute is expected to always make forward progress. The client
remembers the last value that it wrote, and it asserts that if it
ever receives a successful response for a write that it does, that
this counter has advanced by one.
"""

from os import kill
from multiprocessing import Process, Event
from uuid import uuid4
from time import sleep

import argparse
import mgclient

shutdown = Event()


def forward_progress(args):
    conn = mgclient.connect(host=args.host, port=args.port)
    conn.autocommit = True
    cursor = conn.cursor()

    id = str(uuid4())

    cursor.execute("CREATE (v:Vsn { id: $id, counter: 0 }) RETURN v;", {"id": id})
    cursor.fetchall()

    counter = 0

    for i in range(args.ops):
        if shutdown.is_set():
            return
        try:
            cursor.execute(
                """
                MATCH (v: Vsn { id: $id, counter: $old })
                SET v.counter = $new
                RETURN v
            """,
                {"id": id, "old": counter, "new": counter + 1},
            )
            ret = cursor.fetchall()
            if len(ret) != 1:
                print("expected there to be exactly one Vsn associated with this counter, but we got:", ret)
                shutdown.set()
            counter += 1
            continue
        except mgclient.DatabaseError as e:
            if not "failed to receive" in str(e):
                print("encountered unexpected exception:", str(e))
                shutdown.set()
        except mgclient.InterfaceError as e:
            assert "bad session" in str(e)

        print("waiting for server to come back")

        sleep(1)

        try:
            conn = mgclient.connect(host=args.reconnect_host, port=args.reconnect_port)
            conn.autocommit = True
            cursor = conn.cursor()
        except:
            pass


children = []
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-s", "--host", default="127.0.0.1")
    parser.add_argument("-p", "--port", type=int, default=7687)
    parser.add_argument("--reconnect-host", default="127.0.0.1")
    parser.add_argument("--reconnect-port", type=int, default=7687)
    parser.add_argument("-c", "--concurrency", type=int, default=20)
    parser.add_argument("-o", "--ops", type=int, default=1000)
    args = parser.parse_args()

    if args.ops == 0:
        args.ops = 2**64

    for i in range(args.concurrency):
        child = Process(target=forward_progress, args=(args,))
        child.start()
        children.append(child)

    for child in children:
        child.join()
