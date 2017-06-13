## Installation

Memgraph is a 64-bit Linux compatible database management system.  For the
purpose of Alpha testing Memgraph has been packed into Ubuntu 16.04 based
[Docker](https://www.docker.com) image.  Before proceeding with the
installation, please install the Docker engine on your system.  Instructions
how to install Docker can be found
[here](https://docs.docker.com/engine/installation).  Memgraph Docker image was
built with Docker version `1.12`, so all Docker versions since version `1.12`
should work.

### Import

After a successful download, Memgraph can be imported as follows
```
docker load -i /path/to/<memgraph_docker_image_name>.tar.gz
```

### Run

The most convenient way to start Memgraph is
```
docker run -it -p 7687:7687 <memgraph_docker_image_name>
```
The `-it` option enables displaying Memgraph's logs inside the current shell.
The `-p` option is used to specify the port which Memgraph will use to listen
for requests.  Since the default Bolt protocol is `7687`, the straightforward
option is to use that port.

The recommended way to run Memgraph requires some additional configuration. A
folder for database snapshots needs to be created and mounted on the host file
system. It is also recommended to run the container in the background. On Linux
system all of that can be executed as follows
```
# create snapshots folder on the host
mkdir -p memgraph_snapshots
# Docker expects full path to the created folder
FULL_SNAPSHOTS_PATH=$PWD/memgraph_snapshots
# run Memgraph
docker run -d -p 7687:7687 -v ${FULL_SNAPSHOTS_PATH}:/memgraph/snapshots --name <memgraph_docker_container_name> <memgraph_docker_image_name>
```
`-d` means that the container will be detached (run in the background mode).
`-v` mounts snapshots folder inside the container on the host file system
(useful for the recovery process).  With `--name` a user can set a custom name
for the container (useful when the container has to be stopped).
`<memgraph_docker_container_name>` could be any convenient name e.g.
`memgraph_alpha`.

### Configuration Parameters

Memgraph can be run with various parameters. The parameters should be
appended at the end of `docker run` command in the following format
`--param-name=param-value`.
Below is a list of all available parameters

 Name  | Type | Default | Description
-------|------|:-------:|-------------
 port | integer | 7687 | Communication port on which to listen.
 num_workers | integer | 8 |  Number of workers (concurrent threads).
 snapshot_cycle_sec | integer | 300 | Interval, `in seconds`, between two database snapshots. Value of -1 turns the snapshots off. 
 max_retained_snapshots | integer | 3 | Number of retained snapshots, -1 means without limit.
 snapshot_on_db_destruction | bool | false | Make a snapshot when closing Memgraph.
 recover_on_startup | bool | false | Recover the database on startup.

To find more about how to execute queries against
the database please proceed to [Quick Start](quick-start.md).

### Cleanup

Status & Memgraph's logging messages can be checked with:
```
docker ps -a
docker logs -f <memgraph_docker_container_name>
```

To stop Memgraph, execute
```
docker stop <memgraph_docker_container_name>
```

After the container has been stopped, it can be removed by
executing
```
docker rm <memgraph_docker_container_name>
```
