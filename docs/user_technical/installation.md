## Installation

Memgraph is a 64-bit Linux compatible database management system. For the
purpose of Alpha testing Memgraph has been packed into a
[Docker](https://www.docker.com) image based on Ubuntu 16.04. Before
proceeding with the installation, please install the Docker engine on your
system.  Instructions how to install Docker can be found on the
[official Docker website](https://docs.docker.com/engine/installation).
Memgraph Docker image was built with Docker version `1.12` and should be
compatible with all latter versions.

### Docker Import

After a successful download the Memgraph Docker image
can be imported into Docker:

```
docker load -i /path/to/memgraph_alpha_v0.6.0.tar.gz
```

### Image Configuration & Running Memgraph

Memgraph can be started by executing:

```
docker run -it -p 7687:7687 memgraph_alpha_v0.6.0
```

The `-it` option enables displaying Memgraph's logs inside the current shell.
The `-p` option is used to specify the port on which Memgraph will listen for
requests. Memgraph uses the Bolt protocol for network communication, which
uses port `7687` by default.

It is recommended to perform some additional Docker configuration. Memgraph is
currently an in-memory database management system, but it periodically stores
all data to the hard drive. These storages are referred to as *snapshots* and
are used for recovering data in case of a restart.  When starting Memgraph, a
folder for snapshots needs to be created and mounted on the host file system.
It is also recommended to run the Docker container in the background.  On a
Linux system all of that can be achieved with the following shell commands:

```
# Create the snapshots folder on the host.
mkdir -p memgraph
# Docker expects full path to the created folder.
FULL_OUTPUT_PATH=$PWD/memgraph
# Run Memgraph.
docker run -d -p 7687:7687 -v ${FULL_OUTPUT_PATH}:/var/lib/memgraph --name <memgraph_docker_container_name> memgraph_alpha_v0.6.0
```

In the commands above `-d` means that the container will be detached (run in
the background).  `-v` mounts a host folder to a path inside the Docker
container. The output folder contains Memgraph's periodical snapshots and log
file.  The log file should be uploaded to Memgraph's issue tracking system in
case of an error.  With `--name` a custom name for the container can be set
(useful for easier container management).  `<memgraph_docker_container_name>`
could be any convenient name e.g.  `memgraph_alpha`.

### Memgraph Configuration Parameters

Memgraph can be configured with a number of command-line parameters.  The
parameters should be appended to the end of the `docker run` command in the
`--param-name=param-value` format.  Following is a list of available
parameters:

 Name  | Type | Default | Description
-------|------|:-------:|-------------
 --port | integer | 7687 | Communication port on which to listen.
 --num-workers | integer | CPU count[^1] |  Number of Memgraph worker threads.
 --snapshot-cycle-sec | integer | 300 | Interval (seconds) between database snapshots.<br/>Value of -1 turns taking snapshots off.
 --max-retained-snapshots | integer | 3 | Number of retained snapshots.<br/>Value -1 means without limit.
 --snapshot-on-db-exit | bool | false | Make a snapshot when closing Memgraph.
 --recover-on-startup | bool | false | Recover the database on startup using the last<br/>stored snapshot.

[^1]: Maximum number of concurrent executions on the current CPU.

To find more about how to execute queries on Memgraph please proceed to
[Quick Start](quick-start.md).

### Cleanup

Status and Memgraph's logging messages can be checked with:

```
docker ps -a
docker logs -f <memgraph_docker_container_name>
```


Memgraph and its Docker container can be stopped with:

```
docker stop <memgraph_docker_container_name>
```

After the container has stopped, it can be removed by
executing:

```
docker rm <memgraph_docker_container_name>
```

