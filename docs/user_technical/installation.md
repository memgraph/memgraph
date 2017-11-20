## Installation

Memgraph is a 64-bit Linux compatible database management system. Currently,
the Memgraph binary is offered only as a [Docker](https://www.docker.com)
image based on Debian Stretch. Before proceeding with the installation, please
install the Docker engine on the system. Instructions how to install Docker
can be found on the
[official Docker website](https://docs.docker.com/engine/installation).
Memgraph Docker image was built with Docker version `1.12` and should be
compatible with all later versions.

### Docker Import

After a successful download the Memgraph Docker image
can be imported into Docker:

```
docker load -i /path/to/memgraph-<version>-docker.tar.gz
```

### Image Configuration & Running Memgraph

Memgraph can be started by executing:

```
docker run -it -p 7687:7687 memgraph:<version>
```

The `-it` option enables displaying Memgraph's logs inside the current shell.
The `-p` option is used to specify the port on which Memgraph will listen for
requests. Memgraph uses the Bolt protocol for network communication, which
uses port `7687` by default. The `<version>` part are 3 numbers specifying the
version, e.g. `1.2.3`. To always run the latest version, `:<version>` part can
be omitted.

```
docker run -it -p 7687:7687 memgraph
```

It is recommended to perform some additional Docker configuration. Memgraph is
currently an in-memory database management system, but it periodically stores
all data to the hard drive. These storages are referred to as *snapshots* and
are used for recovering data in case of a restart.

When starting Memgraph, a folder for snapshots needs to be created and mounted
on the host file system.  This can be easily done using Docker's named
volumes. For example:

```
# Run Memgraph.
docker run -p 7687:7687 -v mg_data:/var/lib/memgraph memgraph
```

The `-v` option will make a named volume directory called `mg_data` and
Memgraph will store snapshots there. Named volumes are usually found in
`/var/lib/docker/volumes`. This is the recommended way to create persistent
storage.

The same can be achieved for logs and configuration. All supported volumes
which can be mounted are:

  * `/var/lib/memgraph`, for storing snapshots;
  * `/var/log/memgraph`, for storing *full* logs and
  * `/etc/memgraph`, for Memgraph configuration (in `memgraph.conf` file).

Another way to expose the configuration and data is to use a full path to some
directory on the system. In such a case, the directory first needs to be
created and allow docker image to write inside. For example, to create a
snapshots volume in the current directory:

```
# Create the snapshots folder on the host.
mkdir -m 777 mg_data
# Docker expects full path to the created folder.
FULL_OUTPUT_PATH=$PWD/mg_data
# Run Memgraph.
docker run -p 7687:7687 -v ${FULL_OUTPUT_PATH}:/var/lib/memgraph memgraph
```

In this example, `-v` mounts a host folder `$PWD/mg_data` to a path inside the
Docker container. Note that full paths will not be initially populated with
files from the container. This means that if you expose the configuration
`/etc/memgraph` to a full path, the default configuration `memgraph.conf` will
be missing. To avoid confusion, using named volumes is preferred.

Other than setting the configuration, it is also recommended to run the Docker
container in the background. This is achieved with `-d` option. In such a
case, the name should be set for the running container, so that it can be
easily found and shut down when needed. For example:

```
# Run Memgraph.
docker run -p 7687:7687 -d --name <memgraph_docker_container_name> memgraph
```

### Memgraph Configuration Parameters

Memgraph can be configured with a number of command-line parameters.  The
parameters should be appended to the end of the `docker run` command in the
`--param-name=param-value` format.  Following is a list of available
parameters:

 Name  | Type | Default | Description
-------|------|:-------:|-------------
 --interface | string | "0.0.0.0" | IP address on which to listen.
 --port | integer | 7687 | Communication port on which to listen.
 --num-workers | integer | CPU count[^1] |  Number of Memgraph worker threads.
 --gc-cycle-sec | integer | 30 | Interval, in seconds, when the garbage collection (GC) should run. <br/>If set to -1 the GC will never run (use with caution, memory will never get released).
 --memory-warning-threshold | integer | 1024 | Memory warning threshold, in MB. If Memgraph detects there is less available RAM available it will log a warning. Set to 0 to disable.
 --query-execution-time-sec | integer | 30 | Maximum allowed query execution time, in seconds. <br/>Queries exceeding this limit will be aborted. Value of -1 means no limit.
 --query-plan-cache | bool | true | Cache generated query plans.
 --query-plan-cache-ttl | int | 60 | Time to live for cached query plans, in seconds.
 --durability-enabled | bool | true | If database state persistence is enabled (snapshot and write-ahead log).
 --durability-directory | string | "/var/lib/memgraph/durability" | Path to the directory where durability files will be stored.
 --db-recover-on-startup | bool | true | Recover the database on startup (from snapshots and write-ahead logs).
 --snapshot-cycle-sec | integer | 300 | Interval  between database snapshots, in seconds.
 --snapshot-max-retained | integer | 3 | Number of retained snapshots.<br/>Value -1 means without limit.
 --snapshot-on-exit | bool | true | Make a snapshot when closing Memgraph.
 --log-file | string | "/var/log/memgraph/memgraph.log" | Path to where the log should be stored.
 --also-log-to-stderr | bool | false | If `true`, log messages will go to stderr in addition to logfiles.
 --flag-file | string | "" | Path to a file containing additional configuration settings.

All of the parameters can also be found in `/etc/memgraph/memgraph.conf`.

[^1]: Maximum number of concurrent executions on the current CPU.

To find more about how to execute queries on Memgraph please proceed to
**Quick Start**.

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

