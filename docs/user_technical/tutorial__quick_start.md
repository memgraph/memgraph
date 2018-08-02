## Quick Start {#tutorial-quick-start}

This article briefly outlines the basic steps necessary to install and run
Memgraph. It also gives a brief glimpse into the world of OpenCypher and
outlines some information on programmatic querying of Memgraph. The users
should also make sure to read and fully understand the implications of
[telemetry](#telemetry) at the very end of the article.

### Installation

With regards to their own preference, users can download the Memgraph binary
as:

  * [a Debian package for Debian 9 (Stretch)](#debian-installation)
  * [a RPM package for CentOS 7](#RPM-installation)
  * [a Docker image](#docker-installation)

After downloading the binary, users are advised to proceed to the corresponding
section below which outlines the installation details.

It is important to note that newer versions of Memgraph are currently not
backward compatible with older versions. This is mainly noticeable by
being unable to load storage snapshots between different versions.

#### Debian Package Installation {#debian-installation}

After downloading Memgraph as a Debian package, install it by running the
following:

```bash
dpkg -i /path/to/memgraph_<version>.deb
```

On successful installation, Memgraph should already be running. To
make sure that is true, user can start it explicitly with the command:


```bash
systemctl start memgraph
```

To verify that Memgraph is running, user can run the following command:

```bash
journalctl --unit memgraph
```

If successful, the user should receive an output similar to the following:

```bash
Nov 23 13:40:13 hostname memgraph[14654]: Starting 8 BoltS workers
Nov 23 13:40:13 hostname memgraph[14654]: BoltS server is fully armed and operational
Nov 23 13:40:13 hostname memgraph[14654]: BoltS listening on 0.0.0.0 at 7687
```

At this point, Memgraph is ready to process queries. To try out some elementary
queries, the user should proceed to [querying](#querying) section of this
article.

To shut down the Memgraph server, issue the following command:

```bash
systemctl stop memgraph
```

Memgraph configuration is available in `/etc/memgraph/memgraph.conf`. If the
configuration is altered, Memgraph needs to be restarted.

#### RPM Package Installation {#RPM-installation}

After downloading the RPM package of Memgraph, the user can install it by
issuing the following command:

```bash
rpm -U /path/to/memgraph-<version>.rpm
```

After the successful installation, Memgraph can be started as a service. To do
so, the user can type the following command:

```bash
systemctl start memgraph
```

To verify that Memgraph is running, the user should run the following command:

```bash
journalctl --unit memgraph
```

If successful, the user should receive an output similar to the following:

```bash
Nov 23 13:40:13 hostname memgraph[14654]: Starting 8 BoltS workers
Nov 23 13:40:13 hostname memgraph[14654]: BoltS server is fully armed and operational
Nov 23 13:40:13 hostname memgraph[14654]: BoltS listening on 0.0.0.0 at 7687
```

At this point, Memgraph is ready to process queries. To try out some elementary
queries, the user should proceed to [querying](#querying) section of this
article.

To shut down the Memgraph server, issue the following command:

```bash
systemctl stop memgraph
```

Memgraph configuration is available in `/etc/memgraph/memgraph.conf`. If the
configuration is altered, Memgraph needs to be restarted.

#### Docker Installation {#docker-installation}

Before proceeding with the installation, the user should install the Docker
engine on their system. Instructions on how to install Docker can be found on
the [official Docker website](https://docs.docker.com/engine/installation).
Memgraph's Docker image was built with Docker version `1.12` and should be
compatible with all newer versions.

After successful Docker installation, the user should install the Memgraph
Docker image and import it using the following command:

```bash
docker load -i /path/to/memgraph-<version>-docker.tar.gz
```

To actually start Memgraph, the user should issue the following command:

```bash
docker run -p 7687:7687 \
  -v mg_lib:/var/lib/memgraph -v mg_log:/var/log/memgraph -v mg_etc:/etc/memgraph \
  memgraph
```

If successful, the user should be greeted with the following message:

```bash
Starting 8 workers
Server is fully armed and operational
Listening on 0.0.0.0 at 7687
```

At this point, Memgraph is ready to process queries. To try out some elementary
queries, the user should proceed to [querying](#querying) section of this
article.

To stop Memgraph, press `Ctrl-c`.

#### Note about named volumes

Memgraph configuration is available in Docker's named volume `mg_etc`. On
Linux systems it should be in
`/var/lib/docker/volumes/mg_etc/_data/memgraph.conf`. After changing the
configuration, Memgraph needs to be restarted.

If it happens that the named volumes are reused between different Memgraph
versions, Docker will overwrite a folder within the container with existing
data from the host machine. If a new file is introduced, or two versions of
Memgraph are not compatible, some features might not work or Memgraph might
not be able to work correctly. We strongly advise the users to use another
named volume for a different Memgraph version or to remove the existing volume
from the host with the following command:

```bash
docker volume rm <volume_name>
```
#### Note for OS X/macOS Users {#OSX-note}

Although unlikely, some OS X/macOS users might experience minor difficulties
after following the Docker installation instructions. Instead of running on
`localhost`, a Docker container for Memgraph might be running on a custom IP
address. Fortunately, that IP address can be found using the following
algorithm:

1) Find out the container ID of the Memgraph container

By issuing the command `docker ps` the user should get an output similar to the
following:

```bash
CONTAINER ID        IMAGE               COMMAND                  CREATED        ...
9397623cd87e        memgraph            "/usr/lib/memgraph/m…"   2 seconds ago  ...
```

At this point, it is important to remember the container ID of the Memgraph
image.  In our case, that is `9397623cd87e`.

2) Use the container ID to retrieve an IP of the container

```bash
docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' 9397623cd87e
```

The command above should yield the sought IP. If that IP does not correspond to
`localhost`, it should be used instead of `localhost` when firing up the
`neo4j-client` in the [querying](#querying) section.

### Querying {#querying}

Memgraph supports the openCypher query language which has been developed by
[Neo4j](http://neo4j.com). It is a declarative language developed specifically
for interaction with graph databases which is currently going through a
vendor-independent standardization process.

The easiest way to execute openCypher queries against Memgraph is by using
Neo4j's command-line tool. The command-line `neo4j-client` can be installed as
described [on the official website](https://neo4j-client.net).

After installing `neo4j-client`, the user can connect to the running Memgraph
instance by issuing the following shell command:

```bash
neo4j-client -u "" -p ""  localhost 7687
```

After the client has started it should present a command prompt similar to:

```bash
neo4j-client 2.1.3
Enter `:help` for usage hints.
Connected to 'neo4j://@localhost:7687'
neo4j>
```

At this point it is possible to execute openCypher queries on Memgraph. Each
query needs to end with the `;` (*semicolon*) character. For example:

```opencypher
CREATE (u:User {name: "Alice"})-[:Likes]->(m:Software {name: "Memgraph"});
```

The above will create 2 nodes in the database, one labeled "User" with name
"Alice" and the other labeled "Software" with name "Memgraph". It will also
create a relationship that "Alice" *likes* "Memgraph".

To find created nodes and relationships, execute the following query:

```opencypher
MATCH (u:User)-[r]->(x) RETURN u, r, x;
```

#### Supported Languages

If users wish to query Memgraph programmatically, they can do so using the
[Bolt protocol](https://boltprotocol.org). Bolt was designed for efficient
communication with graph databases and Memgraph supports
[Version 1](https://boltprotocol.org/v1) of the protocol. Bolt protocol drivers
for some popular programming languages are listed below:

  * [Java](https://github.com/neo4j/neo4j-java-driver)
  * [Python](https://github.com/neo4j/neo4j-python-driver)
  * [JavaScript](https://github.com/neo4j/neo4j-javascript-driver)
  * [C#](https://github.com/neo4j/neo4j-dotnet-driver)
  * [Ruby](https://github.com/neo4jrb/neo4j)
  * [Haskell](https://github.com/zmactep/hasbolt)
  * [PHP](https://github.com/graphaware/neo4j-bolt-php)

We have included some basic usage examples for some of the supported languages
in the article about [programmatic querying](tutorial__programmatic_querying.md).

### Telemetry {#telemetry}

Telemetry is an automated process by which some useful data is collected at
a remote point. At Memgraph, we use telemetry for the sole purpose of improving
our product, thereby collecting some data about the machine that executes the
database (CPU, memory, OS and kernel information) as well as some data about the
database runtime (CPU usage, memory usage, vertices and edges count).

Here at Memgraph, we deeply care about the privacy of our users and do not
collect any sensitive information. If users wish to disable Memgraph's telemetry
features, they can easily do so by either altering the line in
`/etc/memgraph/memgraph.conf` that enables telemetry (`--telemetry-enabled=true`)
into `--telemetry-enabled=false`, or by including the `--telemetry-enabled=false`
as a command-line argument when running the executable.

### Where to Next

To learn more about the openCypher language, the user should visit our
[openCypher Query Language](open-cypher.md) article. For real-world examples
of how to use Memgraph, we strongly suggest reading through the following
articles:

  * [Analyzing TED Talks](tutorial__analyzing_TED_talks.md)
  * [Graphing the Premier League](tutorial__graphing_the_premier_league.md)
  * [Exploring the European Road Network](tutorial__exploring_the_european_road_network.md)

<!--- TODO(ipaljak) Possible broken link on docs update -->
Details on what can be stored in Memgraph can be found in the article about
[Data Storage](storage.md).

We *welcome and encourage* your feedback!
