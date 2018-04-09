## Quick Start

This chapter outlines installing and running Memgraph, as well as executing
basic queries against the database.

### Installation

The Memgraph binary is offered as:

  * Debian package for Debian 9 (Stretch);
  * RPM package for CentOS 7 and
  * Docker image.

After downloading the binary, proceed to the corresponding section below.

NOTE: Currently, newer versions of Memgraph are not backward compatible with
older versions. This is mainly noticeable by unsupported loading of storage
snapshots between different versions.

#### Docker Installation

Before proceeding with the installation, please install the Docker engine on
the system. Instructions on how to install Docker can be found on the
[official Docker website](https://docs.docker.com/engine/installation).
Memgraph Docker image was built with Docker version `1.12` and should be
compatible with all later versions.

After installing and running Docker, download the Memgraph Docker image and
import it with the following command.

```
docker load -i /path/to/memgraph-<version>-docker.tar.gz
```

Memgraph is then started with another docker command.

```
docker run -p 7687:7687 \
  -v mg_lib:/var/lib/memgraph -v mg_log:/var/log/memgraph -v mg_etc:/etc/memgraph \
  memgraph
```

On success, expect to see output similar to the following.

```
Starting 8 workers
Server is fully armed and operational
Listening on 0.0.0.0 at 7687
```

Memgraph is now ready to process queries, you may now proceed to
[querying](#querying). To stop Memgraph, press `Ctrl-c`.

Memgraph configuration is available in Docker's named volume `mg_etc`. On
Linux systems it should be in
`/var/lib/docker/volumes/mg_etc/_data/memgraph.conf`. After changing the
configuration, Memgraph needs to be restarted.

#### Debian Package Installation

After downloading Memgraph as a Debian package, install it by running the
following.

```
dpkg -i /path/to/memgraph_<version>.deb
```

If the installation was successful, Memgraph should already be running. To
make sure that is true, start it explicitly with the command:

```
systemctl start memgraph
```

To verify that Memgraph is running, run the following command.

```
journalctl --unit memgraph
```

It is expected to see something like the following output.

```
Nov 23 13:40:13 hostname memgraph[14654]: Starting 8 workers
Nov 23 13:40:13 hostname memgraph[14654]: Server is fully armed and operational
Nov 23 13:40:13 hostname memgraph[14654]: Listening on 0.0.0.0 at 7687
```

Memgraph is now ready to process queries, you may now proceed to
[querying](#querying). To shutdown Memgraph server, issue the following
command.

```
systemctl stop memgraph
```

Memgraph configuration is available in `/etc/memgraph/memgraph.conf`. After
changing the configuration, Memgraph needs to be restarted.

#### RPM Package Installation

If you downloaded the RPM package of Memgraph, you can install it by running
the following command.

```
rpm -U /path/to/memgraph-<version>.rpm
```

After the successful installation, Memgraph can be started as a service. To do
so, type the following command.

```
systemctl start memgraph
```

To verify that Memgraph is running, run the following command.

```
journalctl --unit memgraph
```

It is expected to see something like the following output.

```
Nov 23 13:40:13 hostname memgraph[14654]: Starting 8 workers
Nov 23 13:40:13 hostname memgraph[14654]: Server is fully armed and operational
Nov 23 13:40:13 hostname memgraph[14654]: Listening on 0.0.0.0 at 7687
```

Memgraph is now ready to process queries, you may now proceed to
[querying](#querying). To shutdown Memgraph server, issue the following
command.

```
systemctl stop memgraph
```

Memgraph configuration is available in `/etc/memgraph/memgraph.conf`. After
changing the configuration, Memgraph needs to be restarted.

### Querying

Memgraph supports the openCypher query language which has been developed by
[Neo4j](http://neo4j.com). The language is currently going through a
vendor-independent standardization process. It's a declarative language
developed specifically for interaction with graph databases.

The easiest way to execute openCypher queries against Memgraph, is using
Neo4j's command-line tool. The command-line `neo4j-client` can be installed as
described [on the official website](https://neo4j-client.net).

After installing `neo4j-client`, connect to the running Memgraph instance by
issuing the following shell command.

```
neo4j-client --insecure -u "" -p ""  localhost 7687
```

After the client has started it should present a command prompt similar to:

```
neo4j-client 2.1.3
Enter `:help` for usage hints.
Connected to 'neo4j://@localhost:7687' (insecure)
neo4j>
```

At this point it is possible to execute openCypher queries on Memgraph. Each
query needs to end with the `;` (*semicolon*) character. For example:

```
CREATE (u:User {name: "Alice"})-[:Likes]->(m:Software {name: "Memgraph"});
```

The above will create 2 nodes in the database, one labeled "User" with name
"Alice" and the other labeled "Software" with name "Memgraph". It will also
create a relationship that "Alice" *likes* "Memgraph".

To find created nodes and relationships, execute the following query:

```
MATCH (u:User)-[r]->(x) RETURN u, r, x;
```

### Where to Next

To learn more about the openCypher language, visit **openCypher Query
Language** chapter in this document. For real-world examples of how to use
Memgraph visit **Examples** chapter. If you wish to use a programming language
to execute queries on Memgraph, go to the **Drivers** chapter. Details on what
can be stored in Memgraph are in **Data Storage** chapter.

We *welcome and encourage* your feedback!

