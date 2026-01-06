<h1 align="center">
  <br>
  <a href="https://github.com/memgraph/mage"> <img src="https://github.com/memgraph/mage/blob/main/img/wizard.png?raw=true" alt="MAGE" width=20%></a>
  <br>
  MAGE
  <br>
</h1>

<p align="center">
    <a href="https://github.com/memgraph/memgraph/blob/master/licenses/BSL.txt">
      <img src="https://img.shields.io/badge/license-BSL-yellowgreen" alt="license" title="license"/>
    </a>
    <a href="https://github.com/memgraph/memgraph/blob/master/licenses/MEL.txt" alt="Documentation">
      <img src="https://img.shields.io/badge/license-MEL-yellow" alt="license" title="license"/>
    </a>
    <a href="https://docs.memgraph.com/mage/" alt="Documentation">
        <img src="https://img.shields.io/badge/documentation-MAGE-orange" />
    </a>
    <a href="https://hub.docker.com/r/memgraph/memgraph-mage" alt="Documentation">
        <img src="https://img.shields.io/badge/image-Docker-2496ED?logo=docker" />
    </a>
</p>

## Memgraph Advanced Graph Extensions :crystal_ball:

This open-source repository contains all available user-defined graph analytics
modules and procedures that extend the Cypher query language, written by the
team behind Memgraph and its users. You can find and contribute implementations
of various algorithms in multiple programming languages, all runnable inside
Memgraph. This project aims to give everyone the tools they need to tackle the
most challenging graph problems.

### Introduction to query modules with MAGE

Memgraph introduces the concept of [query
modules](https://docs.memgraph.com/memgraph/reference-guide/query-modules/),
user-defined procedures that extend the Cypher query language. These procedures
are grouped into modules that can be loaded into Memgraph. How to run them can
be seen on their official
[documentation](https://docs.memgraph.com/mage/usage/loading-modules). When
started, Memgraph will automatically attempt to load the query modules from all
`*.so` and `*.py` files it finds in the default directory defined with flag
[--query-modules-directory](https://docs.memgraph.com/memgraph/reference-guide/configuration/).

### Daily builds

Stay on the cutting edge with the latest features and improvements by using
[Memgraph Daily Builds](https://memgraph.github.io/daily-builds/#mage). Daily
builds are updated frequently and allow you to test new capabilities before they
reach stable releases.

<p align="left">
<a href="https://memgraph.github.io/daily-builds/#mage">
  <img src="https://img.shields.io/badge/Daily%20Builds-latest-blue?style=for-the-badge" alt="Daily Builds" />
</a>
</p>

### Further reading

If you want more info about MAGE, check out the official [MAGE
Documentation](https://docs.memgraph.com/mage/).

### Algorithm proposition

Furthermore, if you have an **algorithm proposition**, please fill in the survey
on [**mage.memgraph.com**](https://mage.memgraph.com/).

### Community

Make sure to check out the Memgraph community and join us on a survey of
streaming graph algorithms! Drop us a message on the channels below:

<p align="center">
<a href="https://twitter.com/intent/follow?screen_name=memgraphmage">
    <img src="https://img.shields.io/badge/@memgraphmage-1DA1F2?style=for-the-badge&logo=twitter&logoColor=white" alt="Follow @memgraphmage"/>
  </a>
<a href="https://discourse.memgraph.com/">
    <img src="https://img.shields.io/badge/Discourse_forum-000000?style=for-the-badge&logo=discourse&logoColor=white" alt="Discourse forum"/>
</a>
<a href="https://memgr.ph/join-discord">
    <img src="https://img.shields.io/badge/Discord-7289DA?style=for-the-badge&logo=discord&logoColor=white" alt="Discord"/>
</a>
<a href="https://github.com/memgraph">
    <img src="https://img.shields.io/badge/Memgraph_GitHub-181717?style=for-the-badge&logo=github&logoColor=white" alt="Memgraph GitHub"/>
</a>
<a href="https://www.youtube.com/channel/UCZ3HOJvHGxtQ_JHxOselBYg">
    <img src="https://img.shields.io/badge/YouTube-FF0000?style=for-the-badge&logo=youtube&logoColor=white" alt="Memgraph YouTube"/>
</a>
</p>

## Overview

- [Memgraph Advanced Graph Extensions :crystal\_ball:](#memgraph-advanced-graph-extensions-crystal_ball)
  - [Introduction to query modules with MAGE](#introduction-to-query-modules-with-mage)
  - [Further reading](#further-reading)
  - [Algorithm proposition](#algorithm-proposition)
  - [Community](#community)
- [Overview](#overview)
- [Memgraph compatibility](#memgraph-compatibility)
- [How to install MAGE?](#how-to-install-mage)
  - [1. Use MAGE with Docker](#1-use-mage-with-docker)
    - [a) Get MAGE from Docker Hub](#a-get-mage-from-docker-hub)
    - [b) Install MAGE with a Docker build of the repository](#2-install-mage-with-docker-build-of-the-repository)
  - [2. Installing MAGE on Linux distro from source](#2-installing-mage-on-linux-distro-from-source)
    - [Prerequisites](#prerequisites)
- [Running MAGE](#running-mage)
- [MAGE Spells](#mage-spells)
- [Advanced configuration](#advanced-configuration)
- [Testing MAGE](#testing-mage)
- [Contributing](#contributing)
- [Code of Conduct](#code-of-conduct)
- [Feedback](#feedback)

## Memgraph compatibility

Since version 3.0.0 of Memgraph, Mage is built with a matching version number.

<details>
<summary>Before v3.0.0</summary>

With changes in Memgraph API, MAGE started to track version numbers. The table
below lists the compatibility of MAGE with Memgraph versions.
| MAGE version | Memgraph version |
| ------------ | ---------------- |
| >= 1.11.9    | >= 2.11.0        |
| >= 1.5.1     | >= 2.5.1         |
| >= 1.5       | >= 2.5.0         |
| >= 1.4       | >= 2.4.0         |
| >= 1.0 | >= 2.0.0 |
| ^0 | >= 1.4.0 <= 1.6.1 |

</details>

## How to install MAGE?

There are two options to install MAGE.

1) For the [Docker
installation](#1-installing-mage-with-docker), you only need Docker installed.

2) [To build from
source](#2-installing-MAGE-on-linux-distro-from-source), you will
need to install a few things first. Jump to section #2 to check for installation details.

After the installation, you will be ready to query Memgraph and use MAGE
modules. Make sure to have one of [the querying
platforms](https://memgraph.com/docs/memgraph/connect-to-memgraph/) installed as
well.

### 1. Use MAGE with Docker

#### a) Get MAGE from Docker Hub

**1.** This command downloads the image from Docker Hub and runs Memgraph
preloaded with **MAGE** modules:

```
docker run -p 7687:7687 -p 7444:7444 memgraph/memgraph-mage
```

#### 2 Install MAGE with Docker build of the repository

**0. a** Make sure that you have cloned the MAGE GitHub repository and positioned
yourself inside the repo in your terminal:

```bash
git clone --recurse-submodules https://github.com/memgraph/memgraph.git && cd memgraph/mage
```

**0. b** Download Memgraph from our official download site inside your cloned MAGE repository. Set `${MEMGRAPH_VERSION}` to the latest release of Memgraph, and
`${ARCHITECTURE}` to your system architecture (`amd64` or `arm64`):

```bash

curl -L "https://download.memgraph.com/memgraph/v${MEMGRAPH_VERSION}/ubuntu-24.04/memgraph_${MEMGRAPH_VERSION}-1_${ARCHITECTURE}.deb" > memgraph-${ARCHITECTURE}.deb

```

or this one if you are on `arm64`:

```bash
curl -L "https://download.memgraph.com/memgraph/v${MEMGRAPH_VERSION}/ubuntu-24.04-aarch64/memgraph_${MEMGRAPH_VERSION}-1_arm64.deb" > memgraph-arm64.deb
```

**1.** To build the **MAGE** image run the following command where you set `${architecture}` to your system architecture (`amd64` or `arm64`):

```
DOCKER_BUILDKIT=1 docker buildx build \
--tag memgraph-mage:prod \
--target prod \
--platform linux/${architecture} \
--file Dockerfile.release \
--load .

```

This will build any new algorithm added to MAGE, and load it inside Memgraph.

**2.** Start the container with the following command and enjoy Memgraph with
**MAGE**:

```
docker run --rm -p 7687:7687 -p 7444:7444 --name mage memgraph-mage
```

**NOTE**: if you made any changes while the **MAGE** Docker container was
running, you will need to stop it and rebuild the whole image, or you can copy
the `mage` directory inside the Docker container and do the rebuild from there.
To learn more about development with MAGE and Docker, visit the
[documentation](https://memgraph.com/docs/mage/installation).

### 2. Installing MAGE on Linux distro from source

> Note: This step is more suitable for local development.

#### Prerequisites

- Linux based Memgraph package you can download
  [here](https://memgraph.com/download). We offer Ubuntu, Debian and CentOS
  based Memgraph packages. To install, proceed to the following
  [site](https://memgraph.com/docs/memgraph/installation).
- To build and install MAGE query modules you will need:
  - libcurl4
  - libpython3.12
  - libssl-dev
  - libboost-all-dev
  - openssl
  - build-essential
  - make
  - cmake
  - curl
  - g++
  - python3
  - python3-pip
  - python3-setuptools
  - python3-dev
  - clang
  - unixodbc
  - uuid-dev


Since Memgraph needs to load MAGE's modules, there is the `setup` script to help you. With it, you can build the modules so that Memgraph
can load them on start up.

Before you start, don't forget to clone MAGE with `--recurse-submodules` flag:

```bash
git clone https://github.com/memgraph/memgraph.git && cd memgraph/mage

```
Run the following command to install Rust and Python dependencies:
```bash
curl https://sh.rustup.rs -sSf | sh -s -- -y \
&& export PATH="/root/.cargo/bin:${PATH}" \
&& python3 -m  pip install -r python/requirements.txt \
&& python3 -m  pip install -r python/tests/requirements.txt \
&& python3 -m  pip install torch-sparse torch-cluster torch-spline-conv torch-geometric torch-scatter -f https://data.pyg.org/whl/torch-1.12.0+cu102.html \
```



Now you can run the following command to compile and copy the query modules to the `/usr/lib/memgraph/query_modules` path:

```bash
python3 setup build -p /usr/lib/memgraph/query_modules
```

It will generate a `mage/dist` directory and copy the modules to
the `/usr/lib/memgraph/query_modules` directory.


> Note that query modules are loaded into Memgraph on startup so if your
> instance was already running you will need to execute the following query
> inside one of the [querying
> platforms](https://docs.memgraph.com/memgraph/connect-to-memgraph) to load
> them: `CALL mg.load_all();`

## Running MAGE

This is an example of running the PageRank algorithm on a simple graph. You can
find more details on the [documentation
page](https://memgraph.com/docs/mage/query-modules/cpp/pagerank#example).

```
// Create the graph from the image below

CALL pagerank.get()
YIELD node, rank;
```

|             Graph input             |              MAGE output              |
| :---------------------------------: | :-----------------------------------: |
| ![graph_input](img/graph_input.png) | ![graph_output](img/graph_output.png) |

## MAGE Spells

| Algorithms                                                                                                       | Lang   | Description                                                                                                                                                                                                                       |
|------------------------------------------------------------------------------------------------------------------|--------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| [betweenness_centrality](https://memgraph.com/docs/mage/query-modules/cpp/betweenness-centrality)                | C++    | The betweenness centrality of a node is defined as the sum of the of all-pairs shortest paths that pass through the node divided by the number of all-pairs shortest paths in the graph. The algorithm has O(nm) time complexity. |
| [betweenness_centrality_online](https://memgraph.com/docs/mage/query-modules/cpp/betweenness-centrality-online)  | C++    | The betweenness centrality of a node is defined as the sum of the of all-pairs shortest paths that pass through the node divided by the number of all-pairs shortest paths in the graph. The algorithm has O(nm) time complexity. |
| [biconnected_components](https://memgraph.com/docs/mage/query-modules/cpp/biconnected-components)                | C++    | An algorithm for calculating maximal biconnected subgraph. A biconnected subgraph is a subgraph with a property that if any vertex were to be removed, the graph will remain connected.                                           |
| [bipartite_matching](https://memgraph.com/docs/mage/query-modules/cpp/bipartite-matching)                        | C++    | An algorithm for calculating maximum bipartite matching, where matching is a set of nodes chosen in such a way that no two edges share an endpoint.                                                                               |
| [bridges](https://memgraph.com/docs/mage/query-modules/cpp/bridges)                                              | C++    | A bridge is an edge, which when deleted, increases the number of connected components. The goal of this algorithm is to detect edges that are bridges in a graph.                                                                 |
| [community_detection](https://memgraph.com/docs/mage/query-modules/cpp/community-detection)                      | C++    | The Louvain method for community detection is a greedy method for finding communities with maximum modularity in a graph. Runs in _O_(*n*log*n*) time.                                                                            |
| [community_detection_online](https://memgraph.com/docs/mage/query-modules/cpp/community-detection-online)        | C++    | A dynamic community detection algorithm suitable for large-scale graphs based upon label propagation. Runs in O(m) time and has O(mn) space complexity.                                                                           |
| [cycles](https://memgraph.com/docs/mage/query-modules/cpp/cycles)                                                | C++    | Algorithm for detecting cycles on graphs                                                                                                                                                                                          |
| [cugraph](https://memgraph.com/docs/mage/query-modules/cuda/cugraph)                                             | CUDA   | Collection of NVIDIA GPU-powered algorithms integrated in Memgraph. Includes centrality measures, link analysis and graph clusterings.                                                                                            |
| [distance_calculator](https://memgraph.com/docs/mage/query-modules/python/distance-calculator)                   | Python | Module for finding the geographical distance between two points defined with 'lng' and 'lat' coordinates.                                                                                                                         |
| [export_util](https://memgraph.com/docs/mage/query-modules/python/export-util)                                   | Python | A module for exporting the graph database in different formats (JSON).                                                                                                                                                            |
| [graph_analyzer](https://memgraph.com/docs/mage/query-modules/python/graph-analyzer)                             | Python | This Graph Analyzer query module offers insights about the stored graph or a subgraph.                                                                                                                                            |
| [graph_coloring](https://memgraph.com/docs/mage/query-modules/python/graph-coloring)                             | Python | An algorithm for assigning labels to the graph elements subject to certain constraints. In this form, it is a way of coloring the graph vertices such that no two adjacent vertices are of the same color.                        |
| [graph_util](https://memgraph.com/docs/mage/query-modules/cpp/graph_util)                                          | C++ | A module with common graph utility procedures in day-to-day operations with graphs.
| [igraph](https://memgraph.com/docs/mage/query-modules/python/igraphalg)                                          | Python | A module that provides igraph integration with Memgraph and implements igraph algorithms                                                                                                                                          |
| [import_util](https://memgraph.com/docs/mage/query-modules/python/import-util)                                   | Python | A module for importing data generated by the `export_util()`.                                                                                                                                                                     |
| [json_util](https://memgraph.com/docs/mage/query-modules/python/json-util)                                       | Python | A module for loading JSON from a string, local file or remote address.                                                                                                                                                                    |
| [katz_centrality](https://memgraph.com/docs/mage/query-modules/cpp/katz-centrality)                              | C++    | Katz centrality is a centrality measurement that outputs a node's influence based on the number of shortest paths and their weighted length.                                                                                      |
| [katz_centrality_online](https://memgraph.com/docs/mage/query-modules/cpp/katz-centrality-online)                | C++    | Online implementation of the Katz centrality. Outputs the approximate result for Katz centrality while maintaining the order of rankings.                                                                                         |
| [kmeans](https://memgraph.com/docs/mage/query-modules/python/kmeans)                                             | Python | An algorithm for clustering given data.                                                                                                                                                                                                  |
| [leiden_community_detection](https://memgraph.com/docs/mage/query-modules/cpp/leiden-community-detection)                      | C++    | The Leiden method for community detection is an improvement on the Louvain method, designed to find communities with maximum modularity in a graph while addressing issues of disconnected communities. Runs in _O_(*L* *m*)  time, where *L* is the number of iterations of the algorithm.
| [link_prediction_gnn](https://memgraph.com/docs/mage/query-modules/python/link-prediction-with-gnn)              | Python | A module for predicting links in graphs using graph neural networks.                                                                                                                                                          |
| [llm_util](https://memgraph.com/docs/mage/query-modules/python/llm-util)                                        | Python | A module that contains procedures describing graphs in a format best suited for large language models (LLMs).     |
| [max_flow](https://memgraph.com/docs/mage/query-modules/python/max-flow)                                         | Python | An algorithm for calculating maximum flow through a graph using capacity scaling                                                                                                                                                  |
| [meta_util](https://memgraph.com/docs/mage/query-modules/python/meta-util)                                                                                                             | Python | A module that contains procedures describing graphs on a meta-level.                                                                    |
| [node_classification_with_gnn](https://memgraph.com/docs/mage/query-modules/python/node-classification-with-gnn) | Python | A graph neural network-based node classification module.                                                                                                                                                                            |
| [node2vec](https://memgraph.com/docs/mage/query-modules/python/node2vec)                                         | Python | An algorithm for calculating node embeddings from static graphs.                                                                                                                                                                  |
| [node2vec_online](https://memgraph.com/docs/mage/query-modules/python/node2vec-online)                           | Python | An algorithm for calculating node embeddings as new edges arrive                                                                                                                                                                  |
| [node_similarity](https://memgraph.com/docs/mage/query-modules/python/node-similarity)                           | Python | A module that contains similarity measures for calculating the similarity between two nodes.                                                                                                                                      |
| [nxalg](https://memgraph.com/docs/mage/query-modules/python/nxalg)                                               | Python | A module that provides NetworkX integration with Memgraph and implements many NetworkX algorithms                                                                                                                                 |
| [pagerank](https://memgraph.com/docs/mage/query-modules/cpp/pagerank)                                            | C++    | An algorithm that yields the influence measurement based on the recursive information about the connected nodes influence                                                                                                         |
| [pagerank_online](https://memgraph.com/docs/mage/query-modules/cpp/pagerank-online)                              | C++    | A dynamic algorithm made for calculating PageRank in a graph streaming scenario.                                                                                                                                                  |
| [rust_example](cpp/pagerank_module/pagerank_online_module.cpp)                                                   | Rust   | Example of a basic module with input parameters forwarding, made in Rust.                                                                                                                                                         |
| [set_cover](https://memgraph.com/docs/mage/query-modules/python/set-cover)                                       | Python | The algorithm for finding minimum cost subcollection of sets that covers all elements of a universe.
| [set_property](https://memgraph.com/docs/mage/query-modules/python/set-property)                                 | C++    | Utility module to help dynamically set properties on nodes and relationships.                                                                                                                              |
| [temporal_graph_networks](https://memgraph.com/docs/mage/query-modules/python/temporal-graph-networks)           | Python | GNN temporal graph algorithm to predict links or do node classification.                                                                                                                                                          |
| [tsp](https://memgraph.com/docs/mage/query-modules/python/tsp)                                                   | Python | An algorithm for finding the shortest possible route that visits each vertex exactly once.                                                                                                                                        |
| [union_find](https://memgraph.com/docs/mage/query-modules/python/union-find)                                     | Python | A module with an algorithm that enables the user to check whether the given nodes belong to the same connected component.                                                                                                         |
| [uuid_generator](https://memgraph.com/docs/mage/query-modules/cpp/uuid-generator)                                | C++    | A module that generates a new universally unique identifier (UUID).                                                                                                                                                               |
| [vrp](https://memgraph.com/docs/mage/query-modules/python/vrp)                                                   | Python | An algorithm for finding the shortest route possible between the central depot and places to be visited. The algorithm can be solved with multiple vehicles that represent a visiting fleet.                                      |
| [weakly_connected_components](https://memgraph.com/docs/mage/query-modules/cpp/weakly-connected-components)      | C++    | A module that finds weakly connected components in a graph.                                                                                                                                                                       |

## Advanced configuration

- [Advanced query module directories
  setup](https://memgraph.com/docs/mage/installation/source#advanced-configuration)
- [Developing MAGE with
  Docker](https://memgraph.com/docs/mage/installation/docker-build#developing-mage-with-docker)

## Testing MAGE

To test that everything is built, loaded, and working correctly, a python script
can be run. Make sure that the Memgraph instance with **MAGE** is up and
running.

```
# Running unit tests for C++ and Python
python3 tests/test_unit

# Running end-to-end tests
python3 tests/test_e2e
```

Furthermore, to test only specific end-to-end tests, you can add argument `-k`
with substring referring to the algorithm that needs to be tested. To test a
module named `<query_module>`, you would have to run `python3 tests/test_e2e -k <query_module>` where `<query_module>` is the name of the specific module you
want to test.

```
# Running specific end-to-end tests
python3 tests/test_e2e -k weakly_connected_components
```

## Contributing

We encourage everyone to contribute with their own algorithm implementations and
ideas. If you want to contribute or report a bug, please take a look at the
[contributions guide](../CONTRIBUTING.md).

## Code of Conduct

Everyone participating in this project is governed by the [Code of
Conduct](CODE_OF_CONDUCT.md). By participating, you are expected to uphold this
code. Please report unacceptable behavior to <tech@memgraph.com>.

## Feedback

Your feedback is always welcome and valuable to us. Please don't hesitate to
post on our [Community Forum](https://discourse.memgraph.com/).
