<p align="center">
<img src="https://public-assets.memgraph.com/github-readme-images/github-memgraph-repo-banner.png">
</p>

---

<p align="center">
  <a href="https://github.com/memgraph/memgraph/blob/master/licenses/APL.txt">
    <img src="https://img.shields.io/badge/license-APL-green" alt="license" title="license"/>
  </a>
  <a href="https://github.com/memgraph/memgraph/blob/master/licenses/BSL.txt">
    <img src="https://img.shields.io/badge/license-BSL-yellowgreen" alt="license" title="license"/>
  </a>
  <a href="https://github.com/memgraph/memgraph/blob/master/licenses/MEL.pdf" alt="Documentation">
    <img src="https://img.shields.io/badge/license-MEL-yellow" alt="license" title="license"/>
  </a>
</p>

<p align="center">
  <a href="https://memgraph.github.io/daily-builds/">
     <img src="https://img.shields.io/github/actions/workflow/status/memgraph/memgraph/daily_build.yml?branch=master&label=build%20and%20test&logo=github"/>
  </a>
  <a href="https://memgraph.com/docs/" alt="Documentation">
    <img src="https://img.shields.io/badge/documentation-Memgraph-orange" />
  </a>
</p>

<p align="center">
  <a href="https://memgr.ph/join-discord">
    <img src="https://img.shields.io/badge/Discord-7289DA?style=for-the-badge&logo=discord&logoColor=white" alt="Discord"/>
  </a>
</p>

## :clipboard: Description

Memgraph is a high-performance, in-memory graph database that powers real-time
AI context and graph analytics. Built in C/C++, it serves as the graph engine
for GraphRAG pipelines, AI memory systems, and agentic workflows — delivering
sub-millisecond multi-hop traversals for any system that needs structured,
connected context alongside semantic search.

Vector search finds what’s similar. Graph reasoning finds what’s connected —
following relationships, dependencies, and hierarchies that similarity alone
can’t capture. Memgraph provides both in a single query layer: built-in vector
indexes for similarity search combined with full graph traversal, so retrieval
pipelines can run as a single atomic database operation instead of being
scattered across multiple systems.

The same architecture drives real-time graph analytics for fraud detection,
network analysis, infrastructure monitoring, and other operational workloads
where milliseconds matter. Memgraph is fully compatible with Neo4j’s Cypher
query language, ACID-compliant, and highly available.

## :zap: Features

#### AI & Graph Intelligence
- **Vector search indexes** - Built-in vector indexes with cosine similarity for
  hybrid graph + vector retrieval in a single query.
- **Atomic GraphRAG** - Pivot search, graph expansion, ranking, and prompt
  assembly expressed as a single Cypher query.
- **[MAGE](mage/README.md) algorithm library** - 40+ graph algorithms in
  C++, Python, and CUDA including PageRank, community detection, GNN-based
  link prediction, and temporal graph networks.
- **LLM utility module** - Graph-aware context formatting for large language
  models.
- **Real-time schema introspection** - `SHOW SCHEMA INFO` returns the full
  graph ontology for Text2Cypher and AI agent integration.

#### Performance & Query Power
- **In-memory C/C++ engine** - Sub-millisecond traversals with [benchmarked
  performance](https://memgraph.com/benchgraph).
- **Deep-path traversals** - Accumulators and path filtering without additional
  application logic.
- **Custom query modules** - Extend with Python, Rust, and C/C++ code natively.
- **Streaming support** - Ingest from Kafka, Pulsar, and RedPanda with dynamic
  graph algorithms that react to changes in real time.

#### Enterprise
- **High availability** - Raft-based coordination with automatic failover.
- **Multi-tenancy** - Isolated databases with per-tenant role assignments.
- **Fine-grained access control** - Role-based and label-based permissions at
  the node and edge level.
- **Authentication & authorization** - SSO integration, user impersonation, and
  30+ granular permissions.
- **Encryption in transit, monitoring, backup & restore.**



## :video_game: Memgraph Playground

You don't need to install anything to try out Memgraph. Check out
our **[Memgraph Playground](https://playground.memgraph.com/)** sandboxes in
your browser.

<p align="left">
  <a href="https://playground.memgraph.com/">
    <img width="450px" alt="Memgraph Playground" src="https://download.memgraph.com/asset/github/memgraph/memgraph-playground.png">
  </a>
</p>

## :floppy_disk: Download & Install

### Windows

[![Windows](https://img.shields.io/badge/Windows-Docker-0078D6?style=for-the-badge&logo=windows&logoColor=white)](https://memgraph.com/docs/memgraph/install-memgraph-on-windows-docker)
[![Windows](https://img.shields.io/badge/Windows-WSL-0078D6?style=for-the-badge&logo=windows&logoColor=white)](https://memgraph.com/docs/memgraph/install-memgraph-on-windows-wsl)

### macOS

[![macOS](https://img.shields.io/badge/macOS-Docker-000000?style=for-the-badge&logo=macos&logoColor=F0F0F0)](https://memgraph.com/docs/memgraph/install-memgraph-on-macos-docker)
[![macOS](https://img.shields.io/badge/lima-AACF41?style=for-the-badge&logo=macos&logoColor=F0F0F0)](https://memgraph.com/docs/memgraph/install-memgraph-on-ubuntu)

### Linux

[![Linux](https://img.shields.io/badge/Linux-Docker-FCC624?style=for-the-badge&logo=linux&logoColor=black)](https://memgraph.com/docs/memgraph/install-memgraph-on-linux-docker)
[![Debian](https://img.shields.io/badge/Debian-D70A53?style=for-the-badge&logo=debian&logoColor=white)](https://memgraph.com/docs/memgraph/install-memgraph-on-debian)
[![Ubuntu](https://img.shields.io/badge/Ubuntu-E95420?style=for-the-badge&logo=ubuntu&logoColor=white)](https://memgraph.com/docs/memgraph/install-memgraph-on-ubuntu)
[![Cent OS](https://img.shields.io/badge/cent%20os-002260?style=for-the-badge&logo=centos&logoColor=F0F0F0)](https://memgraph.com/docs/memgraph/install-memgraph-from-rpm)
[![Fedora](https://img.shields.io/badge/fedora-0B57A4?style=for-the-badge&logo=fedora&logoColor=F0F0F0)](https://memgraph.com/docs/memgraph/install-memgraph-from-rpm)
[![RedHat](https://img.shields.io/badge/redhat-EE0000?style=for-the-badge&logo=redhat&logoColor=F0F0F0)](https://memgraph.com/docs/memgraph/install-memgraph-from-rpm)

### Kubernetes

[![Helm](https://img.shields.io/badge/Helm_Charts-0F1689?style=for-the-badge&logo=helm&logoColor=white)](https://github.com/memgraph/helm-charts)

Deploy Memgraph on Kubernetes using the official [Helm charts](https://github.com/memgraph/helm-charts),
including charts for standalone and high-availability deployments:

```bash
helm repo add memgraph https://memgraph.github.io/helm-charts
helm install my-memgraph memgraph/memgraph
```

You can find the binaries and Docker images on the [Download
Hub](https://memgraph.com/download) and the installation instructions in the
[official documentation](https://memgraph.com/docs/memgraph/installation).


## :rocket: Daily Builds

Stay on the cutting edge with the latest features and improvements by using
[Memgraph Daily Builds](https://memgraph.github.io/daily-builds/). Daily builds
are updated frequently and allow you to test new capabilities before they reach
stable releases.

<p align="left"> <a href="https://memgraph.github.io/daily-builds/"> <img src="https://img.shields.io/badge/Daily%20Builds-latest-blue?style=for-the-badge" alt="Daily Builds" /> </a> </p>


## :cloud: Memgraph Cloud

Check out [Memgraph Cloud](https://memgraph.com/docs/memgraph-cloud) - a cloud service fully managed on AWS and available in 6 geographic regions around the world. Memgraph Cloud allows you to create projects with Enterprise instances of MemgraphDB from your browser.

<p align="left">
  <a href="https://memgraph.com/docs/memgraph-cloud">
    <img width="450px" alt="Memgraph Cloud" src="https://public-assets.memgraph.com/memgraph-gifs%2Fcloud.gif">
  </a>
</p>

## :link: Connect to Memgraph

[Connect to the database](https://memgraph.com/docs/memgraph/connect-to-memgraph) using Memgraph Lab, mgconsole, various drivers (Python, C/C++ and others) and WebSocket.

### :microscope: Memgraph Lab

Visualize graphs and play with queries to understand your data. [Memgraph Lab](https://memgraph.com/docs/memgraph-lab) is a user interface that helps you explore and manipulate the data stored in Memgraph. Visualize graphs, execute ad hoc queries, and optimize their performance.

<p align="left">
  <a href="https://memgraph.com/docs/memgraph-lab">
    <img width="450px" alt="Memgraph Cloud" src="https://public-assets.memgraph.com/memgraph-gifs%2Flab.gif">
  </a>
</p>

## :file_folder: Import data

[Import data](https://memgraph.com/docs/memgraph/import-data) into Memgraph using Kafka, RedPanda or Pulsar streams, CSV and JSON files, or Cypher commands.

## :bulb: Best Practices

The [memgraph/best-practices](https://github.com/memgraph/best-practices)
repository contains ready-to-use examples covering graph modeling, data import,
query optimization, GraphRAG, high-availability deployment, and more.

## :bookmark_tabs: Documentation

The Memgraph documentation is available at
[memgraph.com/docs](https://memgraph.com/docs).

## :question: Configuration

Command line options that Memgraph accepts are available in the [reference
guide](https://memgraph.com/docs/memgraph/reference-guide/configuration).

## :trophy: Contributing

Welcome to the heart of Memgraph development! We're on a mission to supercharge Memgraph, making it faster, more user-friendly, and even more powerful. We owe a big thanks to our fantastic community of contributors who help us fix bugs and bring incredible improvements to life. If you're passionate about databases and open source, here's your chance to make a difference!

### Compile from Source

Learn how to download, compile and run Memgraph from source with the [Quick Start](https://memgraph.notion.site/Quick-Start-82a99a85e62a4e3d89f6a9fb6d35626d) guide.


### Explore Memgraph Internals

Interested in the nuts and bolts of Memgraph? Our [internals documentation](https://memgraph.notion.site/Memgraph-Internals-12b69132d67a417898972927d6870bd2) is where you can uncover the inner workings of Memgraph's architecture, learn how to build the project from scratch, and discover the secrets of effective contributions. Dive deep into the database!

### Dive into the Contributing Guide
Ready to jump into the action? Explore our [contributing guide](CONTRIBUTING.md) to get the inside scoop on how we develop Memgraph. It's your roadmap for suggesting bug fixes and enhancements. Contribute your skills and ideas!

### Code of Conduct

Our commitment to a respectful and professional community is unwavering. Every participant in Memgraph is expected to adhere to a stringent Code of Conduct. Please carefully review [the complete text](CODE_OF_CONDUCT.md) to gain a comprehensive understanding of the behaviors that are both expected and explicitly prohibited.

We maintain a zero-tolerance policy towards any violations. Our shared commitment to this Code of Conduct ensures that Memgraph remains a place where integrity and excellence are paramount.

### :scroll: License

Memgraph Community is available under the [BSL
license](./licenses/BSL.txt).</br> Memgraph Enterprise is available under the
[MEL license](./licenses/MEL.pdf).

## :star: Special Thanks

We're grateful to all internal and especially external contributors who have
helped improve Memgraph through bug fixes, features, and documentation. Thank
you for making Memgraph better!

<a href="https://github.com/memgraph/memgraph/graphs/contributors">
  <img src="https://contrib.rocks/image?repo=memgraph/memgraph" />
</a>

## :busts_in_silhouette: Community

- :purple_heart: [**Discord**](https://discord.gg/memgraph)
- :ocean: [**Stack Overflow**](https://stackoverflow.com/questions/tagged/memgraphdb)
- :bird: [**Twitter**](https://twitter.com/memgraphdb)
- :movie_camera:
  [**YouTube**](https://www.youtube.com/channel/UCZ3HOJvHGxtQ_JHxOselBYg)

<p align="center">
  <a href="#">
    <img src="https://img.shields.io/badge/⬆️ back_to_top_⬆️-white" alt="Back to top" title="Back to top"/>
  </a>
</p>
