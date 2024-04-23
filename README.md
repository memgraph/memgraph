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
  <a href="https://github.com/memgraph/memgraph/blob/master/licenses/MEL.txt" alt="Documentation">
    <img src="https://img.shields.io/badge/license-MEL-yellow" alt="license" title="license"/>
  </a>
</p>

<p align="center">
  <a href="https://github.com/memgraph/memgraph">
     <img src="https://img.shields.io/github/actions/workflow/status/memgraph/memgraph/package_memgraph.yaml?branch=master&label=build%20and%20test&logo=github"/>
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

Memgraph is an open source graph database built for real-time streaming and
compatible with Neo4j. Whether you're a developer or a data scientist with
interconnected data, Memgraph will get you the immediate actionable insights
fast.

Memgraph directly connects to your streaming infrastructure. You can ingest data
from sources like Kafka, SQL, or plain CSV files. Memgraph provides a standard
interface to query your data with Cypher, a widely-used and declarative query
language that is easy to write, understand and optimize for performance. This is
achieved by using the property graph data model, which stores data in terms of
objects, their attributes, and the relationships that connect them. This is a
natural and effective way to model many real-world problems without relying on
complex SQL schemas.

Memgraph is implemented in C/C++ and leverages an in-memory first architecture
to ensure that you’re getting the [best possible
performance](http://memgraph.com/benchgraph) consistently and without surprises.
It’s also ACID-compliant and highly available.

## :zap: Features

- Custom query modules - Run Python, Rust, and C/C++ code natively; check out
  the [MAGE](https://github.com/memgraph/mage) graph algorithm library.
- Deep-path traversals - Use advanced capabilities such as accumulators and path
  filtering without adding additional application logic.
- Native support for machine learning
- Streaming support & dynamic algorithms
- Multi-tenancy
- High availability replication
- Authentication & authorization
- Role-based and label-based access control
- Monitoring via HTTP server



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

You can find the binaries and Docker images on the [Download
Hub](https://memgraph.com/download) and the installation instructions in the
[official documentation](https://memgraph.com/docs/memgraph/installation).


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

## :bookmark_tabs: Documentation

The Memgraph documentation is available at
[memgraph.com/docs](https://memgraph.com/docs).

## :question: Configuration

Command line options that Memgraph accepts are available in the [reference
guide](https://memgraph.com/docs/memgraph/reference-guide/configuration).

## :trophy: Contributing

Welcome to the heart of Memgraph development! We're on a mission to supercharge Memgraph, making it faster, more user-friendly, and even more powerful. We owe a big thanks to our fantastic community of contributors who help us fix bugs and bring incredible improvements to life. If you're passionate about databases and open source, here's your chance to make a difference!

### Compile from Source

Read the ["### Compile from Source

Read the ["Building Memgraph from source"](https://memgraph.com/docs/getting-started/build-memgraph-from-source) documentation page to learn how to download, compile, and run Memgraph from its source code.

"](https://memgraph.com/docs/getting-started/build-memgraph-from-source) documentation page to learn how to download, compile, and run Memgraph from its source code.

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
[MEL license](./licenses/MEL.txt).

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
