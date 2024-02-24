# Memgraph Release Packaging

## Design Decisions

* Being able to build and test memgraph locally under Docker because multi-OS support
* Being able to push Docker images to the centralized repo (DockerHub)
* Being able to build and test memgraph under CI under Docker
* Being able to iterate on the code changes when adding support for new toolchains and OSe
* Being able to pick and choose toolchains, OSes, build type, hardware architecture
* Take care of enterprise keys, telemetry
* Create generator for Dockerfiles
