## Interfacing with Memgraph

This chapter describes ways to access the Memgraph database.

Currently only the [*Bolt protocol*](#bolt-protocol) is supported. We are
working on implementing a web-browser interface.

### Bolt Protocol

The [Bolt protocol](https://boltprotocol.org/) was designed for efficient
communication with graph databases. Memgraph supports
[Version 1](https://boltprotocol.org/v1/) of the protocol.

Official Bolt protocol drivers are provided for multiple programming languages:

  * [Java](http://neo4j.com/docs/api/java-driver)
  * [Python](http://neo4j.com/docs/api/python-driver)
  * [JavaScript](http://neo4j.com/docs/api/javascript-driver)
  * [C#](http://neo4j.com/docs/api/dotnet-driver)

They can be used for easier building custom interfaces for Memgraph. We
recommend using drivers starting from version 1.3.
