## Interfacing with Memgraph

This chapter describes ways to access the Memgraph database.

Currently supported interface is the [*Bolt protocol*](#bolt-protocol). We are
also working on providing a web based interface, which could be used through a
web browser.

### Bolt Protocol

The [Bolt protocol](https://boltprotocol.org/) was designed for database
applications and it aims to be efficient. Memgraph is using
[Version 1](https://boltprotocol.org/v1/) of the protocol.

Besides using the Bolt protocol specification to build a custom driver, you
can use the already available drivers. Official Bolt protocol drivers are
provided for the following languages.

  * [C#](http://neo4j.com/docs/api/dotnet-driver)
  * [Java](http://neo4j.com/docs/api/java-driver)
  * [JavaScript](http://neo4j.com/docs/api/javascript-driver)
  * [Python](http://neo4j.com/docs/api/python-driver)

They can be used for easier building custom interfaces for Memgraph. We
recommend using drivers starting from version 1.3.
