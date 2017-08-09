# distributed memgraph

This subdirectory structure implements distributed infrastructure of Memgraph.

## conventions

1. Locked: A method having a Locked... prefix indicates that you
have to lock the appropriate mutex before calling this function.

## dependencies

* cereal
* <other memgraph dependencies>