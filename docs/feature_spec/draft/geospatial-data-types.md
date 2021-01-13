# Geospatial Data Types

Neo4j offers the following functionality:

* https://neo4j.com/docs/cypher-manual/current/syntax/spatial/
* https://neo4j.com/docs/cypher-manual/current/functions/spatial/

The question is, how are we going to support equivalent capabilities? We need
something very similar because these are, in general, very well defined types.

The main reasons for implementing this feature are:
  1. Ease of use. At this point, users have to encode/decode time data types
     manually.
  2. Memory efficiency in some cases because user defined encoding could still
     be more efficient.

The number of functionalities that could be built on top of geospatial types is
huge. Probably some C/C++ libraries should be used:
  * https://github.com/OSGeo/gdal.
  * http://geostarslib.sourceforge.net/ Furthermore, the query engine could use
    these data types during query execution (specific for query execution).
  * https://www.cgal.org.
Also, the storage engine could have specialized indices for these types of
data.

A note about the storage is that Memgraph has a limit on the total number of
different data types, 16 at this point. We have to be mindful of that during
the design phase.
