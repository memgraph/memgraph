# Distributed Query Execution

Add the ability to execute graph algorithms on a cluster of machines. The scope
of this is ONLY the query execution without changing the underlying storage
because that's much more complex. The first significant decision here is to
figure out do we implement our own distributed execution engine or deploy
something already available, like [Giraph](https://giraph.apache.org). An
important part is that Giraph by itself isn't enough because people want to
update data on the fly. The final solution needs to provide some updating
capabilities.
