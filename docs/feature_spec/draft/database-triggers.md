# Database Triggers

Memgraph doesn't have any built-in notification mechanism yet. In the case a
user wants to get notified about anything happening inside Memgraph, the only
option is some pull mechanism from the client code. In many cases, that might
be suboptimal.

A natural place to start would be put to some notification code on each update
action inside Memgraph. It's probably too early to send a notification
immediately after WAL delta gets created, but at some point after transaction
commits or after WAL deltas are written to disk might be a pretty good place.
Furthermore, Memgraph has the query module infrastructure. The first
implementation might call a user-defined query module procedure and pass
whatever gets created or updated during the query execution.
