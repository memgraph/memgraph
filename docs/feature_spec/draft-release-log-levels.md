# Release Log Levels

It's impossible to control the log level in Memgraph Community. That means it's
tough to debug issues in interacting with Memgraph. At least three log levels
should be available to the user:

* Log nothing (as it is now).
* Log each executed query.
* Log Bolt server states.

Memgraph Enterprise has the audit log feature. The audit log provides
additional info about each query (user, source, etc.), but it's only available
in the Enterprise edition. Furthermore, the intention of audit logs isn't
debugging.
