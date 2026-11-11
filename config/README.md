# Logging output

Memgraph uses text logs by default. To write newline-delimited JSON log records
to the configured log file and to stderr when stderr logging is enabled, start
Memgraph with:

```shell
memgraph --log-format=json
```

The same option can be added to the Memgraph configuration file. The supported
values are `text` (default) and `json`; the option takes effect at startup.
Destinations, severity filtering, file rotation, retention, and the
`--logger-type=sync|async` setting work as before. This option does not enable
stderr logging or change the log file location.

Each JSON record occupies one physical line and contains:

| Field       | Type    | Meaning                                                                              |
| ----------- | ------- | ------------------------------------------------------------------------------------ |
| `timestamp` | string  | Time the message was emitted, in UTC RFC 3339 format with millisecond precision.     |
| `level`     | string  | Existing severity name: `trace`, `debug`, `info`, `warning`, `error`, or `critical`. |
| `logger`    | string  | Logger name, normally `memgraph_log`.                                                |
| `message`   | string  | Original formatted message, with JSON escaping.                                      |
| `thread_id` | integer | Emitting thread's identifier, including with asynchronous logging.                   |

For example:

```json
{
  "timestamp": "2026-10-30T12:34:56.789Z",
  "level": "info",
  "logger": "memgraph_log",
  "message": "Session closed by peer",
  "thread_id": 42
}
```

Consumers should parse field names rather than depend on their ordering.
Newlines, quotes, backslashes, and control characters in messages are escaped.
Invalid UTF-8 bytes are replaced with the Unicode replacement character so
records remain valid JSON.

This setting does not enable query logging, change redaction, or make sensitive
message contents safe to export. Direct startup diagnostics and emergency writes
to stderr can still
be plain text. Separate audit and NuRaft log files are unaffected.

Memgraph Lab's monitoring WebSocket keeps its existing `event`, `level`, and
`message` representation. Sinks added separately keep their own format.

An OpenTelemetry Collector can ingest the JSON logs using its filelog receiver
and map their fields to its log data model. This is not the OTLP JSON wire
format: it does not add the OpenTelemetry SDK, native OTLP export, or trace IDs.

## Structured diagnostic events

Selected records also contain an `event` object with a `name` and an
`attributes` object. Numeric identifiers, ports, and durations remain JSON
numbers. Other records retain the basic fields above without either object.
Existing human-readable messages and session tags remain unchanged.

| `event.name`            | Attributes and enablement                                                                                                                                                                                                                                                        |
| ----------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `query.failed`          | `db.query.text`, `db.namespace`, `user.name`, `session.id`, `transaction.id`, `query.id`, `phase`, `error.type`, `error.message`, and `outcome=failure`, where available. Still requires `log.failed_queries` and uses ERROR severity. `phase` is `parse`, `prepare`, or `pull`. |
| `query.slow`            | Query/session identifiers, `duration_ms`, and `outcome=success`. Still requires the `log.min_duration_ms` threshold and uses WARNING severity. `query.plan` is only included when `log.query_plan` enables it.                                                                   |
| `connection.accepted`   | `network.protocol.name`, `client.address`, `client.port`, and the Bolt `session.id`, where available. Uses INFO severity.                                                                                                                                                        |
| `connection.closed`     | The same connection identifiers. Uses INFO severity. JSON mode also records server-initiated and WebSocket closures that previously had no log record.                                                                                                                           |
| `connection.error`      | Connection identifiers and `error.type`. Transport errors also carry numeric `error.code` and `error.message`; their type is the error category. Processing exceptions use `session_processing_error`, retaining their existing DEBUG message. Uses ERROR severity.              |
| `authentication.failed` | `session.id`, `outcome=failure`, and a stable `reason`: `unauthenticated`, `session_limit`, or `missing_sso_credentials`. Uses WARNING severity.                                                                                                                                 |

Authentication rejection records are new and only emitted when JSON logging
is enabled. They never include passwords, tokens, authentication metadata, or
attempted usernames. Existing authentication warnings are retained.

Query text, error messages, and plans have the same sensitivity as their
existing opt-in query log messages. JSON formatting is not redaction.
Some message content is repeated in attributes, increasing log size; the existing
query logging controls still determine which queries and plans are recorded.
Identifiers are omitted when not yet assigned; parse failures do not invent a
transaction or query ID. Transaction context is captured before commit clears
it. `query.id`, when present, is the transaction-local Bolt query identifier,
not a globally unique ID.

Event attributes are captured on the emitting thread and carried through the
existing logging queue in asynchronous mode. They are not read from session
state on the logging worker, and do not propagate to unrelated records or
other sessions. The internal transport envelope is removed before records
reach text or monitoring WebSocket sinks.
