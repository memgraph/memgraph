## Parent

`specs/coordinator-sso-auth/PRD.md`

## What to build

Remove the dead `EnableWritingOnMainRpc`. It is fully defined (type alias, request/response
structs, Save/Load), has a registered server handler, and has Prometheus counters, but no
code in any version ever sends it — writing on a promoted main is enabled via the
`writing_enabled` flag carried inside `PromoteToMainRpc`.

Delete the RPC and all its surface. Keep the underlying replication-layer method
`ReplicationState::EnableWritingOnMain()`, which is still used directly elsewhere.

Because no version ever sends this RPC, its removal is safe across a rolling upgrade.

## Acceptance criteria

- [ ] The `EnableWritingOnMainRpc` type alias and its request/response structs (with Save/Load) are removed.
- [ ] The server-handler registration and the handler implementation are removed.
- [ ] The `RpcInfoSpecialize` entry and the Prometheus counters for this RPC are removed.
- [ ] `ReplicationState::EnableWritingOnMain()` is retained and its existing callers are unaffected.
- [ ] The project builds and existing replication/coordination tests pass.

## Blocked by

None - can start immediately
