# Compound GRANT / DENY / REVOKE

**Status:** Proposed
**Author:** Colin Barry
**Last updated:** 2026-08-11

> Allow multiple permission types to be granted, denied, or revoked in a single
> statement, so that the resulting auth state change is atomic: either every
> permission in the statement is applied, or none of them are.

---

## 1. Motivation

Memgraph's fine-grained access control (FGAC) separates label-level permissions
(e.g. `GRANT READ ON NODES CONTAINING LABELS :Person TO alice`) from
property-level permissions (e.g. `GRANT READ {name} ON NODES CONTAINING LABELS
:Person TO alice`) from clause privileges (`GRANT MATCH TO alice`). In practice,
granting useful access to a label requires both the LBAC permission and clause
privilege. Similarly granting PBAC typically requires PBAC for the property,
LBAC for the labels, and the clause privilege to execute the query.

Today each of these is a separate Cypher statement. Each statement independently
acquires the auth lock, mutates the user or role, and persists. There is no way
to express "grant all of these together" in a single statement.

This is a real customer pain point. Users expect `GRANT READ {*} ON NODES
CONTAINING LABELS :Person TO alice` to make `:Person` nodes visible, but it
does not; separate label-level `GRANT READ` and privilege-level `GRANT MATCH`
are still required. The statements cannot be executed atomically because
Memgraph has no transaction support for auth operations.

Compound statements solve this by letting the user express multiple permission
and privilege blocks in a single statement targeting one user or role. The
entire statement executes under a single lock acquisition and a single persist
call, giving all-or-nothing semantics.

```cypher
/* Today: five separate statements, not atomic */
GRANT MATCH, CREATE TO alice;
GRANT READ ON NODES CONTAINING LABELS :Employee TO alice;
GRANT READ {name, age} ON NODES CONTAINING LABELS :Employee TO alice;
GRANT READ ON EDGES OF TYPE :WORKS_AT TO alice;
GRANT READ {since} ON EDGES OF TYPE :WORKS_AT TO alice;

/* Compound: single atomic statement */
GRANT MATCH, CREATE
  AND READ ON NODES CONTAINING LABELS :Employee
  AND READ {name, age} ON NODES CONTAINING LABELS :Employee
  AND READ ON EDGES OF TYPE :WORKS_AT
  AND READ {since} ON EDGES OF TYPE :WORKS_AT
  TO alice;
```

---

## 2. Core principle

> One statement, one lock, one persist. The auth state moves atomically from
> "before" to "after"; there is no observable intermediate state.

Every permission or privilege listed in a compound statement is validated before
any mutation begins. If any permission in the list is invalid (e.g. a privilege
that does not exist, or a property permission combined with an incompatible
entity type), the entire statement is rejected and nothing changes. If all are
valid, they are applied to the in-memory user/role object and persisted in a
single `SaveUser`/`SaveRole` call.

---

## 3. Syntax

A compound statement is a list of **blocks** separated by `AND`. Each block is
strictly one of:

- **Privilege block:** a comma-separated list of privilege keywords
  (e.g. `MATCH, CREATE`) or `ALL PRIVILEGES`. Has no `ON` clause.
- **LBAC block:** a comma-separated list of granular permissions
  (e.g. `READ`, `UPDATE`, `CREATE`, `DELETE`, `SET LABEL`, `*`) followed by
  `ON entityTypeSpec`.
- **PBAC block:** a comma-separated list of property permission types
  (`READ`, `SET PROPERTY`) followed by a property list (`{name, age}` or
  `{*}`) and `ON entityTypeSpec`.

LBAC and PBAC items cannot be mixed within a single block. A PBAC block is
identified by the presence of a property list (`{...}`).

For FGAC-only statements, commas can also separate blocks (matching the
existing `entityPrivilegeList` grammar). `AND` is required when mixing
privilege blocks with FGAC blocks.

The entire statement targets exactly one verb (GRANT, DENY, or REVOKE) and
exactly one user or role. Blocks can appear in any order.

### 3.1 Block separators

`AND` separates blocks. This is required to disambiguate privilege keywords
from FGAC permission keywords that share the same name (`CREATE`, `DELETE`).
A block without an `ON` clause is a privilege block; a block ending with
`ON entityTypeSpec` is an FGAC block (LBAC or PBAC, distinguished by the
presence of a property list).

For FGAC-only compound statements, commas can also separate blocks, preserving
the existing `entityPrivilegeList` grammar. `AND` is required when privilege
blocks are present.

### 3.2 Disambiguation of CREATE and DELETE

`CREATE` and `DELETE` appear in both the global privilege list and the FGAC
granular permission list. They mean different things in each context:

   - `CREATE` as a privilege gates the ability to execute Cypher `CREATE`
     clauses at all.
   - `CREATE` as a permission gates creating nodes/edges with a specific label
     or edge type.
   - `DELETE` as a privilege gates the ability to execute Cypher
     `DELETE`/`DETACH DELETE` clauses at all.
   - `DELETE` as a permission gates deleting nodes with a specific label.

The `AND` block separator resolves this: a block followed by `ON` is always
FGAC; a block followed by `AND`, `TO`, or `FROM` (with no `ON`) is always a
privilege block. This means `CREATE` and `DELETE` as FGAC permissions must
always appear in a block that includes `ON entityTypeSpec`. A standalone
`CREATE` or `DELETE` (terminated by `AND`/`TO`/`FROM`) is always interpreted
as a global privilege.

`SET` and `REMOVE` also appear in both lists but are unambiguous: `SET` and
`REMOVE` as bare tokens only appear in the privilege list, not in
`granularPrivilege` (which has the multi-token forms `SET LABEL`,
`SET PROPERTY`, `REMOVE LABEL`).

### 3.3 Grammar sketch

These three compound rules replace the six existing rules (`grantPrivilege`,
`denyPrivilege`, `revokePrivilege`, `grantPropertyPermission`,
`denyPropertyPermission`, `revokePropertyPermission`). All previously valid
statements parse identically under the new rules.

```ebnf
propertyPermissionType = 'READ' | 'SET' 'PROPERTY' ;
propertyPermissionTypeList = propertyPermissionType {',' propertyPermissionType} ;
propertyList     = '{' ( '*' | symbolicName {',' symbolicName} ) '}' ;

lbacBlock        = granularPrivilege {',' granularPrivilege} 'ON' entityTypeSpec ;
pbacBlock        = propertyPermissionTypeList propertyList 'ON' entityTypeSpec ;
privilegeBlock   = privilegesList | 'ALL' 'PRIVILEGES' ;

fgacBlock        = lbacBlock | pbacBlock ;
block            = privilegeBlock | fgacBlock ;
blockSep         = 'AND' | ',' ;   (* comma only valid between fgacBlocks *)

grantCompound    = 'GRANT' block {blockSep block} 'TO' userOrRole ;
denyCompound     = 'DENY' block {blockSep block} 'TO' userOrRole ;
revokeCompound   = 'REVOKE' block {blockSep block} 'FROM' userOrRole ;
```

The comma separator between blocks is only valid between FGAC blocks
(preserving existing `entityPrivilegeList` syntax). When a privilege block is
present, `AND` must be used. This constraint may be enforced at the grammar
level or as a semantic check.

### 3.4 Examples

**Privilege-only (unchanged from existing syntax)**
```cypher
GRANT MATCH, CREATE TO alice;
GRANT ALL PRIVILEGES TO alice;
```

**LBAC-only (unchanged from existing syntax)**
```cypher
GRANT READ ON NODES CONTAINING LABELS :Employee TO alice;
```

**PBAC-only (unchanged from existing syntax)**
```cypher
GRANT READ {name, age} ON NODES CONTAINING LABELS :Employee TO alice;
GRANT READ, SET PROPERTY {name, age} ON NODES CONTAINING LABELS :Employee TO alice;
```

**Multi-block LBAC (existing comma syntax still works, AND also works)**
```cypher
// Existing syntax: commas between LBAC blocks (still valid)
GRANT READ ON NODES CONTAINING LABELS :Employee,
  READ ON NODES CONTAINING LABELS :Manager
  TO alice;

// New syntax: AND between blocks (also valid)
GRANT READ ON NODES CONTAINING LABELS :Employee
  AND READ ON NODES CONTAINING LABELS :Manager
  TO alice;
```

**LBAC + PBAC**
```cypher
GRANT READ, UPDATE ON NODES CONTAINING LABELS :Employee
  AND READ {name, age} ON NODES CONTAINING LABELS :Employee
  TO alice;
```

**Multiple entity targets**
```cypher
GRANT READ ON NODES CONTAINING LABELS :Employee
  AND READ {name} ON NODES CONTAINING LABELS :Employee
  AND READ ON EDGES OF TYPE :WORKS_AT
  AND READ {weight} ON EDGES OF TYPE :WORKS_AT
  TO alice;
```

**Privileges + FGAC**
```cypher
GRANT MATCH, CREATE
  AND READ ON NODES CONTAINING LABELS :Employee
  AND READ {name, age} ON NODES CONTAINING LABELS :Employee
  TO alice;
```

**ALL PRIVILEGES + FGAC**
```cypher
GRANT ALL PRIVILEGES
  AND READ ON NODES CONTAINING LABELS :Employee
  AND READ {name} ON NODES CONTAINING LABELS :Employee
  AND READ ON EDGES OF TYPE :WORKS_AT
  TO alice;
```

**FGAC first, privileges after (any order)**
```cypher
GRANT READ ON NODES CONTAINING LABELS :Employee
  AND MATCH
  TO alice;
```

**DENY**
```cypher
DENY MATCH
  AND READ ON NODES CONTAINING LABELS :Secret
  TO intern_role;

DENY READ ON NODES CONTAINING LABELS :Secret
  AND READ {salary} ON NODES CONTAINING LABELS :Employee
  TO alice;
```

**REVOKE**
```cypher
REVOKE * ON NODES CONTAINING LABELS :Employee
  AND READ, SET PROPERTY {*} ON NODES CONTAINING LABELS :Employee
  FROM alice;
```

---

## 4. Execution model

A compound statement:

1. **Parses** into a single AST node carrying a list of blocks (each tagged as
   a privilege block, LBAC block, or PBAC block) and a user/role target.

2. **Acquires the auth write lock** once.

3. **Resolves** the target user or role. If neither exists, the statement fails
   and no changes are made.

4. **Validates** the target. No new runtime validation is introduced beyond
   what existing single-block statements already perform (user/role existence,
   license checks, caller authorization). All permission and privilege names
   are validated at parse time by the grammar.

5. **Applies** each block to the in-memory user/role object. Global privileges
   are applied via `Permissions::Grant`/`Deny`/`Revoke`. Entity blocks are
   applied via the existing `FineGrainedAccessPermissions` and
   `PropertyAccessPermissions` methods.

6. **Persists** with a single `SaveUser` or `SaveRole` call. This is a single
   atomic kvstore operation.

7. **Releases** the lock.

`GetUser`/`GetRole` returns a copy of the persisted object. All mutations in
step 5 happen on this copy. The copy replaces the cached state only after
`SaveUser`/`SaveRole` succeeds in step 6. If persistence fails, the copy is
discarded and the on-disk state is unchanged. No rollback mechanism is needed.

---

## 5. Error semantics

| Condition | Behavior |
|---|---|
| Target user/role does not exist | Statement fails; no changes. |
| Duplicate permission in a block (e.g. `READ, READ`) | Accepted; second application is idempotent. |
| Multiple privilege blocks (e.g. `GRANT MATCH AND CREATE TO alice`) | Accepted; semantically equivalent to `GRANT MATCH, CREATE TO alice`. |
| Same entity target in multiple blocks (e.g. two blocks for `:Employee`) | Accepted; permissions from both blocks are applied. |
| `ALL PRIVILEGES` combined with explicit privileges | Accepted; the explicit privileges are redundant but harmless. `ALL PRIVILEGES` applies only to global privileges; it does not grant FGAC entity-level permissions. |
| Mix of LBAC and PBAC items in a single block (e.g. `READ, READ {x} ON ...`) | Rejected at parse time. Use separate blocks: `READ ON ... AND READ {x} ON ...`. |
| Invalid privilege/permission name | Rejected at parse time (syntax error). |
| Property permission on edges without `OF TYPE` | Rejected at parse time (grammar does not match). |
| `SaveUser`/`SaveRole` persistence failure | Exception propagates; the in-memory copy is discarded. The on-disk state is unchanged. |

---

## 6. Backwards compatibility

The compound syntax is fully backwards compatible. Every existing single-block
statement is a valid compound statement with one block:

- `GRANT MATCH TO alice` -- single privilege block, unchanged.
- `GRANT ALL PRIVILEGES TO alice` -- single privilege block, unchanged.
- `GRANT READ ON NODES CONTAINING LABELS :Employee TO alice` -- single LBAC
  block, unchanged.
- `GRANT READ {name} ON NODES CONTAINING LABELS :Employee TO alice` -- single
  PBAC block, unchanged.
- `GRANT READ, SET PROPERTY {name} ON NODES CONTAINING LABELS :Employee TO
  alice` -- single PBAC block with comma-separated items, unchanged.
- `GRANT READ ON NODES CONTAINING LABELS :A, READ ON NODES CONTAINING LABELS
  :B TO alice` -- existing comma-separated multi-block entity syntax
  (`entityPrivilegeList`), unchanged.

`AND` is purely additive: it provides a new way to separate blocks, required
when mixing privileges with FGAC but available everywhere.

No changes to `SHOW PRIVILEGES` or the audit log are needed. `SHOW PRIVILEGES`
enumerates individual permissions held by the user/role; a compound statement
that grants N permissions produces the same N rows as N individual statements
would. The audit log records one entry per query, so a compound statement
appears as a single entry containing the full statement text.

No wire format, storage format, or replication protocol changes are needed.
Compound statements produce the same in-memory mutations as the equivalent
sequence of individual statements; the only difference is that they are applied
and persisted together. `SaveUser`/`SaveRole` produces a single
`UpdateAuthData` system transaction action that replicates the entire serialized
user/role object, so atomicity is preserved on replicas for free.

---

## 7. Constraints

A compound statement targets exactly:

- **One verb.** GRANT, DENY, or REVOKE. These cannot be mixed in a single
  statement.
- **One user or role.** Granting the same permissions to multiple users requires
  separate statements.

---

## 8. Known limitations and future direction

- **Multiple user/role targets.** Supporting `TO alice, bob` could be added
  later if there is demand.
- **Mixed verbs.** A single statement uses exactly one verb. Combining
  GRANT and DENY (e.g. grant READ on `:Public` and deny READ on `:Secret`)
  requires separate statements.

---

## 9. Alternatives considered

### Full auto-auth (implicit label grant on property grant)

When a user grants property-level access, automatically grant the corresponding
label-level access too. This matches the customer's original expectation ("why
doesn't `GRANT READ {*}` make nodes visible?").

**Rejected.** The DENY and REVOKE semantics are ambiguous. Should denying a
property implicitly deny the label? Should revoking a property revoke the label
only if no other properties remain granted? These questions have no obvious
answers, and implicit side effects in an authorization system are dangerous.

### GRANT-only auto-auth (hybrid)

A narrower variant: automatically grant label-level READ when property-level
READ is granted, but only for GRANT. DENY and REVOKE remain fully explicit.

This solves the customer's specific problem with zero new syntax. However, it
introduces an asymmetry: GRANT has implicit side effects while DENY and REVOKE
do not. This is surprising in its own way ("why did GRANT add permissions I
didn't ask for?") and does not generalize to the broader problem of needing
multiple permission types applied atomically. Compound statements solve the same
problem explicitly and consistently across all three verbs.

### Auth transactions (BEGIN / COMMIT for auth queries)

Wrap multiple auth statements in a transaction block, giving full atomicity:
```cypher
BEGIN;
GRANT READ ON NODES CONTAINING LABELS :Person TO alice;
GRANT READ {name} ON NODES CONTAINING LABELS :Person TO alice;
COMMIT;
```

This would be the most general solution, but auth queries run outside the normal
transaction machinery. Adding transactional semantics to auth would require a
new lock-holding model, rollback support, and interaction with the replication
protocol. The implementation cost is disproportionate to the problem being
solved. Compound statements achieve the same atomicity for the common case at a
fraction of the complexity.

### Comma-only block separator

Using commas to separate blocks works for FGAC-only compound statements (LBAC
and PBAC blocks), but becomes ambiguous when privilege blocks are added:
`CREATE` and `DELETE` are both privilege names and FGAC permission names, so
`GRANT CREATE, READ ON :Employee TO alice` cannot be parsed unambiguously. `AND`
is required to separate blocks that could contain these overloaded keywords.
