# Compound GRANT / DENY / REVOKE

**Status:** Proposed
**Author:** Colin Barry
**Last updated:** 2026-08-05

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

## 3. Scope tiers

The feature has a natural layering. Each tier adds a new permission shape to the
compound syntax. The tiers are listed here for discussion; which tiers ship in
which release is a product decision.

tl;dr:
- Tier 1 allows us to specify LBAC and PBAC permissions in a single query.
- Tier 2 allows us to specify LBAC permissions, PBAC permissions, and privileges in a
single query. **I propose we implement this**.

### Tier 1: LBAC and PBAC

Combine label-level FGAC permissions (`READ`, `UPDATE`, etc.) with
property-level FGAC permissions (`READ {props}`, `SET PROPERTY {props}`) in a
single statement. Each block is strictly one type: LBAC or PBAC. Multiple
blocks (different labels, different permission types, or a mix of nodes and
edges) are separated by commas.

```cypher
// Today: three separate statements, not atomic
GRANT READ ON NODES CONTAINING LABELS :Employee TO alice;
GRANT READ {name, age} ON NODES CONTAINING LABELS :Employee TO alice;
GRANT READ ON EDGES OF TYPE :WORKS_AT TO alice;

// Tier 1: single atomic statement
GRANT READ ON NODES CONTAINING LABELS :Employee,
  READ {name, age} ON NODES CONTAINING LABELS :Employee,
  READ ON EDGES OF TYPE :WORKS_AT
  TO alice;
```

Commas separate blocks within the compound statement, just as they do in
existing multi-target entity privilege grants. This is fully backwards
compatible with the current syntax.

This is the direct customer pain point and the minimum viable feature.

### Tier 2: LBAC, PBAC, and privileges

Customers are also confused by needing `GRANT MATCH TO alice` (a global
privilege) in addition to FGAC grants.

It is possible to extend compound statements to include global privileges
alongside FGAC permissions, as long as we disambiguate privileges.  Two
keywords appear in both the global privilege list and the FGAC granular
permission list: `CREATE` and `DELETE`. They mean different things in each
context:

   - `CREATE` as a privilege gates the ability to execute Cypher `CREATE`
     clauses at all.
   - `CREATE` as a permission gates creating nodes/edges with a specific label
     or edge type.
   - `DELETE` as a privilege gates the ability to execute Cypher
     `DELETE`/`DETACH DELETE` clauses at all.
   - `DELETE` as a permission gates deleting nodes with a specific label.

Without disambiguation, `GRANT CREATE, READ ON NODES CONTAINING LABELS
:Employee TO alice` is ambiguous: is `CREATE` the global privilege or the
label-level FGAC permission?

Tier 2 resolves this by using `AND` (instead of commas) to separate blocks
within a compound statement. Each block is parsed independently: a privilege
block has no `ON` clause; an entity block ends with `ON entityTypeSpec`.

This means that Tier 2 changes the block separator from `,` to `AND`. The
existing comma-separated multi-target entity privilege syntax (e.g.
`GRANT READ ON :A, READ ON :B TO alice`) must be rewritten using `AND`. This
is a minor syntax break, but it only affects users who already use the
multi-target form, which is rare in practice.

```cypher
// Today: five separate statements
GRANT MATCH TO alice;
GRANT CREATE TO alice;
GRANT READ ON NODES CONTAINING LABELS :Employee TO alice;
GRANT READ {name, age} ON NODES CONTAINING LABELS :Employee TO alice;
GRANT READ ON EDGES OF TYPE :WORKS_AT TO alice;

// Tier 2: single atomic statement
GRANT MATCH, CREATE
  AND READ ON NODES CONTAINING LABELS :Employee
  AND READ {name, age} ON NODES CONTAINING LABELS :Employee
  AND READ ON EDGES OF TYPE :WORKS_AT
  TO alice;

// ALL PRIVILEGES works too
GRANT ALL PRIVILEGES
  AND READ ON NODES CONTAINING LABELS :Employee
  AND READ {name} ON NODES CONTAINING LABELS :Employee
  AND READ ON EDGES OF TYPE :WORKS_AT
  TO alice;
```

---

## 4. Syntax

A compound statement is a list of **blocks**, each block being strictly one of:

- **LBAC block:** a comma-separated list of label-level permissions
  (e.g. `READ`, `CREATE`, `*`) followed by `ON entityTypeSpec`.
- **PBAC block:** a comma-separated list of property-level permissions
  (e.g. `READ {name}`, `SET PROPERTY {salary}`) followed by `ON entityTypeSpec`.
- **Privilege block** (Tier 2 only): a comma-separated list of privilege
  keywords (e.g. `MATCH, CREATE`) or `ALL PRIVILEGES`.

LBAC and PBAC items cannot be mixed within a single block. This preserves
backwards compatibility with existing syntax (see Section 7).

The entire statement targets exactly one verb (GRANT, DENY, or REVOKE) and
exactly one user or role. Blocks can appear in any order.

### 4.1 Block separators

**Tier 1** uses commas to separate blocks, matching the existing syntax for
multi-target entity privilege grants. This is fully backwards compatible.

**Tier 2** uses `AND` to separate blocks. This is necessary to disambiguate
privilege keywords from FGAC permission keywords that share the same name
(`CREATE`, `DELETE`). Each block is parsed independently: a privilege block has
no `ON` clause; an LBAC/PBAC block ends with `ON entityTypeSpec`.

### 4.2 Grammar sketch

```ebnf
lbacBlock        = granularPrivilege {',' granularPrivilege} 'ON' entityTypeSpec ;
pbacBlock        = propertyPermission {',' propertyPermission} 'ON' entityTypeSpec ;
privilegeBlock   = privilegesList | 'ALL' 'PRIVILEGES' ;

(* Tier 1: commas separate blocks *)
tier1Statement   = verb (lbacBlock | pbacBlock) {',' (lbacBlock | pbacBlock)}
                   ('TO' | 'FROM') userOrRole ;

(* Tier 2: AND separates blocks *)
block            = privilegeBlock | lbacBlock | pbacBlock ;
tier2Statement   = verb block {'AND' block} ('TO' | 'FROM') userOrRole ;
```

A block without `ON` is a privilege block; a block ending with
`ON entityTypeSpec` is an LBAC or PBAC block. A PBAC block is identified by
the presence of a property list (`{...}`) on its items.

### 4.3 Examples

**FGAC only (Tier 1)**
```cypher
// LBAC + PBAC on the same label
GRANT READ, UPDATE ON NODES CONTAINING LABELS :Employee,
  READ {name, age} ON NODES CONTAINING LABELS :Employee
  TO alice;

// Multiple entity targets
GRANT READ ON NODES CONTAINING LABELS :Employee,
  READ {name} ON NODES CONTAINING LABELS :Employee,
  READ ON EDGES OF TYPE :WORKS_AT,
  READ {weight} ON EDGES OF TYPE :WORKS_AT
  TO alice;

// Deny across different targets
DENY READ ON NODES CONTAINING LABELS :Secret,
  READ {salary} ON NODES CONTAINING LABELS :Employee
  TO alice;

// Revoke all LBAC and PBAC on :Employee
REVOKE * ON NODES CONTAINING LABELS :Employee,
  READ {*}, SET PROPERTY {*} ON NODES CONTAINING LABELS :Employee
  FROM alice;

// Multiple label targets
GRANT READ ON NODES CONTAINING LABELS :Employee,
  READ ON NODES CONTAINING LABELS :Manager
  TO alice;
```

**Privileges + FGAC (Tier 2)**
```cypher
// Global MATCH + CREATE privileges, plus LBAC and PBAC on :Employee
GRANT MATCH, CREATE
  AND READ ON NODES CONTAINING LABELS :Employee
  AND READ {name, age} ON NODES CONTAINING LABELS :Employee
  TO alice;

// ALL PRIVILEGES plus FGAC on two entity targets
GRANT ALL PRIVILEGES
  AND READ ON NODES CONTAINING LABELS :Employee
  AND READ {name} ON NODES CONTAINING LABELS :Employee
  AND READ ON EDGES OF TYPE :WORKS_AT
  TO alice;

// FGAC first, privileges after (any order)
GRANT READ ON NODES CONTAINING LABELS :Employee
  AND MATCH
  TO alice;

// Privilege-only (unchanged from existing syntax)
GRANT MATCH, CREATE TO alice;

// ALL PRIVILEGES only (unchanged from existing syntax)
GRANT ALL PRIVILEGES TO alice;

// DENY with privileges
DENY MATCH
  AND READ ON NODES CONTAINING LABELS :Secret
  TO intern_role;
```

---

## 5. Execution model

All tiers share the same execution model. A compound statement:

1. **Parses** into a single AST node carrying a list of blocks (each tagged as
   a privilege block or entity block) and a user/role target.

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

## 6. Error semantics

| Condition | Behavior |
|---|---|
| Target user/role does not exist | Statement fails; no changes. |
| Duplicate permission in a block (e.g. `READ, READ`) | Accepted; second application is idempotent. |
| Multiple privilege blocks (Tier 2, e.g. `GRANT MATCH AND CREATE TO alice`) | Accepted; semantically equivalent to `GRANT MATCH, CREATE TO alice`. |
| Same entity target in multiple blocks (e.g. two blocks for `:Employee`) | Accepted; permissions from both blocks are applied. |
| `ALL PRIVILEGES` combined with explicit privileges (Tier 2) | Accepted; the explicit privileges are redundant but harmless. `ALL PRIVILEGES` applies only to global privileges; it does not grant FGAC entity-level permissions. |
| Mix of LBAC and PBAC items in a single block (e.g. `READ, READ {x} ON ...`) | Rejected at parse time. Use separate blocks: `READ ON ... AND READ {x} ON ...`. |
| Invalid privilege/permission name | Rejected at parse time (syntax error). |
| Property permission on edges without `OF TYPE` | Rejected at parse time (grammar does not match). |
| `ALL PRIVILEGES` with no following `AND` or `TO`/`FROM` and extra tokens | Rejected at parse time. |
| `SaveUser`/`SaveRole` persistence failure | Exception propagates; the in-memory copy is discarded. The on-disk state is unchanged. |

---

## 7. Backwards compatibility

**Tier 1** is fully backwards compatible. Existing privilege and permission
statements are valid compound statements with a single block. `GRANT MATCH TO
alice`, `GRANT READ ON NODES CONTAINING LABELS :Employee TO alice`, and
`GRANT READ {name} ON NODES CONTAINING LABELS :Employee TO alice` all parse
unchanged. The existing comma-separated multi-target entity privilege syntax
(e.g. `GRANT READ ON :A, READ ON :B TO alice`) also parses unchanged, since
Tier 1 uses commas as the block separator.

**Tier 2** introduces a minor syntax break: commas between blocks are replaced
by `AND`. The existing comma-separated multi-target entity privilege syntax
must be rewritten: `GRANT READ ON :A, READ ON :B TO alice` becomes
`GRANT READ ON :A AND READ ON :B TO alice`. This only affects users who
already use the multi-target form, which is rare in practice.

All other existing statements parse unchanged, including the existing
`propertyPermissionTypeList` syntax (e.g. `GRANT READ, SET PROPERTY {name}
ON ...`), which remains valid as a single PBAC block.

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

## 8. Constraints

A compound statement targets exactly:

- **One verb.** GRANT, DENY, or REVOKE. These cannot be mixed in a single
  statement.
- **One user or role.** Granting the same permissions to multiple users requires
  separate statements.

---

## 9. Known limitations and future direction

- **Multiple user/role targets.** Supporting `TO alice, bob` could be added
  later if there is demand.
- **Mixed verbs.** A single statement uses exactly one verb. Combining
  GRANT and DENY (e.g. grant READ on `:Public` and deny READ on `:Secret`)
  requires separate statements.

---

## 10. Alternatives considered

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
