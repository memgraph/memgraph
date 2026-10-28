# PRD: Overridable denies (`WEAK DENY`)

Status: Draft.

Lets a `DENY` on one role be overridden by a `GRANT` of the same thing on
another, so that a user holding two overlapping roles gets the access the
broader role intends.

## Problem Statement

A user's active permissions and privileges are derived from a combination of all
the user's roles, and the user's own permissions and privileges.

Denies in our model are based on two rules:
- `DENY` always wins (over a `GRANT`)
- If a permission or privilege is not specified, this is an implicit (or "silent") `DENY`

This simple model works in the vast majority of cases, but leaves one gap: when
combining permissions or privileges, if any role has a `DENY`, that cannot be
overridden, not even if another role has an explicit `GRANT`.

For example, if the `HR` role has `GRANT READ {salary} ON NODES CONTAINING
LABELS :Employee`, whilst an `engineer` role has `DENY READ {salary} ON NODES
CONTAINING LABELS :Employee`, anyone who is both an `engineer` and `HR` will no
longer see the `salary` properties on `(:Employee)` nodes.

This can be worked around using explicit `GRANT`s only, but if the schema
features a large amount of labels or properties, this requires a proportional
amount of rules to cover all these labels and permissions. Furthermore, these
rules must be kept up to date as the schema evolves: new labels or properties
would mean additional rules to each role or user that could interact with them.

## Solution

This adds a `strength` level to a `DENY` permission or privilege. In the
first delivery, we will support two levels: `WEAK DENY` and `STRONG DENY`.

- `STRONG` (the default, and what `DENY` means today). Absolute. No `GRANT` overrides it.
- `WEAK`. The same prohibition, except a `GRANT` of the same thing from another
  role overrides it.

```cypher
// engineer reads every property except salary
GRANT     READ          ON NODES CONTAINING LABELS :Employee TO ROLE engineer;
GRANT     READ {*}      ON NODES CONTAINING LABELS :Employee TO ROLE engineer;
WEAK DENY READ {salary} ON NODES CONTAINING LABELS :Employee TO ROLE engineer;

// HR reads salary; only the explicit {salary} grant overrides the weak deny
GRANT     READ          ON NODES CONTAINING LABELS :Employee TO ROLE HR;
GRANT     READ {*}      ON NODES CONTAINING LABELS :Employee TO ROLE HR;
GRANT     READ {salary} ON NODES CONTAINING LABELS :Employee TO ROLE HR;
```

Reading `salary` on an `(:Employee)` node:

| Roles held | Result |
|---|---|
| `engineer` | `Null` |
| `HR` | visible |
| `engineer` and `HR` | visible |

The third row is the case that does not work today.

The keyword is optional and precedes `DENY`. `WEAK DENY`, `STRONG DENY`, and a
bare `DENY` are all accepted; omitting it means `STRONG`. Applies to property,
label, and edge-type denies, and to privilege denies. Denying a user a database
is not covered; see Out of Scope.

**Advice to users:** keep using `DENY`. Switch the specific permissions or
privilege to a `WEAK DENY` when two roles collide and the stronger role should win.

Memgraph does not report a collision. The symptom is a property reading `Null`,
which is indistinguishable from a property that was never set, and the
administrator finds the cause by inspecting the user's roles with
`SHOW PRIVILEGES FOR <user>`. Nothing has to be predicted in advance, but
nothing arrives unprompted either.

## User Stories

1. As an administrator, I want a deny on one role to be overridable by a grant of the same thing on another, so that a user holding both roles gets the broader access.
2. As an administrator, I want existing `DENY` statements to keep their current meaning after upgrade, so that nothing I have already configured changes.
3. As an administrator, I want `WEAK` to be something I opt into per statement, so that prohibitions stay absolute unless I say otherwise.
4. As an administrator, I want to fix an overlap by weakening the deny that causes it, so that I do not have to enumerate properties or restructure roles.
5. As an administrator, I want `WEAK DENY` to work with a `{*}` grant in the same role, so that I can express "everything except X" without listing the rest.
6. As an administrator, I want a weak deny on a specific property to hold against a wildcard grant, from any role, so that only a grant naming what was denied can override it.
7. As an administrator, I want a strong deny to win over a grant of the same thing, whatever else weak-denies it, so that a genuine prohibition cannot be relaxed by a second role.
8. As an administrator, I want the result to be the same no matter what order a user's roles were assigned in, so that access is reproducible.
9. As an auditor, I want `SHOW PRIVILEGES` to distinguish weak denies from strong ones, so that I do not read a weak deny as a protection it is not.
10. As an auditor, I want two roles denying the same label, edge type, or property at different strengths to appear as separate entries, so that the weaker one is visible.
11. As an administrator, I want `REVOKE` to remove a deny whatever its strength, so that there is nothing new to learn about removing permissions.
12. As an administrator, I want weak denies available on labels, edge types, and privileges as well as properties, so that the same overlap problem has the same answer everywhere.
13. As an administrator, I want a grant of the same thing that I give a user directly to override a weak deny on one of their roles, so that I can make an exception for one person without editing a shared role.
14. As an administrator, I want a role scoped to one database to only override weak denies on that database, so that scoping a role still limits what it can do.

## Implementation Decisions

### An override needs a grant of the same thing

A weak deny is overridden only by a grant of exactly what it denies: the same
privilege; the same label or edge-type rule (same symbols and matching mode); or
the same property under the same rule. A grant on `*` overrides a weak deny on
`*`, and nothing else.

This is how a user's permissions are already combined. The user's own
permissions and those of every role they hold on the database are merged into
one set, keyed by exactly these targets, and today a deny at a key beats a grant
at the same key. A weak deny changes only that comparison: at the same key, a
grant beats a weak deny, and a strong deny beats both.

One role cannot hold a grant and a deny at the same key, because granting
clears the deny and denying clears the grant. So a grant beside a weak deny
always comes from another role or from the user, and the origin of a
permission never has to be tracked.

Everything else is unchanged. A deny on one rule still wins over a grant on
another rule that also matches, and a wildcard is still consulted only when
nothing more specific decides.

### User permissions count as a separate source

A user can hold permissions directly as well as through roles. A grant given to
a user directly overrides a weak deny of the same thing on any of their roles,
the same as a grant from another role would, so an administrator can make an
exception for one person without editing a shared role.

A weak deny given to a user directly is overridden by a grant of the same thing
from any of that user's roles. The user level is another source, not a
privileged one.

One exception: on a database the user reaches only through a role, the user's
own privileges such as `CREATE` are left out, while their own label, edge-type,
and property permissions are kept. So a user grant there does not override a
role's weak deny of a privilege, although it does for the others. This
difference predates weak denies and is tracked as a separate issue.

### An override can differ between databases

A role can be scoped to particular databases, and only the roles a user holds on
the database being queried take part in deciding access there. So a weak deny
can be overridden on one database and hold on another, for the same user with
the same roles.

Take a user holding both of these:

- `engineer`, on `db1` and `db2`, weak-denying `salary`.
- `HR`, on `db1` only, granting `salary`.

The user reads `salary` on `db1`, where `HR` is one of their roles and its grant
overrides the weak deny. On `db2` they get `Null`, because `HR` is not a role
they hold there and nothing overrides the deny.

This follows from how roles are already scoped: a role's grants and denies
apply only on the databases it covers. An administrator diagnosing an unexpected `Null` should check which databases the
overriding role covers, not just that the role exists.

### Across roles, the strongest deny wins

A user's roles are combined by a fold whose order is not defined. Strengths
therefore combine by taking the strongest: if any role says `STRONG DENY`, the deny
is strong. Anything order-dependent, such as "last one wins", would make the
same configuration return different answers on different runs.

Within a single role, re-issuing a deny overwrites its strength, as re-issuing a
grant or deny already overwrites today. `WEAK DENY` then `STRONG DENY` on one
role leaves `STRONG`; the reverse leaves `WEAK`. The overwrite is silent, which
is why `SHOW PRIVILEGES` showing the strength matters.

A property wildcard does not overwrite named properties. Granting, denying, or
revoking `{*}` on a role changes only the role's `{*}` entry, so
`STRONG DENY {salary}` then `WEAK DENY {*}` leaves `salary` strongly denied,
`GRANT {salary}` then `GRANT {*}` keeps the named grant that overrides another
role's `WEAK DENY {salary}`, and `REVOKE {*}` leaves named properties in place.

These rules look like one question and are not. All belong in the documentation.

### Wildcards are not specific

A `{*}` grant does not override a weak deny on `{salary}`, whether it is on the
same role or another. In the same role this is what makes "everything except
`salary`" work: the role grants `{*}` and weak-denies `{salary}`. On another
role it means the overriding role must name what it grants, so the override is
visible in that role's own statements.

Wildcards work differently for properties than for labels and edge types. For
properties, `{*}` is an entry alongside the named ones, and the named entry is
consulted first. For labels and edge types, `*` is not a rule at all; it is
held separately and consulted only when no specific rule matches. System
privileges have no wildcard: `ALL PRIVILEGES` sets every bit individually, so
every privilege deny is already specific.

### No `WEAK GRANT`

There is no weak grant, and there is no coherent meaning for one. A `GRANT` is
already the weaker half of the model: it loses to every `DENY`. Weakening it
further would need grants to lose to other grants, and grants do not compete,
they combine.

The asymmetry is the point. Strength exists to say how firmly a prohibition is
held, because prohibitions are what block each other. Permissions simply add up.

### `SHOW PRIVILEGES` shows the strength

`SHOW PRIVILEGES` returns three columns: `privilege`, `effective`, and
`description`. The strength goes in `effective`, which today reads `GRANT` or
`DENY` and gains `STRONG DENY` (replacing `DENY`) and `WEAK DENY`. No column is added, so nothing
parsing the result by position breaks.

Putting it in `effective` is required, not cosmetic. Rows sharing a privilege
and an `effective` value are merged into one before display, so a strength held
anywhere else would collapse a weak deny and a strong deny on the same property
into a single row and hide the weaker one. That is the case an auditor most
needs to see.

For labels, edge types, and properties, a grant, a strong deny, and a weak deny
on the same thing each get their own row, even when another one wins.
These rows report what the user's roles carry, not which one won, so revoking a
strong deny does not bring into play a weak deny the auditor did not see.
Privileges such as `CREATE` keep one row showing the result, as before.

### Compatibility

Existing denies load as `STRONG`, so enforcement is unchanged on upgrade and
nothing already configured has to be revisited.

One visible change: `SHOW PRIVILEGES` reports an existing deny as `STRONG DENY`
where it used to say `DENY`. The column count and their meanings are unchanged,
so this only affects something matching the `effective` value as an exact
string. Accepted: leaving it as `DENY` would force an auditor to ask which
strength it is, which is the doubt the column exists to remove.

Weak denies are stored in a new optional field beside the existing grant and
deny records. The existing deny record keeps its meaning and holds strong
denies, so an auth store written by an earlier version has no weak field and
loads with every deny strong. A deny present in both records counts as strong,
the stronger of the two, so every combination has a meaning. The stored entity
version, `kCurrentEntityVersion`, goes from 4 to 5. No migration step is needed:
a version 4 record has no weak fields, so it loads with every deny strong.

During a rolling upgrade, a replica still on an earlier version can receive a
version 5 user or role from an upgraded main. From 3.14.0, the replica rejects
it and system replication to that replica fails until it is upgraded. Earlier
versions accept it and ignore the weak field, so a weak deny is not enforced
on that replica. When upgrading from 3.13 or earlier, don't issue `WEAK DENY`
until every instance is upgraded.

## Testing Decisions

`tests/e2e/fine_grained_access/property_fga_tests.py` holds the existing
fine-grained access tests, including `test_role_merge_deny_wins`, which asserts
that a deny on one role beats a grant on another. That test states today's
behaviour for a plain `DENY` and must keep passing unchanged: it is the
compatibility guarantee. It gains a `WEAK DENY` sibling asserting the opposite
outcome.

Cases that need covering, each of which distinguishes this feature from doing
nothing:

- The worked example above, all three rows of its outcome table.
- A weak deny on `{salary}` and a `{*}` grant on the same role: denied. A `{*}`
  grant on another role: denied. A `{salary}` grant on another role: allowed.
- A weak deny on `*` overridden by a `*` grant on another role.
- A strong deny on any role beating a weak deny and a grant of the same thing on
  others.
- A `{salary}` grant under a different label rule not overriding the weak deny.
- A user-level grant overriding a weak deny on one of that user's roles.
- The same set of roles assigned in a different order, giving the same answer.
- A weak deny surviving a round trip through the auth store, and an auth store
  written before this feature loading with every deny strong.
- `SHOW PRIVILEGES` showing a weak deny and a strong deny on the same property
  as separate rows.
- A weak deny overridden on one database and holding on another, where the
  overriding role is scoped to only one of them.

## Out of Scope

- **Any strength beyond `WEAK` and `STRONG`.** Two levels ship. The encoding leaves
  room for more; see Future direction.
- **`WEAK GRANT`.** Not deferred, but incoherent. See above.
- **Making any deny weak by default.** `STRONG` stays the default, so an overridable
  deny always takes a deliberate act.
- **Weak denies of database access.** `DENY DATABASE` stays absolute. Database
  access is a pair of name sets, and granting every database behaves like any
  other wildcard, so nothing structural prevents it; it is left out because
  nobody has asked for it. Adding it later is additive and changes nothing
  specified here.
- **Weak denies of impersonation targets.** `DENY IMPERSONATE_USER alice TO ...`,
  which names the users who may not be impersonated, takes no strength and stays
  absolute, for the same reason. The `IMPERSONATE_USER` privilege itself, written
  without targets, is a privilege like any other and can be weak-denied:
  `WEAK DENY IMPERSONATE_USER TO ROLE support`.
- **Detecting conflicting roles.** Nothing warns that two roles disagree, either when
  the roles are assigned or when a query hits the disagreement. That is a separate
  feature which would compose with this one. The consequence for users is described
  under Advice above, and it is unchanged from today.

## Known limitations and future direction

### Limitations

- **The administrator decides, in advance, which prohibition may be overridden.** This gives a mechanism, not an answer. Two orthogonal roles that each deny what the other grants are only resolved if the right one was marked `WEAK`.

### Future direction

Strength is a level on a scale rather than a yes-or-no flag, and the scale has
room in it. Two extensions are possible later without disturbing what is already
stored:

- **Further named levels**, such as `MEDIUM DENY`, inserted between the existing
  two.
- **Numeric strengths**, exposing the levels as numbers and letting an
  administrator rank denies directly, with `WEAK` and `STRONG` as aliases.

Neither is planned, and neither should be added without first answering what a
level means. A level is only useful once it says *who* may override it; adding
one without that rule leaves administrators choosing between keywords with no
basis for choosing. Numeric strengths carry a further cost: rankings that stay
meaningful as the role set grows are hard to choose and harder to revisit, which
is why role priority numbers were rejected for this feature.
