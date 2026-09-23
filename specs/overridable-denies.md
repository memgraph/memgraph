# PRD: Overridable denies (`WEAK DENY`)

Status: Draft.

Lets a `DENY` on one role be overridden by a `GRANT` on another, so that a user
holding two overlapping roles gets the access the broader role intends.

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
- `WEAK`. The same prohibition, except a `GRANT` from another role overrides it.

```cypher
// engineer reads every property except salary
GRANT     READ          ON NODES CONTAINING LABELS :Employee TO ROLE engineer;
GRANT     READ {*}      ON NODES CONTAINING LABELS :Employee TO ROLE engineer;
WEAK DENY READ {salary} ON NODES CONTAINING LABELS :Employee TO ROLE engineer;

// HR reads salary, and says so explicitly
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
label, and edge-type denies, and to privilege denies.

**Advice to users:** keep using `DENY`. Switch the specific permissions or
privilege to a `WEAK DENY` when two roles collide and the stronger role should win.

Memgraph does not report a collision. The symptom is a property reading `Null`,
which is indistinguishable from a property that was never set, and the
administrator finds the cause by inspecting the user's roles with
`SHOW PRIVILEGES FOR <user>`. Nothing has to be predicted in advance, but
nothing arrives unprompted either.

## User Stories

1. As an administrator, I want a deny on one role to be overridable by a grant on another, so that a user holding both roles gets the broader access.
2. As an administrator, I want existing `DENY` statements to keep their current meaning after upgrade, so that nothing I have already configured changes.
3. As an administrator, I want `WEAK` to be something I opt into per statement, so that prohibitions stay absolute unless I say otherwise.
4. As an administrator, I want to fix an overlap by weakening the deny that causes it, so that I do not have to enumerate properties or restructure roles.
5. As an administrator, I want `WEAK DENY` to work with a `{*}` grant, so that I can express "everything except X" without listing the rest.
6. As an administrator, I want a weak deny on a specific property to still hold against a wildcard grant in the same role, so that the common case does what it reads like.
7. As an administrator, I want a strong deny on any role to win over a weak deny on another, so that a genuine prohibition cannot be relaxed by a second role.
8. As an administrator, I want the result to be the same no matter what order a user's roles were assigned in, so that access is reproducible.
9. As an auditor, I want `SHOW PRIVILEGES` to distinguish weak denies from strong ones, so that I do not read a weak deny as a protection it is not.
10. As an auditor, I want two roles denying the same thing at different strengths to appear as separate entries, so that the weaker one is visible.
11. As an administrator, I want `REVOKE` to remove a deny whatever its strength, so that there is nothing new to learn about removing permissions.
12. As an administrator, I want weak denies available on labels, edge types, and privileges as well as properties, so that the same overlap problem has the same answer everywhere.
13. As an administrator, I want a grant I give a user directly to override a weak deny on one of their roles, so that I can make an exception for one person without editing a shared role.

## Implementation Decisions

### Where a permission comes from is part of the answer

Today a user's permissions are flattened into one set before anything is
checked, because deny always wins and it makes no difference which role a deny
came from. Weak denies break that: the same grant overrides a weak deny when it
comes from another role and does not when it comes from the same one, so the
origin of each permission has to survive into the check.

This is the substantial part of the work, and it is larger than adding a
keyword. It is not visible to users, but it is why this feature is not a small
change.

### User permissions count as a separate source

A user can hold permissions directly as well as through roles. A grant given to
a user directly overrides a weak deny on any of their roles, the same as a grant
from another role would, so an administrator can make an exception for one
person without editing a shared role.

A weak deny given to a user directly is overridden by a grant from any of that
user's roles. The user level is another source, not a privileged one.

### Across roles, the strongest deny wins

A user's roles are combined by a fold whose order is not defined. Strengths
therefore combine by taking the strongest: if any role says `STRONG DENY`, the deny
is strong. Anything order-dependent, such as "last one wins", would make the
same configuration return different answers on different runs.

Within a single role, re-issuing a deny overwrites its strength, as re-issuing a
grant or deny already overwrites today. `WEAK DENY` then `STRONG DENY` on one
role leaves `STRONG`; the reverse leaves `WEAK`. The overwrite is silent, which
is why `SHOW PRIVILEGES` showing the strength matters.

These two rules look like one question and are not. Both belong in the documentation.

### Specificity ranks within a role; across roles, any grant wins

Two rules, and the split between them is what makes the feature work.

**Within the role that holds the weak deny**, the more specific statement wins. A
role that grants `{*}` and weak-denies `{salary}` denies `salary`: the explicit
property is more specific than the wildcard. Without this a weak deny would be
self-cancelling, because the wildcard grant it is meant to carve a hole in sits
in the same role.

**Across roles**, specificity does not apply. Any grant from another role
overrides a weak deny, wildcard or not. Without this the feature does not solve
the problem it exists for: the overriding role usually grants `{*}`, and ranking
that below the weak deny would leave the user denied.

These two produce opposite answers for statements that look identical, which is
deliberate. `GRANT READ {*}` beside a weak deny on the same role does not
override it; the same statement on a different role does. Origin decides, not
wording.

The worked example above grants `{salary}` explicitly on `HR` even though `{*}`
would be enough, because a reader should be able to see the override without
knowing this rule.

Wildcards work differently for properties than for labels and edge types, and
the rules above hold for both. For properties, `{*}` is an entry alongside the
named ones, and the named entry is consulted first. For labels and edge types,
`*` is not a rule at all; it is held separately and consulted only when no
specific rule matches. Both give "specific beats wildcard" within a role.
System privileges have no wildcard: `ALL PRIVILEGES` sets every bit
individually, so every privilege deny is already specific.

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

Rows stay per role, so a weak deny and the grant that overrides it both appear,
each against the role that holds it. `SHOW PRIVILEGES` reports what each role
carries, not which one won.

### Changing a strength warns

Re-issuing a deny at a different strength changes it silently, and the statement
that does it looks almost identical to the one it replaces. Turning a strong
deny into a weak one is a security-relevant change made in passing, so it
returns a warning saying what changed. The statement still succeeds.

### Compatibility

Existing denies load as `STRONG`, so enforcement is unchanged on upgrade and
nothing already configured has to be revisited.

One visible change: `SHOW PRIVILEGES` reports an existing deny as `STRONG DENY`
where it used to say `DENY`. The column count and their meanings are unchanged,
so this only affects something matching the `effective` value as an exact
string. Accepted: leaving it as `DENY` would force an auditor to ask which
strength it is, which is the doubt the column exists to remove.

Strength is stored as a new optional field beside the existing grant and deny
records, rather than by changing them, so an auth store written by an older
version loads unchanged. The stored format and its versions are documented in
`auth-json-versions.md`, which this feature extends rather than revises.

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
- A weak deny and a `{*}` grant on the same role: denied. The same grant on
  another role: allowed.
- A strong deny on any role beating a weak deny and a grant on others.
- A user-level grant overriding a weak deny on one of that user's roles.
- The same set of roles assigned in a different order, giving the same answer.
- A weak deny surviving a round trip through the auth store, and an auth store
  written before this feature loading with every deny strong.
- `SHOW PRIVILEGES` showing a weak deny and a strong deny on the same property
  as separate rows.

## Out of Scope

- **Any strength beyond `WEAK` and `STRONG`.** Two levels ship. The encoding leaves
  room for more; see Future direction.
- **`WEAK GRANT`.** Not deferred, but incoherent. See above.
- **Making any deny weak by default.** `STRONG` stays the default, so an overridable
  deny always takes a deliberate act.
- **Detecting conflicting roles.** Nothing warns that two roles disagree, either when
  the roles are assigned or when a query hits the disagreement. That is a separate
  feature which would compose with this one. The consequence for users is described
  under Advice above, and it is unchanged from today.

## Known limitations and future direction

### Limitations

- **The administrator decides, in advance, which prohibition may be overridden.** This gives a mechanism, not an answer. Two orthogonal roles that each deny what the other grants are only resolved if the right one was marked `WEAK`.
- **`WEAK DENY` on a privilege such as `AUTH` is legitimate and rarely what you want.** It is not restricted, and the documentation carries a worked warning. Reaching for `WEAK` reflexively on privileges that gate user management is how an overridable deny becomes an incident.
- **Denying a user a database is not covered.** `DENY DATABASE` is a plain yes-or-no per database with nothing to hold a strength, so it stays absolute. Only property, label, edge-type, and privilege denies take a strength.
- **No prior art.** Cerbos reaches the same outcome by making grant-beat-deny the default across roles, which is a breaking change; Apache Ranger's "exclude from deny" is a carve-out written inside the deny itself. This shape is new, which is both the differentiator and the risk.

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
