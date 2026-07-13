## Parent

`specs/coordinator-sso-auth/PRD.md`

## What to build

Remove the unused `COORDINATOR` privilege from the auth privilege model. The privilege is
defined and wired end-to-end (enum bit, grammar, visitor, AST, query→permission mapping,
required-privileges attachment for `CoordinatorQuery`) but never gates any real operation,
because coordinator queries only run on coordinators and coordinators do not enforce
privileges.

Delete the privilege from every layer while keeping the `COORDINATOR` **lexer keyword**,
which is still used by other coordinator syntax (`ADD COORDINATOR`, `SHOW COORDINATOR
SETTINGS`, etc.). Only the privilege-rule occurrence and the permission plumbing go.

No migration is performed: a previously-persisted grant simply stops being reported and
stops being checked. The freed enum bit is left reserved and not reused.

## Acceptance criteria

- [ ] `COORDINATOR` is removed from the `Permission` enum, from `kPermissionsAll`, and from permission stringification.
- [ ] `COORDINATOR` is removed from the grammar privilege rule, the visitor mapping, the AST privilege list, the query→permission mapping, and the required-privileges attachment for `CoordinatorQuery`.
- [ ] The `COORDINATOR` lexer keyword remains and all coordinator syntax that uses it still parses.
- [ ] No migration code is added; the freed bit is documented as reserved / not reused.
- [ ] The project builds and existing auth/privilege tests pass.

## Blocked by

None - can start immediately
