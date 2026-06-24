# Feature Specifications

This directory holds **product-level feature specifications**: what a feature does,
who it is for, the behavior users can rely on, and the product decisions (and their
rationale) behind it.

Specs answer *what* and *why for the user*. They are intentionally not engineering
design docs:

- **Specs (`specs/`)** — product behavior, user-facing surface, decisions and their
  motivation, guarantees, and limitations. Audience: product, support, docs, and any
  engineer who needs to know the intended behavior.
- **ADRs (`ADRs/`)** — architecture decision records: each captures a single significant,
  hard-to-reverse architectural decision — the problem, the criteria it was weighed against,
  the choice made, and its consequences. Short and to the point (about a page), focused on the
  decision and its trade-offs, not on implementation detail or diagrams. ADRs are
  complementary to any verbose tech spec written while designing the solution. See
  [`ADRs/README.md`](../ADRs/README.md) for the convention and template.

A feature may have both: a spec describing the contract with the user, and one or more
ADRs recording the architectural decisions behind it.

## Conventions

- One feature per file, named `kebab-case.md`.
- Start with a status line (Draft / Experimental / GA / Superseded) and a one-line summary.
- Describe behavior in terms a user or operator can observe, not internal classes.
- Be honest about limitations and out-of-scope items — a spec that hides the edges is
  worse than no spec.
- When behavior changes, amend the spec and note what changed and why, rather than
  silently rewriting history.
