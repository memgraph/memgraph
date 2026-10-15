# String Functions Operate On Code Points ADR

**Author**
Gareth Andrew Lloyd (github.com/Ignition)

**Status**
PROPOSED

**Date**
August 15, 2026

**Problem**

Every Cypher string function that measures or cuts a string has to agree on
what one "character" is. Memgraph stores strings as UTF-8 and, until now, used
the byte as that unit: `size` returned the buffer length, and `substring`,
`left` and `right` cut at byte offsets. Two things follow. Results depend on
how the string happens to be stored rather than on what it says, so the same
text measures differently under a different encoding. And a cut can land inside
a character and return a fragment of one, which is not valid UTF-8 at all, so
the engine can hand a client bytes it cannot decode.

There are four candidate units, and the choice is a language-semantics decision
rather than an implementation detail: bytes, UTF-16 code units, Unicode code
points, and extended grapheme clusters (what a reader would call a character,
defined by UAX #29).

**Criteria**

- *Independence from how a string is stored* (highest weight). A length that
  changes when the storage encoding changes is not a property of the value, and
  these results are returned to clients, compared, and used in predicates.
- *Stability over time* (high). The same bytes must measure the same after an
  upgrade. These values are stored in properties, used in `WHERE`, and can sit
  behind an index, so a length that shifts underneath is a correctness problem
  rather than an inconvenience.
- *Agreement with the reference implementation* (medium). Divergence here is
  silent: both engines return a number, and only the numbers differ.
- *Cost* (low, but real). Anything needing Unicode tables is a dependency
  decision, not a patch.

**Decision**

String functions operate on **Unicode code points**.

Bytes and UTF-16 code units both fail the first criterion, being properties of
the encoding rather than of the text. It is worth noting that the reference
implementation runs on a platform whose natural string unit is the UTF-16 code
unit and deliberately does not use it: its manual says `size()` of a character
is 1 "even if the character does not fit in the 16 bits of one char". So this
is not an artefact of that platform, and matching it costs us nothing that we
would not have chosen anyway.

Grapheme clusters are the honest answer to "how many characters does a reader
see", and we are not choosing them. Their definition lives in Unicode data
files that change between versions: new joining behaviour makes sequences that
counted as several clusters count as one. A stored property's length, and a
predicate over it, would then change meaning on an upgrade with no write to the
database. They also need the tables, which means an ICU-class dependency that
Memgraph does not currently carry. Code points, by contrast, are fixed forever
for a given sequence of bytes.

The cost of this decision is real and should be stated plainly. A combining
mark counts on its own, so `e` followed by an acute accent has length two. A
cut can still separate a mark from its base character, or split an emoji built
from joined code points, even though it can no longer split a character's
bytes. The reference implementation behaves identically in each of these cases,
which was verified rather than assumed, and our UTF-8 implementation was
checked to give the same answers as its UTF-16 one on the inputs that
distinguish the four units.

Should grapheme-aware behaviour be wanted later, it should arrive as separate
functions. Redefining these would be a second breaking change, and would make
results depend on the Unicode version the server was built against.
