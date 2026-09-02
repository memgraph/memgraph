# Storage Reports Its Own Errors ADR

**Author**
Gareth Andrew Lloyd (github.com/Ignition)

**Status**
PROPOSED

**Date**
September 2, 2026

**Problem**

The graph engine detects conditions it has to report: an index that does not
exist, a vector whose dimension does not match the index it is being added to,
a write the active mode forbids, a type constraint that a property violates.
Until now it reported several of these by throwing exception types declared in
the query layer, so the lower layer named the upper one in its own sources, and
its index headers pulled the query layer's exception header into every
translation unit that used them.

Two things made that more than untidy. The layer that reports an error decides
how the error is classified: these types derived from the query layer's
exception, and a session translates that into a client error, meaning "do not
retry". Anything deriving from the plain base exception is instead presented as
transient, meaning "retry". So the choice of base class was silently deciding
driver behaviour. And the engine's error mode is part of its interface, so
saying that its failures are query failures constrains any other consumer of
the engine.

**Criteria**

- *The layer that detects a condition declares it* (highest weight). Otherwise
  the dependency runs the wrong way and the classification is decided by
  inheritance rather than by the code that translates it.
- *Observability must not shift silently* (high). The counters keyed on
  exception names, and the error class a driver sees, are things operators and
  clients already depend on. A layering change is not a reason to move them.
- *Cost of conversion* (medium). Around eighty sites throw or catch these.
- *Fit with how the engine already reports failures* (medium). Accessor
  operations already return a result type carrying an error enum rather than
  throwing.

**Decision**

**Storage declares and raises the conditions it detects. New storage-detected
errors are returned monadically rather than thrown.**

The types the engine was borrowing now live beside it, over the base exception,
sharing a base that states what they have in common: the sender can correct the
condition, and repeating the request will not help. A session maps that base to
a client error in one visible place, so the classification is a line of code
rather than a consequence of which base was inherited. Each type reports the
same name as before, so the counters are unchanged.

The existing sites were converted as throws rather than to a return type. The
engine's result type carries a flat enum with no message, while these carry
formatted text naming the index or the dimension, so converting them would have
meant introducing a richer error type and changing the signature of everything
along the search path at the same time as moving the declarations. Doing both at
once would have put an observable behaviour change inside a mechanical move.

New errors the engine detects should be returned, not thrown. The result type is
already the engine's idiom for accessor failures, a return states the failure in
the signature where a caller cannot miss it, and it needs no translation step to
be classified correctly. This is deliberately a rule about new code: it shrinks
the exception boundary over time without a single large conversion whose
blast radius is every caller of vector and text search.

Two borrowed uses remain and are not covered here: a type constraint violation
still raises the query layer's general exception, and snapshot durability
includes the query layer's syntax tree. Both are worth their own change, and the
second is not an error-reporting question at all.
