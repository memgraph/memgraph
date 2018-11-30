# High Availability (abbr. HA)

## Introduction

High availability is a characteristic of a system which aims to ensure a
certain level of operational performance for a higher-than-normal period.
Although there are multiple ways to design highly available systems, Memgraph
strives to achieve HA by elimination of single points of failure. In essence,
this implies adding redundancy to the system so that a failure of a component
does not imply the failure of the entire system.

## Theoretical Background

The following chapter serves as an introduction into some theoretical aspects
of Memgraph's high availability implementation. If the reader is solely
interested in design decisions around HA implementation, they can skip this
chapter.

An important implication of any HA implementation stems from Eric Brewer's
[CAP Theorem](https://fenix.tecnico.ulisboa.pt/downloadFile/1126518382178117/10.e-CAP-3.pdf)
which states that it is impossible for a distributed system to simultaneously
achieve:

  * Consistency (C - every read receives the most recent write or an error)
  * Availability (A - every request receives a response that is not an error)
  * Partition tolerance (P - The system continues to operate despite an
                             arbitrary number of messages being dropped by the
                             network between nodes)

In the context of HA, Memgraph should strive to achieve CA.

### Consensus

Implications of the CAP theorem naturally lead us towards introducing a
cluster of machines which will have identical internal states. When a designated
machine for handling client requests fails, it can simply be replaced with
another.

Well... turns out this is not as easy as it sounds :(

Keeping around a cluster of machines with consistent internal state is an
inherently difficult problem. More precisely, this problem is as hard as
getting a cluster of machines to agree on a single value, which is a highly
researched area in distributed systems. Our research of state of the art
consensus algorithms lead us to Diego Ongaro's
[Raft algorithm](https://raft.github.io/raft.pdf).

#### Raft

As you might have guessed, analyzing each subtle detail of Raft goes way
beyond the scope of this document. In the remainder of the chapter we will
outline only the most important ideas and implications, leaving all further
analysis to the reader. Detailed explanation can be found either in Diego's
[dissertation](https://ramcloud.stanford.edu/~ongaro/thesis.pdf) \[1\] or the
Raft [paper](https://raft.github.io/raft.pdf) \[2\].

In essence, Raft allows us to implement the previously mentioned idea of
managing a cluster of machines with identical internal states. In other
words, the Raft protocol allows us to manage a cluster of replicated
state machines which is fully functional as long as the *majority* of
the machines in the cluster operate correctly.

Another important fact is that those state machines must be *deterministic*.
In other words, the same command on two different machines with the same
internal state must yield the same result. This is important because Memgraph,
as a black box, is not entirely deterministic. Non-determinism can easily be
introduced by the user (e.g. by using the `rand` function) or by algorithms
behind query execution (e.g. introducing fuzzy logic in the planner could yield
a different order of results). Luckily, once we enter the storage level,
everything should be fully deterministic.

To summarize, Raft is a protocol which achieves consensus in a cluster of
deterministic state machines via log replication. The cluster is fully
functional if the majority of the machines work correctly. The reader
is strongly encouraged to gain a deeper understanding (at least read through
the paper) of Raft before reading the rest of this document.

## Integration with Memgraph

The first thing that should be defined is a single instruction within the
context of Raft (i.e. a single entry in a replicated log). As mentioned
before, these instructions should be completely deterministic when applied
to the state machine. We have therefore decided that the appropriate level
of abstraction within Memgraph corresponds to `StateDelta`-s (data structures
which describe a single change to the Memgraph state, used for durability
in WAL). Moreover, a single instruction in a replicated log will consist of a
batch of `StateDelta`s which correspond to a single **committed** transaction.
This decision both improves performance and handles some special cases that
present themselves otherwise by leveraging the knowledge that the transaction
should be committed.

"What happens with aborted transactions?"

A great question, they are handled solely by the leader which is the only
machine that communicates with the client. Aborted transactions do not alter
the state of the database and there is no need to replicate it to other machines
in the cluster. If, for instance, the leader dies before returning the result
of some read operation in an aborted transaction, the client will notice that
the leader has crashed. A new leader will be elected in the next term and the
client should retry the transaction.

"OK, that makes sense! But, wait a minute, this is broken by design! Merely
generating `StateDelta`s on the leader for any transaction will taint its
internal storage before sending the first RPC to some follower. This deviates
from Raft and will crash the universe!"

Another great observation. It is indeed true that applying `StateDelta`s makes
changes to local storage, but only a single type of `StateDelta` makes that
  change durable. That `StateDelta` type is called `TRANSACTION_COMMIT` and we
will change its behaviour when working as a HA instance. More precisely, we
must not allow the transaction engine to modify the commit log saying that
the transaction has been committed. That action should be delayed until those
`StateDelta`s have been applied to the majority of the cluster. At that point
the commit log can be safely modified leaving it up to Raft to ensure the
durability of the transaction.

We should also address one subtle detail that arises in this case. Consider
the following scenario:

  * The leader starts working on a transaction which creates a new record in the
    database. Suppose that record is stored in the leader's internal storage
    but the transaction was not committed (i.e. no such entry in the commit log).
  * The leader should start replicating those `StateDelta`s to its followers
    but, suddenly, it's cut off from the rest of the cluster.
  * Due to timeout, a new election is held and a new leader has been elected.
  * Our old leader comes back to life and becomes a follower.
  * The new leader receives a transaction which creates that same record, but
    this transaction is successfully replicated and committed by the new leader.

The problem lies in the fact that there is still a record within the internal
storage of our old leader with the same transaction ID and GID as the recently
committed record by the new leader. Obviously, this is broken. As a solution, on
each transition from `Leader` to `Follower`, we will reinitialize storage, reset
the transaction engine and recover data from the Raft log. This will ensure all
ongoing transactions which have "polluted" the storage will be gone.

"When will followers append that transaction to their commit logs?"

When the leader deduces that the transaction is safe to commit, it will include
the relevant information in all further heartbeats which will alert the
followers that it is safe to commit those entries from their raft logs.
Naturally, the followers need not to delay appending data to the commit log
as they know that the transaction has already been committed (from the clusters
point of view). If this sounds really messed up, seriously, read the Raft paper.

"How does the raft log differ from WAL"

Conceptually, it doesn't. When operating in HA, we don't really need the
recovery mechanisms implemented in Memgraph thus far. When a dead machine
comes back to life, it will eventually come in sync with the rest of the
cluster and everything will be done using the machine's raft log as well
as the messages received from the cluster leader.

"Those logs will become huge, isn't that recovery going to be painfully slow?"

True, but there are mechanisms for making raft logs more compact. The most
popular method is, wait for it, making snapshots :)
Although the process of bringing an old machine back to life is a long one,
it doesn't really affect the performance of the cluster in a great degree.
The cluster will work perfectly fine with that machine being way out of sync.

"I don't know, everything seems to be a lot slower than before!"

Absolutely true, the user should be aware that they will suffer dire
consequences on the performance side if they choose to be highly available.
As Frankie says, "That's life!".

"Also, I didn't really care about most of the things you've said. I'm
not a part of the storage team and couldn't care less about the issues you
face, how does HA affect 'my part of the codebase'?"

Answer for query execution: That's ok, you'll be able to use the same beloved
API (when we implement it, he he :) towards storage and continue to
make fun of us when you find a bug.

Answer for infrastructure: We'll talk. Some changes will surely need to
be made on the Memgraph client. There is a chapter in Diego's dissertation
called 'Client interaction', but we'll cross that bridge when we get there.
There will also be the whole 'integration with Jepsen tests' thing going on.

Answer for analytics: I'm astonished you've read this article. Wanna join
storage?

### Subtlety Regarding Reads

As we have hinted in the previous chapter, we would like to bypass log
replication for operations which do not alter the internal state of Memgraph.
Those operations should therefore be handled only by the leader, which is not
as trivial as it seems. The subtlety arises from the fact that a (newly-elected)
leader can have an entry in its log which was committed by the previous leader
that has crashed but that entry is not yet committed in its internal storage
by the current leader. Moreover, the rule about safely committing logs that are
replicated on the majority of the cluster only applies for entries replicated in
the leaders current term. Therefore, we are faced with two issues:

  * We cannot simply perform read operations if the leader has a non-committed
    entry in its log (breaks consistency).
  * Replicating those entries onto the majority of the cluster is not enough
    to guarantee that they can be safely committed.

This can be solved by introducing a blank no-op operation which the new leader
will try to replicate at the start of its term. Once that operation is
replicated and committed, the leader can safely perform those non-altering
operations on its own.

For further information about these issues, you should check out section
5.4.2 from the raft paper \[1\] which hints as to why its not safe to commit
entries from previous terms. Also, you should check out section 6.4 from
the thesis \[2\] which goes into more details around efficiently processing
read-only queries.

## How do we test HA

[Check this out](https://jepsen.io/analyses/dgraph-1-0-2)
