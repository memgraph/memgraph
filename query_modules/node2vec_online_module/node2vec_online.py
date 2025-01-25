# Copyright 2025 Memgraph Ltd.
#
# Licensed as a Memgraph Enterprise file under the Memgraph Enterprise
# License (the "License"); by using this file, you agree to be bound by the terms of the License, and you may not use
# this file except in compliance with the License. You may obtain a copy of the License at https://memgraph.com/legal.

import multiprocessing
import time
from inspect import cleandoc
from itertools import chain, repeat

import mgp
from node2vec_online_utils.w2v_learners import GensimWord2Vec
from node2vec_online_utils.walk_sampling import StreamWalkUpdater


class Node2VecContext:
    def __init__(self):
        self._updater = None
        self._learner = None
        self._start_time = None

    @property
    def updater(self):
        return self._updater

    @property
    def learner(self):
        return self._learner

    @property
    def start_time(self):
        return self._start_time

    @updater.setter
    def updater(self, updater):
        self._updater = updater

    @learner.setter
    def learner(self, learner):
        self._learner = learner

    @start_time.setter
    def start_time(self, start_time):
        self._start_time = start_time

    def is_initialized(self):
        return self._learner is not None and self._updater is not None

    def update_model(self, source: int, target: int, current_time: int):
        sampled_pairs = self.updater.process_new_edge(source, target, current_time)
        self.learner.partial_fit(sampled_pairs)


node2vec_context = Node2VecContext()


@mgp.read_proc
def set_streamwalk_updater(
    ctx: mgp.ProcCtx,
    half_life: int = 7200,
    max_length: int = 3,
    beta: mgp.Number = 0.9,
    cutoff: int = 604800,
    sampled_walks: int = 4,
    full_walks: bool = False,
) -> mgp.Record(message=str):
    """
    This function sets updater to StreamWalk.

    If parameters are already set, function doesn't reset parameters.

    :param half_life: Half-life in seconds for time decay
    :param max_length: Maximum length of the sampled temporal random walks
    :param beta: Damping factor for long paths
    :param cutoff: Temporal cutoff in seconds to exclude very distant past
    :param sampled_walks:  Number of sampled walks for each edge update
    :param full_walks: Return every node of the sampled walk for representation learning (full_walks=True) or
    only the endpoints of the walk (full_walks=False)

    :return: empty record

    Example call (give all information):
       CALL node2vec_online.set_stream_walk_updater(7200, 3, 0.9, 604800, 4, False) YIELD *;

    Example call (with predefined parameters):
       CALL node2vec_online.set_stream_walk_updater() YIELD *;
    """
    global node2vec_context

    if node2vec_context.is_initialized():
        return mgp.Record(
            message="StreamWalk updater is already initialized."
            "Call: `CALL node2vec_online.reset() YIELD *:` in order to set parameters again. "
            "Warning: all embeddings will be lost."
        )

    node2vec_context.updater = StreamWalkUpdater(
        half_life=half_life,
        max_length=max_length,
        beta=beta,
        cutoff=cutoff,
        sampled_walks=sampled_walks,
        full_walks=full_walks,
    )
    return mgp.Record(message="Parameters for StreamWalk updater are set.")


@mgp.read_proc
def set_word2vec_learner(
    ctx: mgp.ProcCtx,
    embedding_dimension: int = 128,
    learning_rate: mgp.Number = 0.01,
    skip_gram: bool = True,
    negative_rate: mgp.Number = 10,
    threads: mgp.Nullable[int] = None,
) -> mgp.Record(message=str):
    """
     This function needs to be called to set parameters.

     If parameters are already set, function doesn't reset parameters.

     :param embedding_dimension number of dimensions of the representation of embedding vector
     :param learning_rate learning rate
     :param skip_gram use skip-gram model
     :param negative_rate negative rate for skip-gram model
     :param threads maximum number of threads for parallelization
     :return: empty record


    Example call (give all information):
        CALL node2vec_online.set_gensim_word2vec_learner(128, 0.01, True, 10, 1) YIELD *;

    Example call (with predefined parameters):
        CALL node2vec_online.set_gensim_word2vec_learner() YIELD *;
    """

    if not mgp.is_enterprise_valid():
        raise mgp.AbortError("To use node2vec online module you need a valid enterprise license.")

    global node2vec_context

    if node2vec_context.is_initialized():
        return mgp.Record(
            message="Word2Vec learner is already initialized. "
            "Call: `CALL node2vec_online.reset() YIELD *;`in order to set parameters again. "
            "Warning: all embeddings will be lost."
        )

    if threads is None:
        threads = multiprocessing.cpu_count()

    node2vec_context.learner = GensimWord2Vec(
        embedding_dimension=embedding_dimension,
        learning_rate=learning_rate,
        skip_gram=skip_gram,
        negative_rate=negative_rate,
        threads=threads,
    )
    return mgp.Record(message="Parameters for Word2Vec learner are set.")


@mgp.read_proc
def reset() -> mgp.Record(message=str):
    """
    This function resets parameters and embeddings already learned.
    """

    if not mgp.is_enterprise_valid():
        raise mgp.AbortError("To use node2vec online module you need a valid enterprise license.")

    global node2vec_context

    node2vec_context.start_time = None
    node2vec_context.updater = None
    node2vec_context.learner = None
    return mgp.Record(message="Updater and learner are ready to be set again.")


@mgp.read_proc
def get(
    ctx: mgp.ProcCtx,
) -> mgp.Record(node=mgp.Vertex, embedding=mgp.List[mgp.Number]):
    """
    Returns current node embeddings as list of mgp.Vertex and embedding which is vector

    Example call (give all information):
        CALL graph_analyzer.analyze() YIELD *;

    Example call (with parameter):
        CALL graph_analyzer.analyze(['nodes', 'edges']) YIELD *;
    """

    if not mgp.is_enterprise_valid():
        raise mgp.AbortError("To use node2vec online module you need a valid enterprise license.")

    if not node2vec_context.is_initialized():
        raise mgp.AbortError(
            "Learner or updater are not initialized. Initialize them by calling:"
            "`CALL node2vec_online.set_word2vec_learner() YIELD *;`"
            "`CALL node2vec_online.set_streamwalk_updater() YIELD *;`"
        )

    embedding_vectors = node2vec_context.learner.get_embedding_vectors()

    embeddings_dict = {}

    for node_id, embedding in embedding_vectors.items():
        embeddings_dict[node_id] = [float(e) for e in embedding]

    return [
        mgp.Record(node=ctx.graph.get_vertex_by_id(node_id), embedding=embedding)
        for node_id, embedding in embeddings_dict.items()
    ]


@mgp.read_proc
def update(ctx: mgp.ProcCtx, edges: mgp.List[mgp.Edge]) -> mgp.Record():
    """
    For this function to work, Learner and Updater must be set.

    This function updates embeddings of nodes.

    Example call (give all information):
        MATCH (n)-[e]->(m)
        WITH COLLECT(e) as edges_list
        CALL node2vec_online.update(edges_list) RETURN edges_list
        LIMIT 5;

    Other possibility is to set TRIGGER call:
        CALL node2vec_online.update(['nodes', 'edges']) YIELD *;
    """

    if not mgp.is_enterprise_valid():
        raise mgp.AbortError("To use node2vec online module you need a valid enterprise license.")

    global node2vec_context

    current_time = time.time()

    if node2vec_context.start_time is None:
        node2vec_context.start_time = current_time

    if not node2vec_context.is_initialized():
        raise mgp.AbortError(
            "Learner or updater are not initialized. Initialize them by calling:"
            "`CALL node2vec_online.set_word2vec_learner() YIELD *;`"
            "`CALL node2vec_online.set_streamwalk_updater() YIELD *;` "
        )

    for e in edges:
        ctx.check_must_abort()
        node2vec_context.update_model(e.from_vertex.id, e.to_vertex.id, int(current_time))
    return mgp.Record()


@mgp.read_proc
def help() -> mgp.Record(name=str, value=str):
    """Shows manual page for graph_analyzer."""

    if not mgp.is_enterprise_valid():
        raise mgp.AbortError("To use node2vec online module you need a valid enterprise license.")

    records = []

    def make_records(name, doc):
        return (mgp.Record(name=n, value=v) for n, v in zip(chain([name], repeat("")), cleandoc(doc).splitlines()))

    for func in (help, set_streamwalk_updater, set_word2vec_learner, get, update):
        records.extend(make_records("Procedure '{}'".format(func.__name__), func.__doc__))

    return records
