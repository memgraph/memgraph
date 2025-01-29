# Copyright 2025 Memgraph Ltd.
#
# Licensed as a Memgraph Enterprise file under the Memgraph Enterprise
# License (the "License"); by using this file, you agree to be bound by the terms of the License, and you may not use
# this file except in compliance with the License. You may obtain a copy of the License at https://memgraph.com/legal.

from typing import Any, List

from gensim.models import Word2Vec


class GensimWord2Vec:
    """
    gensim.Word2Vec wrapper for online representation learning

    Parameters
    ----------
    embedding_dimension : int
        Dimensions of the representation
    learning_rate : float
        Learning rate
    skip_gram: bool
        Use skip-gram model
    negative_rate: int
        Negative rate
    threads: int
        Maximum number of threads for parallelization
    """

    def __init__(
        self,
        embedding_dimension: int = 128,
        learning_rate: float = 0.01,
        skip_gram: bool = True,
        negative_rate: int = 10,
        threads: int = 4,
    ):
        self.embedding_dimension = embedding_dimension
        self.learning_rate = learning_rate
        self.skip_gram = skip_gram
        self.negative_rate = negative_rate
        self.threads = threads
        self.num_epochs = 1
        self.model = None

    def partial_fit(self, sentences: List[List[Any]]) -> None:
        if self.model is None:
            params = {
                "min_count": 1,
                "vector_size": self.embedding_dimension,
                "window": 1,
                "alpha": self.learning_rate,
                "min_alpha": self.learning_rate,
                "sg": int(self.skip_gram),
                "epochs": self.num_epochs,
                "workers": self.threads,
            }
            if self.negative_rate <= 0:
                self.negative_rate = 0
                self.model = Word2Vec(sentences, negative=self.negative_rate, hs=1, **params)  # hierarchical softmax
            else:
                self.model = Word2Vec(sentences, negative=self.negative_rate, **params)
        # update model
        self.model.build_vocab(sentences, update=True)
        self.model.train(sentences, epochs=self.num_epochs, total_examples=self.model.corpus_count)

    def get_embedding_vectors(self):
        if self.model is None:
            return {}
        vectors = self.model.wv.vectors
        indices = self.model.wv.index_to_key
        embeddings = {indices[i]: vectors[i] for i in range(len(indices))}
        return embeddings
