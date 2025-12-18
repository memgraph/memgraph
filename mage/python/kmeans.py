"""
Purpose of this query module is to offer easy kmeans clustering algorithm on top of the embeddings that you
might have stored in nodes. All you need to do is call kmeans.get_clusters(5, "embedding") where 5
represents number of clusters you want to get, and "embedding" represents node property name in which
embedding of node is stored
"""
from typing import List, Tuple

import mgp
from sklearn.cluster import KMeans


def get_created_clusters(
    number_of_clusters: int,
    embeddings: List[List[float]],
    nodes: List[mgp.Vertex],
    init: str,
    n_init: int,
    max_iter: int,
    tol: float,
    algorithm: str,
    random_state: int,
) -> List[Tuple[mgp.Vertex, int]]:
    kmeans = KMeans(
        n_clusters=number_of_clusters,
        init=init,
        n_init=n_init,
        max_iter=max_iter,
        tol=tol,
        algorithm=algorithm,
        random_state=random_state,
    ).fit(embeddings)
    return [(nodes[i], label) for i, label in enumerate(kmeans.labels_)]


def extract_nodes_embeddings(ctx: mgp.ProcCtx, embedding_property: str) -> Tuple[List[mgp.Vertex], List[List[float]]]:
    nodes = []
    embeddings = []
    for node in ctx.graph.vertices:
        nodes.append(node)
        embeddings.append(node.properties.get(embedding_property))
    return nodes, embeddings


@mgp.read_proc
def get_clusters(
    ctx: mgp.ProcCtx,
    n_clusters: mgp.Number,
    embedding_property: str = "embedding",
    init: str = "k-means++",
    n_init: mgp.Number = 10,
    max_iter: mgp.Number = 10,
    tol: mgp.Number = 1e-4,
    algorithm: str = "lloyd",
    random_state: int = 1998,
) -> mgp.Record(node=mgp.Vertex, cluster_id=mgp.Number):
    nodes, embeddings = extract_nodes_embeddings(ctx, embedding_property)

    nodes_labels_list = get_created_clusters(
        number_of_clusters=n_clusters,
        embeddings=embeddings,
        nodes=nodes,
        init=init,
        n_init=n_init,
        max_iter=max_iter,
        tol=tol,
        algorithm=algorithm,
        random_state=random_state,
    )
    return [mgp.Record(node=node, cluster_id=int(label)) for node, label in nodes_labels_list]


@mgp.write_proc
def set_clusters(
    ctx: mgp.ProcCtx,
    n_clusters: mgp.Number,
    embedding_property: str = "embedding",
    cluster_property="cluster_id",
    init: str = "k-means++",
    n_init: mgp.Number = 10,
    max_iter: mgp.Number = 10,
    tol: mgp.Number = 1e-4,
    algorithm: str = "lloyd",
    random_state=1998,
) -> mgp.Record(node=mgp.Vertex, cluster_id=mgp.Number):
    nodes, embeddings = extract_nodes_embeddings(ctx, embedding_property)

    nodes_labels_list = get_created_clusters(
        number_of_clusters=n_clusters,
        embeddings=embeddings,
        nodes=nodes,
        init=init,
        n_init=n_init,
        max_iter=max_iter,
        tol=tol,
        algorithm=algorithm,
        random_state=random_state,
    )

    for vertex, label in nodes_labels_list:
        vertex.properties.set(cluster_property, int(label))

    return [mgp.Record(node=node, cluster_id=int(label)) for node, label in nodes_labels_list]
