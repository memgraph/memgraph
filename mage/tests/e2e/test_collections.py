import pytest
from gqlalchemy import Memgraph


def test_flatten_e2e():
    """End-to-end test for collections_module.flatten function."""
    memgraph = Memgraph()

    # Test simple nested list
    result = memgraph.execute_and_fetch(
        """
        RETURN collections_module.flatten([[1, 2], [3, 4], 5]) as result
        """
    )
    assert next(result)["result"] == [1, 2, 3, 4, 5]

    # Test deeply nested list
    result = memgraph.execute_and_fetch(
        """
        RETURN collections_module.flatten([1, [2, [3, [4, 5]], 6], 7]) as result
        """
    )
    assert next(result)["result"] == [1, 2, 3, 4, 5, 6, 7]

    # Test list with nulls
    result = memgraph.execute_and_fetch(
        """
        WITH [1] as first_part
        WITH first_part + COLLECT(null) + [[2, null, 3]] + COLLECT(null) + [4] as input_list
        RETURN collections_module.flatten(input_list) as result
        """
    )
    assert next(result)["result"] == [1, 2, 3, 4]

    # Test empty lists
    result = memgraph.execute_and_fetch(
        """
        WITH [] as empty_list
        WITH COLLECT(empty_list) + [empty_list] + [[empty_list]] as input_list
        RETURN collections_module.flatten(input_list) as result
        """
    )
    assert next(result)["result"] == []

    # Test mixed types
    result = memgraph.execute_and_fetch(
        """
        WITH [1] as nums,
             ["text", 2.5] as mixed,
             [true, false] as bools
        WITH COLLECT(nums) + [mixed] + [bools] + COLLECT(null) as input_list
        RETURN collections_module.flatten(input_list) as result
        """
    )
    assert next(result)["result"] == [1, "text", 2.5, True, False]

    # Test with graph data
    memgraph.execute(
        """
        CREATE (m1:Movie {title: "The Matrix", year: 1999}),
               (m2:Movie {title: "The Matrix Reloaded", year: 2003}),
               (m3:Movie {title: "The Matrix Revolutions", year: 2003})
        """
    )

    result = memgraph.execute_and_fetch(
        """
        MATCH (m:Movie)
        WITH collect(m) as all_movies
        WITH [all_movies[0], [all_movies[1], all_movies[2]]] as nested_movies
        RETURN collections_module.flatten(nested_movies) as flattened_movies
        """
    )
    flattened_movies = next(result)["flattened_movies"]
    assert len(flattened_movies) == 3
    assert all(movie.get("title").startswith("The Matrix") for movie in flattened_movies)

    # Cleanup
    memgraph.execute("MATCH (n) DETACH DELETE n")
