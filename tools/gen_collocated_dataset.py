#!/usr/bin/python3
# "large": {"vertices": 1632803, "edges": 30622564},

import argparse
import random

import numpy as np


# sum has to be greater than zero
def generate_numbers_with_specific_sum(count, min_val, max_val, target_sum):
    # Step 1: Generate initial random numbers
    # random_numbers = [random.uniform(min_val, max_val) for _ in range(count)]
    # random_numbers = np.random.poisson(lam=target_sum/count, size=count)
    random_numbers = np.random.gamma(shape=target_sum / count, scale=count / target_sum, size=count)
    random_numbers = np.clip(random_numbers, min_val, max_val)

    # Step 2: Calculate the current sum of the generated numbers
    current_sum = sum(random_numbers)

    # Step 3: Scale the numbers to adjust their sum to the target_sum
    scale_factor = target_sum / current_sum
    scaled_numbers = [int(round(max(min(num * scale_factor, max_val), min_val))) for num in random_numbers]

    return scaled_numbers


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate collocated dataset with n vertices and m edges.")

    parser.add_argument("v", type=int, help="Number of vertices")
    parser.add_argument("e", type=int, help="Number of edges")
    parser.add_argument("f", type=str, help="Output file")
    args = parser.parse_args()

    # max number of edges 10k
    rand_edge_n = generate_numbers_with_specific_sum(2 * args.v, 0, 10000, args.e + args.v)
    # print(rand_edge_n)

    with open(args.f, "w") as file:
        # header
        file.write(f"CREATE INDEX ON :User (id);\n")
        file.write(f"CREATE INDEX ON :User;\n")
        # vertices
        for i in range(args.v):
            file.write(f"CREATE (:User {{id: {i}}});\n")
        # edges
        out = False
        id = 0
        for edges in rand_edge_n:
            # get the edges from near the vertex
            edge_min = int(max(id - edges / 2, 0))
            edge_max = int(min(edge_min + edges, args.v))
            edges_range = range(edge_min, edge_max)
            if out:
                for edge in edges_range:
                    if edge == id:
                        continue
                    file.write(f"MATCH (n:User {{id: {id}}}), (m:User {{id: {edge}}}) CREATE (n)-[e: Friend]->(m);\n")
                id = id + 1  # bump after the in and out edges were done
            else:
                for edge in edges_range:
                    if edge == id:
                        continue
                    file.write(f"MATCH (n:User {{id: {edge}}}), (m:User {{id: {id}}}) CREATE (n)-[e: Friend]->(m);\n")
            out = not out
        # footer
        file.write(f"DROP INDEX ON :User (id);\n")
        file.write(f"DROP INDEX ON :User;\n")
        file.write(f"STORAGE MODE ON_DISK_TRANSACTIONAL;\n")
