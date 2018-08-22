#!/usr/bin/env python3
# -*- coding: utf-8 -*-

'''
Kafka benchmark dataset generator.
'''


import random
import sys
from argparse import ArgumentParser


def get_edge():
    from_node = random.randint(0, args.nodes)
    to_node = random.randint(0, args.nodes)

    while from_node == to_node:
        to_node = random.randint(0, args.nodes)
    return (from_node, to_node)


def parse_args():
    argp = ArgumentParser(description=__doc__)
    argp.add_argument("--nodes", type=int, default=100, help="Number of nodes.")
    argp.add_argument("--edges", type=int, default=30, help="Number of edges.")
    return argp.parse_args()


args = parse_args()
edges = set()

for i in range(args.nodes):
    print("%d\n" % i)

for i in range(args.edges):
    edge = get_edge()
    while edge in edges:
        edge = get_edge()

    edges.add(edge)
    print("%d %d\n" % edge)

sys.exit(0)
