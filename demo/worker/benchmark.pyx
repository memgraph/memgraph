# -*- coding: utf-8 -*-

from libcpp.string cimport string
from libcpp.vector cimport vector

cdef extern from "benchmark.hpp":
    string benchmark_json(const string& host,
                          const string& port,
                          int connections_per_query,
                          double duration,
                          const vector[string]& queries)


def benchmark(host, port, connections_per_query, duration, queries):
    '''
    '''
    return benchmark_json(<const string&> host, <const string&> port,
                          <int> connections_per_query, <double> duration,
                          <const vector[string]&> queries)
