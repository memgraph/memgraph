# -*- coding: utf-8 -*-

from libcpp.string cimport string
from libcpp.vector cimport vector

cdef extern from "benchmark.hpp" nogil:
    string benchmark_json(const string& host,
                          const string& port,
                          int connections_per_query,
                          double duration,
                          const vector[string]& queries)


def benchmark(host, port, connections_per_query, duration, queries):
    '''
    '''
    result = benchmark_json(<const string&> host, <const string&> port,
                            <int> connections_per_query, <double> duration,
                            <const vector[string]&> queries)
    return result
