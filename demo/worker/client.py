# -*- coding: utf-8 -*-

from os import path
from json import loads
from subprocess import check_output
from benchmark import benchmark


def subprocess_client(args):
    '''
    Runs the client in a separate process.

    :param args: list of strings, process arguments (the first element is
                 path to the exe)
    '''
    result = check_output(args)
    return loads(result.decode('utf-8'))


def wrapped_client(*args):
    '''
    Runs the client via the python module which was built using cython.

    :param args: [str], list of ctypes objects
                 the interface is specified inside the benchmark.pyx
                 Mapping:
                    b'string' -> string
                    [b'string',...] -> vector<string>
    '''
    result = benchmark(*args)
    return loads(result.decode('utf-8'))


def main_subprocess():
    '''
    Example of subprocess call.
    '''
    exe = path.join(path.dirname(path.abspath(__file__)), "benchmark_json.out")
    args = ["localhost", "7474", "16", "1",
            "CREATE (n{id:@}) RETURN n", "CREATE (n{id:@}) RETURN n"]
    result = subprocess_client([exe] + args)
    return result


def main_wrapped():
    '''
    Example of cython call.
    '''
    args = [b"localhost", b"7474", 16, 1,
            [b"CREATE (n{id:@}) RETURN n", b"CREATE (n{id:@}) RETURN n"]]
    result = wrapped_client(*args)
    return result


def main():
    '''
    '''
    print("Subprocess:")
    print(main_subprocess())
    print("Wrapped:")
    print(main_wrapped())

if __name__ == '__main__':
    main()
