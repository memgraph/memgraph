#!/usr/bin/python3

import os
import struct

# Import the target transform script.

import transform


# Constants used for communicating with the memgraph process.

COMMUNICATION_TO_PYTHON_FD = 1000
COMMUNICATION_FROM_PYTHON_FD = 1002


# Functions used to get data from the memgraph process.

def get_data(num_bytes):
    data = bytes()
    while len(data) < num_bytes:
        data += os.read(COMMUNICATION_TO_PYTHON_FD, num_bytes - len(data))
    return data


def get_size():
    fmt = "I"  # uint32_t
    data = get_data(struct.calcsize(fmt))
    return struct.unpack(fmt, data)[0]


def get_batch():
    batch = []
    count = get_size()
    for i in range(count):
        size = get_size()
        batch.append(get_data(size))
    return batch


# Functions used to put data to the memgraph process.

TYPE_NONE = 0x10
TYPE_BOOL_FALSE = 0x20
TYPE_BOOL_TRUE = 0x21
TYPE_INT = 0x30
TYPE_FLOAT = 0x40
TYPE_STR = 0x50
TYPE_LIST = 0x60
TYPE_DICT = 0x70


def put_data(data):
    written = 0
    while written < len(data):
        written += os.write(COMMUNICATION_FROM_PYTHON_FD, data[written:])


def put_size(size):
    fmt = "I"  # uint32_t
    put_data(struct.pack(fmt, size))


def put_type(typ):
    fmt = "B"  # uint8_t
    put_data(struct.pack(fmt, typ))


def put_string(value):
    data = value.encode("utf-8")
    put_size(len(data))
    put_data(data)


def put_value(value, ids):
    if value is None:
        put_type(TYPE_NONE)
    elif type(value) is bool:
        if value:
            put_type(TYPE_BOOL_TRUE)
        else:
            put_type(TYPE_BOOL_FALSE)
    elif type(value) is int:
        put_type(TYPE_INT)
        put_data(struct.pack("q", value))  # int64_t
    elif type(value) is float:
        put_type(TYPE_FLOAT)
        put_data(struct.pack("d", value))  # double
    elif type(value) is str:
        put_type(TYPE_STR)
        put_string(value)
    elif type(value) is list:
        if id(value) in ids:
            raise ValueError("Recursive objects are not supported!")
        ids_new = ids + [id(value)]
        put_type(TYPE_LIST)
        put_size(len(value))
        for item in value:
            put_value(item, ids_new)
    elif type(value) is dict:
        if id(value) in ids:
            raise ValueError("Recursive objects are not supported!")
        ids_new = ids + [id(value)]
        put_type(TYPE_DICT)
        put_size(len(value))
        for key, item in value.items():
            if type(key) is not str:
                raise TypeError("Dictionary keys must be strings!")
            put_string(key)
            put_value(item, ids_new)
    else:
        raise TypeError("Unsupported value type {}!".format(str(type(value))))


# Functions used to continuously process data.

def put_params(params):
    if type(params) != dict:
        raise TypeError("Parameters must be a dict!")
    put_value(params, [])


class StreamError(Exception):
    pass


def process_batch():
    # Get the data that should be transformed.
    batch = get_batch()

    # Transform the data.
    ret = transform.stream(batch)

    # Sanity checks for the transformed data.
    if type(ret) != list:
        raise StreamError("The transformed items must be a list!")
    for item in ret:
        if type(item) not in [list, tuple]:
            raise StreamError("The transformed item must be a tuple "
                              "or a list!")
        if len(item) != 2:
            raise StreamError("There must be exactly two elements in the "
                              "transformed item!")
        if type(item[0]) != str:
            raise StreamError("The first transformed element of an item "
                              "must be a string!")
        if type(item[1]) != dict:
            raise StreamError("The second transformed element of an item "
                              "must be a dictionary!")

    # Send the items to the server.
    put_size(len(ret))
    for query, params in ret:
        put_string(query)
        put_params(params)


# Main entry point.

if __name__ == "__main__":
    while True:
        process_batch()
