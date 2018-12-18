#!/usr/bin/env python3

import tensorflow as tf

# Load libmemgraph_op.so
memgraph_op_module = tf.load_op_library('libmemgraph_op.so')


def main():
    query = """match (u :User)-->(m :Movie)
              where u.id in $input_list
              return u.id, m.id;"""

    # Input list used in query
    input_list = [1, 2, 3, 4, 5]

    # Create tensorflow session
    with tf.Session() as sess:

        # Query placeholder
        query_holder = tf.placeholder(tf.string)

        # Input list placeholder
        input_list_holder = tf.placeholder(tf.int64)

        # Create Memgraph op, and put placeholders for input
        memgraph_op = memgraph_op_module.memgraph_op(query_holder,
                                                     input_list_holder,
                                                     output_dtype=tf.int64)

        # Run Memgraph op
        output = sess.run(memgraph_op, {query_holder: query,
                                        input_list_holder: input_list})

        # First output is list of headers
        print("Headers:")
        for i in output[0]:
            print(i)

        # Output matrix (rows), query results
        print("Rows: ")
        for i in output[1]:
            print(i)

if __name__ == "__main__":
    main()
