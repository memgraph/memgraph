#!/usr/bin/env python3

import numpy as np
import tensorflow as tf
memgraph_op_module = tf.load_op_library('/home/dino/git/memgraph/build/src/tensorflow/libmemgraph_op.so')

def main():

  query = "match (u:User)-->(m) where u.id in $input_list return u.id, m.id;"
  input_list = [1, 2, 3, 4, 5]

  with tf.Session() as sess:
    query_holder = tf.placeholder(tf.string)
    input_list_holder = tf.placeholder(tf.int64)
    
    memgraph_op = memgraph_op_module.memgraph_op(query_holder, \
      input_list_holder, \
      output_dtype = tf.int64)
    
    output = sess.run(memgraph_op, {query_holder: query, \
      input_list_holder: input_list})
    
    for i in output[0]:
      print(i)
    for i in output[1]:
      print(i)

if __name__ == "__main__":
  main()
