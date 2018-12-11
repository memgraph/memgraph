#!/usr/bin/env python3

import numpy as np
import tensorflow as tf
memgraph_op_module = tf.load_op_library('/home/dino/git/memgraph/build/src/tensorflow/libmemgraph_op.so')

def main():
  with tf.Session() as sess:
    c = tf.placeholder(tf.string)
    d = tf.placeholder(tf.int64)
    memgraph_op = memgraph_op_module.memgraph_op(c, d, T = tf.int64)
    output = sess.run(memgraph_op, {c: "match (u:User)-->(m:Movie) return collect(id(m)) as t", d:[]})
    for i in output[0]:
      print(i)
    for i in output[1]:
      print(i)

if __name__ == "__main__":
  main()