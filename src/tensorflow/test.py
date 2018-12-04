#!/usr/bin/env python3

import numpy as np
import tensorflow as tf
bolt_wrapper_module = tf.load_op_library('/home/dino/git/memgraph/build/src/tensorflow/libbolt_wrapper.so')

def main():
  with tf.Session() as sess:
    c = tf.placeholder(tf.string)
    d = tf.placeholder(tf.string)
    bolt_wrapper = bolt_wrapper_module.bolt_wrapper(c, d)
    output = sess.run(bolt_wrapper, {c: "match (n :Movie) return n.title as title, n.id as id;", d: ""})
    for i in output[0]:
      print(i)
    for i in output[1]:
      print(i)

if __name__ == "__main__":
  main()