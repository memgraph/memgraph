import unittest
import tensorflow as tf
import numpy as np

class MemgrapOp:

  def __init__(self, path_to_lib):
    self.__memgraph_op_module = tf.load_op_library(path_to_lib)
  
  def runQuery(self, query, type, input_list=[]):
    with tf.Session() as sess:
      query_holder = tf.placeholder(tf.string)
      input_list_holder = tf.placeholder(tf.int64)
      memgraph_op = self.__memgraph_op_module.memgraph_op(query_holder, input_list_holder, output_dtype=type)
      return sess.run(memgraph_op, {query_holder: query, input_list_holder: input_list})

  def runQueryAdvance(self, query, input_list = [], **kargs):
    with tf.Session() as sess:
      query_holder = tf.placeholder(tf.string)
      input_list_holder = tf.placeholder(tf.int64)
      memgraph_op = self.__memgraph_op_module.memgraph_op(query_holder, input_list_holder, **kargs)
      return sess.run(memgraph_op, {query_holder: query, input_list_holder: input_list})



class MemgraphOpTest(unittest.TestCase):

  def assertRowEqual(self, expected, result):
    np.testing.assert_array_equal(expected, result)

  def setUp(self):
    self.memgraph_op = MemgrapOp('../../../build/src/tensorflow/libmemgraph_op.so')

  def test_output_int64(self):
    header, rows = self.memgraph_op.runQuery("MATCH (n :User) RETURN n.id AS id ORDER BY id LIMIT 5;", tf.int64)
    self.assertEqual(header, b'id')
    self.assertRowEqual(rows, [[1],[2],[3],[4],[5]])

  def test_output_double(self):
    header, rows = self.memgraph_op.runQuery("MATCH (n :User)-[r :Rating]-(:Movie) WHERE r.score < 5 RETURN r.score AS score ORDER BY score DESC LIMIT 5;", tf.double)
    self.assertEqual(header, b'score')
    self.assertRowEqual(rows, [[4.5],[4.5],[4.5],[4.5],[4.5]])

  def test_output_bool(self):
    header, rows = self.memgraph_op.runQuery("MATCH (n :User) RETURN n.name = 'Albert' AS eq ORDER BY n.name LIMIT 5", tf.bool)
    self.assertEqual(header, b'eq')
    self.assertRowEqual(rows, [[False], [True], [True], [False], [False]])

  def test_output_string(self):
    header, rows = self.memgraph_op.runQuery("MATCH (n :User) RETURN n.name AS name ORDER BY name DESC LIMIT 5", tf.string)
    self.assertEqual(header, b'name')
    self.assertRowEqual(rows, [[b'Winston'], [b'Winny'], [b'Winnifred'], [b'Winnie'], [b'Winifred']])

  def test_input_list(self):
    header, rows = self.memgraph_op.runQuery("MATCH (n :User) WHERE n.id IN [25,35,45,55,65] RETURN n.name AS name ORDER BY name ASC", tf.string)
    self.assertEqual(header, b'name')
    self.assertRowEqual(rows, [[b'Benny'],[b'Bernard'],[b'Beverly'],[b'Gilbert'],[b'Wilhelmina']])

  def test_matrix_out(self):
    header, rows = self.memgraph_op.runQuery("MATCH (n :User)-->(m :Movie) RETURN n.id AS NID, m.id AS MID ORDER BY NID, MID LIMIT 10", tf.int64)
    self.assertRowEqual(header, [b'NID', b'MID'])
    self.assertRowEqual(rows,[[2,62],[2,110],[2,223],[2,261],[2,319],[2,527],[3,110],[3,527],[4,173],[4,260]])

  def test_connection_host(self):
    runner = lambda : self.memgraph_op.runQueryAdvance("RETURN 1;", output_dtype=tf.int64, host = "10.0.0.1", port = 1234)
    with self.assertRaises(Exception) as context:
      runner()
    print(str(context.exception))
    self.assertTrue('Cannot connect to memgraph' in str(context.exception))

  def test_connection_port(self):
    runner = lambda : self.memgraph_op.runQueryAdvance("RETURN 1;", output_dtype=tf.int64, port = 8888)
    with self.assertRaises(Exception) as context:
            runner()
    self.assertTrue('Cannot connect to memgraph' in str(context.exception))

  def test_connection_ssl(self):
    runner = lambda : self.memgraph_op.runQueryAdvance("RETURN 1;", output_dtype=tf.int64, use_ssl = True)
    with self.assertRaises(Exception) as context:
            runner()
    print(str(context.exception))
    self.assertTrue('Cannot connect to memgraph' in str(context.exception))

  def test_wrong_query(self):
    runner = lambda : self.memgraph_op.runQueryAdvance("SELECT * FROM table", output_dtype=tf.int64)
    with self.assertRaises(Exception) as context:
            runner()
    self.assertTrue('Query error' in str(context.exception))

  def test_wrong_list_size(self):
    runner = lambda : self.memgraph_op.runQueryAdvance("MATCH (n :User)-->(m :Movie) RETURN n.id, collect(m.id);", output_dtype=tf.int64)
    with self.assertRaises(Exception) as context:
            runner()
    self.assertTrue('List has wrong size' in str(context.exception))

  def test_different_types(self):
    runner = lambda : self.memgraph_op.runQueryAdvance("MATCH (n :User)-->(m :Movie) RETURN n.id, m.title;", output_dtype=tf.int64)
    with self.assertRaises(Exception) as context:
            runner()
    self.assertTrue('Wrong type' in str(context.exception))

  def test_different_types_string(self):
    header, rows = self.memgraph_op.runQueryAdvance("MATCH (n :User)-->(m :Movie) RETURN n.id AS NID, m.title AS TITLE ORDER BY NID, TITLE LIMIT 2;", output_dtype=tf.string)
    self.assertRowEqual(header, [b'NID', b'TITLE'])
    self.assertRowEqual(rows, [[b'2', b'2001: A Space Odyssey'], [b'2', b'Cat on a Hot Tin Roof']])