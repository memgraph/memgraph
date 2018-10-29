# Tensorflow Op - Technicalities

The final result should be a shared object (".so") file that can be
dynamically loaded by the Tensorflow runtime in order to directly
access the bolt client.

## About Tensorflow

Tensorflow is usually used with Python such that the Python code is used
to define a directed acyclic computation graph. Basically no computation
is done in Python. Instead, values from Python are copied into the graph
structure as constants to be used by other Ops. The directed acyclic graph
naturally ends up with two sets of border nodes, one for inputs, one for
outputs. These are sometimes called "feeds".

Following the Python definition of the graph, during training, the entire
data processing graph/pipeline is called from Python as a single expression.
This leads to lazy evaluation since the called result has already been
defined for a while.

Tensorflow internally works with tensors, i.e. n-dimensional arrays. That
means all of its inputs need to be matrices as well as its outputs. While
it is possible to feed data directly from Python's numpy matrices straight
into Tensorflow, this is less desirable than using the Tensorflow data API
(which defines data input and processing as a Tensorflow graph) because:

  1. The data API is written in C++ and entirely avoids Python and as such
  is faster
  2. The data API, unlike Python is available in "Tensorflow serving". The
  default way to serve Tensorflow models in production.

Once the entire input pipeline is defined via the tf.data API, its input
is basically a list of node IDs the model is supposed to work with. The
model, through the data API knows how to connect to Memgraph and execute
openCypher queries in order to get the remaining data it needs.
(For example features of neighbouring nodes.)

## The Interface

I think it's best you read the official guide...  
<https://www.tensorflow.org/extend/adding_an_op>  
And especially the addition that specifies how data ops are special  
<https://www.tensorflow.org/extend/new_data_formats>  

## Compiling the TF Op

There are two options for compiling a custom op.  
One of them involves pulling the TF source, adding your code to it and
compiling via bazel.  
This is probably awkward to do for us and would
significantly slow down compilation.  

The other method involves installing Tensorflow as a Python package and
pulling the required headers from for example:  
`/usr/local/lib/python3.6/site-packages/tensorflow/include`  
We can then compile our Op with our regular build system.

This is practical since we can copy the required headers to our repo.
If necessary, we can have several versions of the headers to build several
versions of our Op for every TF version which we want to support.
(But this is unlikely to be required as the API should be stable).
