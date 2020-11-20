# Tensorflow Op - Technicalities

The final result should be a shared object (".so") file that can be dynamically
loaded by the Tensorflow runtime in order to directly access the bolt client.

## About Tensorflow

Tensorflow is usually used with Python such that the Python code is used to
define a directed acyclic computation graph. Basically no computation is done
in Python. Instead, values from Python are copied into the graph structure as
constants to be used by other Ops. The directed acyclic graph naturally ends up
with two sets of border nodes, one for inputs, one for outputs. These are
sometimes called "feeds".

Following the Python definition of the graph, during training, the entire data
processing graph/pipeline is called from Python as a single expression.  This
leads to lazy evaluation since the called result has already been defined for a
while.

Tensorflow internally works with tensors, i.e. n-dimensional arrays. That means
all of its inputs need to be matrices as well as its outputs. While it is
possible to feed data directly from Python's numpy matrices straight into
Tensorflow, this is less desirable than using the Tensorflow data API (which
defines data input and processing as a Tensorflow graph) because:

  1. The data API is written in C++ and entirely avoids Python and as such is
     faster
  2. The data API, unlike Python is available in "Tensorflow serving". The
     default way to serve Tensorflow models in production.

Once the entire input pipeline is defined via the tf.data API, its input is
basically a list of node IDs the model is supposed to work with. The model,
through the data API knows how to connect to Memgraph and execute openCypher
queries in order to get the remaining data it needs.  (For example features of
neighbouring nodes.)

## The Interface

I think it's best you read the official guide...
<https://www.tensorflow.org/extend/adding_an_op> And especially the addition
that specifies how data ops are special
<https://www.tensorflow.org/extend/new_data_formats>

## Compiling the TF Op

There are two options for compiling a custom op.  One of them involves pulling
the TF source, adding your code to it and compiling via bazel.  This is
probably awkward to do for us and would significantly slow down compilation.

The other method involves installing Tensorflow as a Python package and pulling
the required headers from for example:
`/usr/local/lib/python3.6/site-packages/tensorflow/include` We can then compile
our Op with our regular build system.

This is practical since we can copy the required headers to our repo.  If
necessary, we can have several versions of the headers to build several
versions of our Op for every TF version which we want to support.  (But this is
unlikely to be required as the API should be stable).

## Example for Using the Bolt Client Tensorflow Op

### Dynamic Loading

``` python3
import tensorflow as tf

mg_ops = tf.load_op_library('/usr/bin/memgraph/tensorflow_ops.so')
```

### Basic Usage

``` python3
dataset = mg_ops.OpenCypherDataset(
            # This is probably unfortunate as the username and password
            # get hardcoded into the graph, but for the simple case it's fine
            "hostname:7687", auth=("user", "pass"),

            # Our query
            '''
            MATCH (n:Train) RETURN n.id, n.features
            ''',

            # Cast return values to these types
            (tf.string, tf.float32))

# Some Tensorflow data api boilerplate
iterator = dataset.make_one_shot_iterator()
next_element = iterator.get_next()

# Up to now we have only defined our computation graph which basically
# just connects to Memgraph
# `next_element` is not really data but a handle to a node in the Tensorflow
# graph, which we can and do evaluate
# It is a Tensorflow tensor with shape=(None, 2)
# and dtype=(tf.string, tf.float)
# shape `None` means the shape of the tensor is unknown at definition time
# and is dynamic and will only be known once the tensor has been evaluated

with tf.Session() as sess:
    node_ids = sess.run(next_element)
    # `node_ids` contains IDs and features of all the nodes
    # in the graph with the label "Train"
    # It is a numpy.ndarray with a shape ($n_matching_nodes, 2)
```

### Memgraph Client as a Generic Tensorflow Op

Other than the Tensorflow Data Op, we'll want to support a generic Tensorflow
Op which can be put anywhere in the Tensorflow computation Graph. It takes in
an arbitrary tensor and produces a tensor. This would be used in the GraphSage
algorithm to fetch the lowest level features into Tensorflow

```python3
requested_ids = np.array([1, 2, 3])
ids_placeholder = tf.placeholder(tf.int32)

model = mg_ops.OpenCypher()
    "hostname:7687", auth=("user", "pass"),
    """
    UNWIND $node_ids as nid
    MATCH (n:Train {id: nid})
    RETURN n.features
    """,

    # What to call the input tensor as an openCypher parameter
    parameter_name="node_ids",

    # Type of our resulting tensor
    dtype=(tf.float32)
)

features = model(ids_placeholder)

with tf.Session() as sess:
    result = sess.run(features,
                    feed_dict={ids_placeholder: requested_ids})
```

This is probably easier to implement than the Data Op, so it might be a good
idea to start with.

### Production Usage

During training, in the GraphSage algorithm at least, Memgraph is at the
beginning and at the end of the Tensorflow computation graph.  At the
beginning, the Data Op provides the node IDs which are fed into the generic
Tensorflow Op to find their neighbours and their neighbours and their features.

Production usage differs in that we don't use the Data Op. The Data Op is
effectively cut off and the initial input is fed by Tensorflow serving, with
the data found in the request.

For example a JSON request to classify a node might look like:

`POST http://host:port/v1/models/GraphSage/versions/v1:classify`

With the contents:

```json
{
    "examples": [
        {"node_id": 1},
        {"node_id": 2}
    ],
}
```

Every element of the "examples" list is an example to be computed. Each is
represented by a dict with keys matching names of feeds in the Tensorflow graph
and values being the values we want fed in for each example.

The REST API then replies in kind with the classification result in JSON.

Note about adding our custom Op to Tensorflow serving.  Our Ops .so can be
added into the Bazel build to link with Tensorflow serving or it can be
dynamically loaded by starting Tensorflow serving with a flag
`--custom_op_paths`.

### Considerations

There might be issues here that the url to connect to Memgraph is hardcoded
into the op and would thus be wrong when moved to production, requiring some
type of a hack to make work. We probably want to solve this by having the
client op take in another tf.Variable as an input which would contain a
connection url and username/password.  We have to research whether this makes
it easy enough to move to production, as the connection string variable is
still a part of the graph, but maybe easier to replace.

It is probably the best idea to utilize openCypher parameters to make our
queries flexible. The exact API as to how to declare the parameters in Python
is open to discussion.

The Data Op might not even be necessary to implement as it is not key for
production use. It can be replaced in training mode with feed dicts and either

 1. Getting the initial list of nodes via a Python Bolt client
 2. Creating a separate Tensorflow computation graph that gets all the relevant
    node IDs into Python
