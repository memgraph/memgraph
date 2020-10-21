# Property Storage

Although the reader is probably familiar with properties in *Memgraph*, let's
briefly recap.

Both vertices and edges can store an arbitrary number of properties. Properties
are, in essence, ordered pairs of property names and property values. Each
property name within a single graph element (edge/node) can store a single
property value. Property names are represented as strings, while property values
must be one of the following types:

 Type      | Description
-----------|------------
 `Null`    | Denotes that the property has no value. This is the same as if the property does not exist.
 `String`  | A character string, i.e. text.
 `Boolean` | A boolean value, either `true` or `false`.
 `Integer` | An integer number.
 `Float`   | A floating-point number, i.e. a real number.
 `List`    | A list containing any number of property values of any supported type. It can be used to store multiple values under a single property name.
 `Map`     | A mapping of string keys to values of any supported type.

Property values are modeled in a class conveniently called `PropertyValue`.

## Mapping Between Property Names and Property Keys.

Although users think of property names in terms of descriptive strings
(e.g. "location" or "department"), *Memgraph* internally converts those names
into property keys which are, essentially, unsigned 16-bit integers.

Property keys are modelled by a not-so-conveniently named class called
`Property` which can be found in `storage/types.hpp`. The actual conversion
between property names and property keys is done within the `ConcurrentIdMapper`
but the internals of that implementation are out of scope for understanding
property storage.

## PropertyValueStore

Both `Edge` and `Vertex` objects contain an instance of `PropertyValueStore`
object which is responsible for storing properties of a corresponding graph
element.

An interface of `PropertyValueStore` is as follows:

 Method    | Description
-----------|------------
 `at`      | Returns the `PropertyValue` for a given `Property` (key).
 `set`     | Stores a given `PropertyValue` under a given `Property` (key).
 `erase`   | Deletes a given `Property` (key) alongside its corresponding `PropertyValue`.
 `clear`   | Clears the storage.
 `iterator`| Provides an extension of `std::input_iterator` that iterates over storage.

## Storage Location

By default, *Memgraph* is an in-memory database and all properties are therefore
stored in working memory unless specified otherwise by the user. User has an
option to specify via the command line which properties they wish to be stored
on disk.

Storage location of each property is encapsulated within a `Property` object
which is ensured by the `ConcurrentIdMapper`. More precisely, the unsigned 16-bit
property key has the following format:

```
|---location--|------id------|
|-Memory|Disk-|-----2^15-----|
```

In other words, the most significant bit determines the location where the
property will be stored.

### In-memory Storage

The underlying implementation of in-memory storage for the time being is
`std::vector<std::pair<Property, PropertyValue>>`. Implementations of`at`, `set`
and `erase` are linear in time. This implementation is arguably more efficient
than `std::map` or `std::unordered_map` when the average number of properties of
a record is relatively small (up to 10) which seems to be the case.

### On-disk Storage

#### KVStore

Disk storage is modeled by an abstraction of key-value storage as implemented in
`storage/kvstore.hpp'. An interface of this abstraction is as follows:

 Method         | Description
----------------|------------
 `Put`          | Stores the given value under the given key.
 `Get`          | Obtains the given value stored under the given key.
 `Delete`       | Deletes a given (key, value) pair from storage..
 `DeletePrefix` | Deletes all (key, value) pairs where key begins with a given prefix.
 `Size`         | Returns the size of the storage or, optionally, the number of stored pairs that begin with a given prefix.
 `iterator`     | Provides an extension of `std::input_iterator` that iterates over storage.

Keys and values in this context are of type `std::string`.

The actual underlying implementation of this abstraction uses
[RocksDB]{https://rocksdb.org} &mdash; a persistent key-value store for fast
storage.

It is worthy to note that the custom iterator implementation allows the user
to iterate over a given prefix. Otherwise, the implementation follows familiar
c++ constructs and can be used as follows:

```
KVStore storage = ...;
for (auto it = storage.begin(); it != storage.end(); ++it) {}
for (auto kv : storage) {}
for (auto it = storage.begin("prefix"); it != storage.end("prefix"); ++it) {}
```

Note that it is not possible to scan over multiple prefixes. For instance, one
might assume that you can scan over all keys that fall in a certain
lexicographical range. Unfortunately, that is not the case and running the
following code will result in an infinite loop with a touch of undefined
behavior.

```
KVStore storage = ...;
for (auto it = storage.begin("alpha"); it != storage.end("omega"); ++it) {}
```

#### Data Organization on Disk

Each `PropertyValueStore` instance can access a static `KVStore` object that can
store `(key, value)` pairs on disk. The key of each property on disk consists of
two parts &mdash; a unique identifier (unsigned 64-bit integer) of the current
record version (see mvcc docummentation for further clarification) and a
property key as described above. The actual value of the property is serialized
into a bytestring using bolt `BaseEncoder`. Similarly, deserialization is
performed by bolt `Decoder`.
