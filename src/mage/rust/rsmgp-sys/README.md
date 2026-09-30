# Memgraph Mage Rust Query Modules

`rsmgp-sys` stands for Rust Memgraph Procedures "system" library to develop
query modules for Memgraph in Rust.

Adding a new Rust Memgraph query module is simple, just add the following to
your `Cargo.toml` project file.

```
[dependencies]
c_str_macro = "1.0.2"
rsmgp-sys = "^0.1.0"

[lib]
name = "query_module_name"
crate-type = ["cdylib"]
```

## Usage and Implementation Details

In the module/procedure code, it's preferred to use `Edge`, `List`, `Map`,
`Memgraph`, `Path`, `Property`, `Value`, `Vertex` structures. Structures
prefixed by `Mgp` are there to be a bridge between the procedure code and
Memgraph.  `Mgp` prefixed structures should be used to return data to Memgraph
/ client.  Underlying pointers prefixed by `mgp_` are exposed but shouldn't be
directly manipulated in the procedure call. The whole point of this library is
to hide that complexity as much as possible.

Memgraph Rust Query Modules API uses
[CStr](https://doc.rust-lang.org/std/ffi/struct.CStr.html) (`&CStr`) because
that's the most compatible type between Rust and Memgraph engine. [Rust
String](https://doc.rust-lang.org/std/string/struct.String.html) can validly
contain a null-byte in the middle of the string (0 is a valid Unicode
codepoint). This means that not all Rust strings can actually be translated to
C strings. While interacting with the `rsmgp` API, built-in `CStr` or
[c_str](https://docs.rs/c_str) library should be used because Memgraph query
modules API only provides C strings.
