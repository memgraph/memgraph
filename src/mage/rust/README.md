# Memgraph Mage Rust Query Modules

`rsmgp-sys` stands for Rust Memgraph Procedures "system" library to develop
query modules for Memgraph in Rust.

It's possible to create your Memgraph query module by adding a new project to
the `rust/` folder. The project has to be a standard Rust project with `[lib]`
section specifying dynamic lib as the `crate-type`.

```
[lib]
name = "query_module_name"
crate-type = ["cdylib"]
```

Please take a look at the example project.
