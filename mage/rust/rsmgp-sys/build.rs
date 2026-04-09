use std::env;
use std::fs;
use std::path::PathBuf;

fn main() {
    let mg_procedure_path = "mgp/mg_procedure.h";

    println!("cargo:rerun-if-changed={}", mg_procedure_path);

    let bindings = bindgen::Builder::default()
        .header(mg_procedure_path)
        .blocklist_function("mgp_*")
        .rustified_enum("mgp_error")
        .rustified_enum("mgp_value_type")
        .parse_callbacks(Box::new(bindgen::CargoCallbacks::new()))
        .generate()
        .expect("Unable to generate bindings")
        .to_string();

    let mut bindings_string = "#[cfg(test)] use mockall::automock;\n".to_owned();
    bindings_string.push_str("#[cfg_attr(test, automock)]\npub(crate) mod ffi {\nuse super::*;\n");
    bindings_string.push_str(
        &bindgen::Builder::default()
            .header(mg_procedure_path)
            .blocklist_type(".*")
            .parse_callbacks(Box::new(bindgen::CargoCallbacks::new()))
            .generate()
            .expect("Unable to generate bindings")
            .to_string(),
    );
    bindings_string.push_str("}\n");
    bindings_string.push_str(&bindings);

    let out_path = PathBuf::from(env::var("OUT_DIR").unwrap()).join("bindings.rs");
    fs::write(out_path, bindings_string.as_bytes()).expect("Couldn't write bindings!");
}
