// Copyright (c) 2016-2021 Memgraph Ltd. [https://memgraph.com]
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//! Macro definitions and top level data structures.
//!
//! [define_procedure], [init_module], [close_module] all accept a function accepting [Memgraph]
//! and returning [crate::result::Result] because that allows using `?` operator which is a very
//! convenient way of propagating execution errors.
//!
//! Example
//!
//! ```no run
//! |memgraph: &Memgraph| -> Result<()> {
//!     // Implementation
//! }
//! ```

use std::ffi::CStr;

use crate::memgraph::*;
#[double]
use crate::mgp::ffi;
use crate::value::*;
use mockall_double::double;

/// Defines a new procedure callable by Memgraph engine.
///
/// Wraps call to the Rust function provided as a second argument. In addition to the function
/// call, it also sets the catch_unwind hook and properly handles execution errors. The first macro
/// argument is the exact name of the C procedure Memgraph will be able to run (the exact same name
/// has to be registered inside [init_module] phase. The second argument has to be a function
/// accepting reference [Memgraph]. The [Memgraph] object could be used to interact with Memgraph
/// instance.
///
/// Example
///
/// ```no run
/// define_procedure!(procedure_name, |memgraph: &Memgraph| -> Result<()> {
///     // Implementation
/// }
/// ```
#[macro_export]
macro_rules! define_procedure {
    ($c_name:ident, $rs_func:expr) => {
        #[no_mangle]
        extern "C" fn $c_name(
            args: *mut mgp_list,
            graph: *mut mgp_graph,
            result: *mut mgp_result,
            memory: *mut mgp_memory,
        ) {
            let prev_hook = panic::take_hook();
            panic::set_hook(Box::new(|_| { /* Do nothing. */ }));

            let procedure_result = panic::catch_unwind(|| {
                let memgraph = Memgraph::new(args, graph, result, memory, std::ptr::null_mut());
                match $rs_func(&memgraph) {
                    Ok(_) => (),
                    Err(e) => {
                        let msg = e.to_string();
                        println!("{}", msg);
                        let c_msg =
                            CString::new(msg).expect("Unable to create Memgraph error message!");
                        set_memgraph_error_msg(&c_msg, &memgraph);
                    }
                }
            });

            panic::set_hook(prev_hook);
            match procedure_result {
                Ok(_) => {}
                Err(e) => {
                    println!("Procedure panic!");
                    let memgraph = Memgraph::new(args, graph, result, memory, std::ptr::null_mut());
                    match e.downcast::<&str>() {
                        Ok(msg) => {
                            println!("{}", msg);
                            let c_msg = CString::new(msg.as_bytes())
                                .expect("Unable to create Memgraph PANIC error message!");
                            set_memgraph_error_msg(&c_msg, &memgraph);
                        }
                        Err(_) => {
                            println!("Unknown type of panic!.");
                        }
                    }
                }
            }
        }
    };
}

/// Initializes Memgraph query module.
///
/// Example
///
/// ```no run
/// init_module!(|memgraph: &Memgraph| -> Result<()> {
///     memgraph.add_read_procedure(
///         procedure_name,
///         c_str!("procedure_name"),
///         &[define_type!("list", Type::List, Type::Int),],
///     )?;
///     Ok(())
/// });
/// ```
#[macro_export]
macro_rules! init_module {
    ($init_func:expr) => {
        #[no_mangle]
        pub extern "C" fn mgp_init_module(
            module: *mut mgp_module,
            memory: *mut mgp_memory,
        ) -> c_int {
            let prev_hook = panic::take_hook();
            panic::set_hook(Box::new(|_| { /* Do nothing. */ }));
            let result = panic::catch_unwind(|| {
                let memgraph = Memgraph::new(
                    std::ptr::null_mut(),
                    std::ptr::null_mut(),
                    std::ptr::null_mut(),
                    memory,
                    module,
                );
                $init_func(&memgraph)
            });
            panic::set_hook(prev_hook);
            match result {
                Ok(_) => 0,
                Err(_) => 1,
            }
        }
    };
}

/// Closes Memgraph query module.
///
/// Example
///
/// ```no run
/// close_module!(|memgraph: &Memgraph| -> Result<()> {
///     // Implementation
/// }
#[macro_export]
macro_rules! close_module {
    ($close_func:expr) => {
        #[no_mangle]
        pub extern "C" fn mgp_shutdown_module() -> c_int {
            let prev_hook = panic::take_hook();
            panic::set_hook(Box::new(|_| { /* Do nothing. */ }));
            let result = panic::catch_unwind(|| $close_func());
            panic::set_hook(prev_hook);
            match result {
                Ok(_) => 0,
                Err(_) => 1,
            }
        }
    };
}

/// Used to pass expected types (arguments, optional arguments, result field types) to the Memgraph
/// engine.
pub enum Type {
    Any,
    Bool,
    Number,
    Int,
    Double,
    String,
    Map,
    Vertex,
    Edge,
    Path,
    Nullable,
    List,
    Date,
    LocalTime,
    LocalDateTime,
    Duration,
}

/// Used to pass expected type during procedure registration for required parameters and result
/// values.
///
/// The final type is deduced by processing types field from left to right. E.g., [Type::Any]
/// means any return type, [Type::List], [Type::Int] means a list of integer values.
///
/// `deprecated` is used to register deprecated result type.
pub struct NamedType<'a> {
    pub name: &'a CStr,
    pub types: &'a [Type],
    pub deprecated: bool,
}

/// Used to pass expected type during procedure registration for optional parameters.
///
/// The final type is deduced by processing types field from left to right. E.g., [Type::Any]
/// means any return type, [Type::List], [Type::Int] means a list of integer values.
///
/// `default` stores value in case the caller doesn't pass any value.
pub struct OptionalNamedType<'a> {
    pub name: &'a CStr,
    pub types: &'a [Type],
    pub default: &'a MgpValue,
}

/// Defines a regular type.
///
/// Example of defining a list of integers
///
/// ```no run
/// define_type!("name", Type::List, Type::Int);
/// ```
#[macro_export]
macro_rules! define_type {
    ($name:literal, $($types:expr),+) => {
        NamedType {
            name: &c_str!($name),
            types: &[$($types),+],
            deprecated: false,
        }
    };
}

/// Defines a deprecated type.
///
/// Example of defining a list of integers
///
/// ```no run
/// define_deprecated_type!("name", Type::List, Type::Int);
/// ```
#[macro_export]
macro_rules! define_deprecated_type {
    ($name:literal, $($types:expr),+) => {
        NamedType {
            name: &c_str!($name),
            types: &[$($types),+],
            deprecated: true,
        }
    };
}

/// Defines an optional type.
///
/// Example of defining an int.
///
/// ```no run
/// define_optional_type!("name", &MgpValue::make_int(0, &memgraph), Type::List, Type::Int);
/// ```
#[macro_export]
macro_rules! define_optional_type {
    ($name:literal, $default:expr, $($types:expr),+) => {
        OptionalNamedType {
            name: &c_str!($name),
            types: &[$($types),+],
            default: $default,
        }
    };
}

/// Sets error that will be returned to the caller.
pub fn set_memgraph_error_msg(msg: &CStr, memgraph: &Memgraph) {
    unsafe {
        invoke_void_mgp_func!(
            ffi::mgp_result_set_error_msg,
            memgraph.result_ptr(),
            msg.as_ptr()
        )
        .expect("Unable to pass error message to the Memgraph engine.");
    }
}

#[cfg(test)]
mod tests {
    use c_str_macro::c_str;
    use serial_test::serial;

    use super::*;
    use crate::mgp::mock_ffi::*;
    use crate::{mock_mgp_once, with_dummy};

    #[test]
    #[serial]
    fn test_set_error_msg() {
        mock_mgp_once!(mgp_result_set_error_msg_context, |_, _| {
            crate::mgp::mgp_error::MGP_ERROR_NO_ERROR
        });

        with_dummy!(|memgraph: &Memgraph| {
            set_memgraph_error_msg(c_str!("test_error"), &memgraph);
        });
    }
}
