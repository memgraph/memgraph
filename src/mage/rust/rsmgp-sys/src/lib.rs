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
//! A library to simplify implementation of Memgraph Query Modules.
//!
//! Example
//!
//! ```
//! use c_str_macro::c_str;
//! // All possible rsmgp modules (probably not all will be required).
//! use rsmgp_sys::edge::*;
//! use rsmgp_sys::list::*;
//! use rsmgp_sys::map::*;
//! use rsmgp_sys::memgraph::*;
//! use rsmgp_sys::mgp::*;
//! use rsmgp_sys::path::*;
//! use rsmgp_sys::property::*;
//! use rsmgp_sys::result::*;
//! use rsmgp_sys::rsmgp::*;
//! use rsmgp_sys::value::*;
//! use rsmgp_sys::vertex::*;
//! use rsmgp_sys::{close_module, define_procedure, define_type, init_module};
//! // The following are required because of used macros.
//! use std::ffi::CString;
//! use std::os::raw::c_int;
//! use std::panic;
//!
//! init_module!(|memgraph: &Memgraph| -> Result<()> {
//!     memgraph.add_read_procedure(
//!         basic, // Has to be the same as specified in the `define_procedure!`.
//!         c_str!("basic"), // Name under which Memgraph will register the procedure.
//!         &[define_type!("input_string", Type::String)],  // Required arguments.
//!         &[],                                            // Optional arguments.
//!         &[define_type!("output_string", Type::String)], // Return fields.
//!     )?;
//!     Ok(())
//! });
//!
//! define_procedure!(basic, |memgraph: &Memgraph| -> Result<()> {
//!     let result = memgraph.result_record()?;
//!     let args = memgraph.args()?;
//!     let output_string = args.value_at(0)?;
//!     result.insert_mgp_value(
//!         c_str!("output_string"),
//!         &output_string.to_mgp_value(&memgraph)?,
//!     )?;
//!     Ok(())
//! });
//!
//! close_module!(|| -> Result<()> { Ok(()) });
//! ```

#[cfg(test)]
extern crate mockall;

extern crate mockall_double;

mod temporal;
mod testing;

pub mod edge;
pub mod list;
pub mod map;
pub mod memgraph;
pub mod mgp;
pub mod path;
pub mod property;
pub mod result;
pub mod rsmgp;
pub mod value;
pub mod vertex;
