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
//! Abstraction to interact with Memgraph.

use std::ffi::CStr;
use std::os::raw::c_char;

use crate::list::*;
use crate::mgp::*;
use crate::result::*;
use crate::rsmgp::*;
use crate::vertex::*;
// Required here, if not present, tests linking fails.
#[double]
use crate::mgp::ffi;
use mockall_double::double;

#[derive(Debug, PartialEq)]
pub enum MgpError {
    UnknownError,
    UnableToAllocate,
    InsufficientError,
    OutOfRange,
    LogicError,
    DeletedObject,
    InvalidArgument,
    KeyAlreadyExists,
    ImmutableObject,
    ValueConversion,
    SerializationError,
}

pub(crate) trait MgpDefault {
    fn default() -> Self;
}

macro_rules! mgp_default_mut_ptr {
    ($t:ident) => {
        impl MgpDefault for *mut $t {
            fn default() -> Self {
                std::ptr::null_mut()
            }
        }
    };
}

macro_rules! mgp_default_const_ptr {
    ($t:ident) => {
        impl MgpDefault for *const $t {
            fn default() -> Self {
                std::ptr::null()
            }
        }
    };
}

macro_rules! mgp_default_zero {
    ($t:ident) => {
        impl MgpDefault for $t {
            fn default() -> Self {
                0
            }
        }
    };
}

mgp_default_mut_ptr!(mgp_date);
mgp_default_mut_ptr!(mgp_duration);
mgp_default_mut_ptr!(mgp_edge);
mgp_default_mut_ptr!(mgp_edges_iterator);
mgp_default_mut_ptr!(mgp_list);
mgp_default_mut_ptr!(mgp_local_date_time);
mgp_default_mut_ptr!(mgp_local_time);
mgp_default_mut_ptr!(mgp_map);
mgp_default_mut_ptr!(mgp_map_item);
mgp_default_mut_ptr!(mgp_map_items_iterator);
mgp_default_mut_ptr!(mgp_path);
mgp_default_mut_ptr!(mgp_proc);
mgp_default_mut_ptr!(mgp_properties_iterator);
mgp_default_mut_ptr!(mgp_property);
mgp_default_mut_ptr!(mgp_result_record);
mgp_default_mut_ptr!(mgp_type);
mgp_default_mut_ptr!(mgp_value);
mgp_default_mut_ptr!(mgp_vertex);
mgp_default_mut_ptr!(mgp_vertices_iterator);

mgp_default_const_ptr!(c_char);
mgp_default_const_ptr!(u64);

mgp_default_zero!(i32);
mgp_default_zero!(i64);
mgp_default_zero!(u64);

impl MgpDefault for mgp_edge_id {
    fn default() -> Self {
        mgp_edge_id { as_int: 0 }
    }
}

impl MgpDefault for mgp_vertex_id {
    fn default() -> Self {
        mgp_vertex_id { as_int: 0 }
    }
}

impl MgpDefault for mgp_edge_type {
    fn default() -> Self {
        mgp_edge_type {
            name: std::ptr::null(),
        }
    }
}

impl MgpDefault for mgp_label {
    fn default() -> Self {
        mgp_label {
            name: std::ptr::null(),
        }
    }
}

impl MgpDefault for mgp_value_type {
    fn default() -> Self {
        mgp_value_type::MGP_VALUE_TYPE_NULL
    }
}

impl MgpDefault for f64 {
    fn default() -> Self {
        0.0
    }
}

pub(crate) fn to_rust_mgp_error(error: mgp_error) -> Option<MgpError> {
    match error {
        mgp_error::MGP_ERROR_NO_ERROR => None,
        mgp_error::MGP_ERROR_UNKNOWN_ERROR => Some(MgpError::UnknownError),
        mgp_error::MGP_ERROR_UNABLE_TO_ALLOCATE => Some(MgpError::UnableToAllocate),
        mgp_error::MGP_ERROR_INSUFFICIENT_BUFFER => Some(MgpError::InsufficientError),
        mgp_error::MGP_ERROR_OUT_OF_RANGE => Some(MgpError::OutOfRange),
        mgp_error::MGP_ERROR_LOGIC_ERROR => Some(MgpError::LogicError),
        mgp_error::MGP_ERROR_DELETED_OBJECT => Some(MgpError::DeletedObject),
        mgp_error::MGP_ERROR_INVALID_ARGUMENT => Some(MgpError::InvalidArgument),
        mgp_error::MGP_ERROR_KEY_ALREADY_EXISTS => Some(MgpError::KeyAlreadyExists),
        mgp_error::MGP_ERROR_IMMUTABLE_OBJECT => Some(MgpError::ImmutableObject),
        mgp_error::MGP_ERROR_VALUE_CONVERSION => Some(MgpError::ValueConversion),
        mgp_error::MGP_ERROR_SERIALIZATION_ERROR => Some(MgpError::SerializationError),
    }
}

macro_rules! invoke_mgp_func {
    ($result_type:ty, $func:expr) => {{
        let result : Result<$result_type, MgpError> = {
            let mut result: $result_type = MgpDefault::default();
            match to_rust_mgp_error($func(&mut result)) {
                None => Ok(result),
                Some(err) => Err(err),
            }
        };
        result
    }};
    ($result_type:ty, $func:expr, $($args:expr),+) => {{
        let result : Result<$result_type, MgpError> = {
            let mut result: $result_type = MgpDefault::default();
            match to_rust_mgp_error($func($($args),+, &mut result )) {
                None => Ok(result),
                Some(err) => Err(err),
            }
        };
        result
    }};
}

macro_rules! invoke_void_mgp_func {
    ($func:expr, $($args:expr),+) => {{
        let result : Result<(), MgpError> = {
            match to_rust_mgp_error($func($($args),+)) {
                None => Ok(()),
                Some(err) => Err(err),
            }
        };
        result
    }};
}

macro_rules! invoke_mgp_func_with_res {
    ($result_type:ty, $err:expr, $func:expr) => {{
        invoke_mgp_func!($result_type, $func).map_err(|_| -> Error { $err })
    }};
    ($result_type:ty, $err:expr, $func:expr, $($args:expr),+) => {{
        invoke_mgp_func!($result_type, $func, $($args),+).map_err(|_| -> Error { $err })
    }};
}

macro_rules! invoke_void_mgp_func_with_res {
    ($err:expr, $func:expr, $($args:expr),+) => {{
        invoke_void_mgp_func!($func, $($args),+).map_err(|_| -> Error { $err })
    }};
}

pub(crate) use invoke_mgp_func;
pub(crate) use invoke_mgp_func_with_res;
pub(crate) use invoke_void_mgp_func;
pub(crate) use invoke_void_mgp_func_with_res;

/// Combines the given array of types from left to right to construct [mgp_type]. E.g., if the
/// input is [Type::List, Type::Int], the constructed [mgp_type] is going to be list of integers.
fn resolve_mgp_type(types: &[Type]) -> *mut mgp_type {
    unsafe {
        let mut mgp_type_ptr: *mut mgp_type = std::ptr::null_mut();
        for field_type in types.iter().rev() {
            mgp_type_ptr = match field_type {
                Type::Any => invoke_mgp_func!(*mut mgp_type, ffi::mgp_type_any).unwrap(),
                Type::Bool => invoke_mgp_func!(*mut mgp_type, ffi::mgp_type_bool).unwrap(),
                Type::Number => invoke_mgp_func!(*mut mgp_type, ffi::mgp_type_number).unwrap(),
                Type::Int => invoke_mgp_func!(*mut mgp_type, ffi::mgp_type_int).unwrap(),
                Type::Double => invoke_mgp_func!(*mut mgp_type, ffi::mgp_type_float).unwrap(),
                Type::String => invoke_mgp_func!(*mut mgp_type, ffi::mgp_type_string).unwrap(),
                Type::Map => invoke_mgp_func!(*mut mgp_type, ffi::mgp_type_map).unwrap(),
                Type::Vertex => invoke_mgp_func!(*mut mgp_type, ffi::mgp_type_node).unwrap(),
                Type::Edge => invoke_mgp_func!(*mut mgp_type, ffi::mgp_type_relationship).unwrap(),
                Type::Path => invoke_mgp_func!(*mut mgp_type, ffi::mgp_type_path).unwrap(),
                Type::Nullable => {
                    invoke_mgp_func!(*mut mgp_type, ffi::mgp_type_nullable, mgp_type_ptr).unwrap()
                }
                Type::List => {
                    invoke_mgp_func!(*mut mgp_type, ffi::mgp_type_list, mgp_type_ptr).unwrap()
                }
                Type::Date => invoke_mgp_func!(*mut mgp_type, ffi::mgp_type_date).unwrap(),
                Type::LocalTime => {
                    invoke_mgp_func!(*mut mgp_type, ffi::mgp_type_local_time).unwrap()
                }
                Type::LocalDateTime => {
                    invoke_mgp_func!(*mut mgp_type, ffi::mgp_type_local_date_time).unwrap()
                }
                Type::Duration => invoke_mgp_func!(*mut mgp_type, ffi::mgp_type_duration).unwrap(),
            };
        }
        mgp_type_ptr
    }
}

/// Main object to interact with Memgraph instance.
#[derive(Clone)]
pub struct Memgraph {
    args: *mut mgp_list,
    graph: *mut mgp_graph,
    result: *mut mgp_result,
    memory: *mut mgp_memory,
    module: *mut mgp_module,
}

impl Memgraph {
    /// Create a new Memgraph object.
    ///
    /// Required to be public because the required pointers have to passed during module
    /// initialization and procedure call phase.
    pub fn new(
        args: *mut mgp_list,
        graph: *mut mgp_graph,
        result: *mut mgp_result,
        memory: *mut mgp_memory,
        module: *mut mgp_module,
    ) -> Memgraph {
        Memgraph {
            args,
            graph,
            result,
            memory,
            module,
        }
    }

    /// Creates a new object with all underlying data set to null. Used for the testing purposes.
    #[cfg(test)]
    pub(crate) fn new_default() -> Memgraph {
        Memgraph {
            args: std::ptr::null_mut(),
            graph: std::ptr::null_mut(),
            result: std::ptr::null_mut(),
            memory: std::ptr::null_mut(),
            module: std::ptr::null_mut(),
        }
    }

    /// Arguments passed to the procedure call.
    pub fn args(&self) -> Result<List> {
        // TODO(gitbuda): Avoid list copy when accessing procedure arguments.
        unsafe { List::mgp_copy(self.args_ptr(), &self) }
    }

    /// Returns pointer to the object with all arguments passed to the procedure call.
    pub(crate) fn args_ptr(&self) -> *mut mgp_list {
        self.args
    }

    /// Returns pointer to the object with graph data.
    pub(crate) fn graph_ptr(&self) -> *mut mgp_graph {
        self.graph
    }

    /// Returns pointer to the object where results could be stored.
    pub(crate) fn result_ptr(&self) -> *mut mgp_result {
        self.result
    }

    /// Returns pointer to the memory object for advanced memory control.
    pub(crate) fn memory_ptr(&self) -> *mut mgp_memory {
        self.memory
    }

    /// Returns pointer to the module object.
    pub fn module_ptr(&self) -> *mut mgp_module {
        self.module
    }

    pub fn vertices_iter(&self) -> Result<VerticesIterator> {
        unsafe {
            let mgp_iterator = invoke_mgp_func!(
                *mut mgp_vertices_iterator,
                ffi::mgp_graph_iter_vertices,
                self.graph_ptr(),
                self.memory_ptr()
            );
            if mgp_iterator.is_err() {
                return Err(Error::UnableToCreateGraphVerticesIterator);
            }
            Ok(VerticesIterator::new(mgp_iterator.unwrap(), &self))
        }
    }

    pub fn vertex_by_id(&self, id: i64) -> Result<Vertex> {
        unsafe {
            let mgp_vertex_ptr = invoke_mgp_func!(
                *mut mgp_vertex,
                ffi::mgp_graph_get_vertex_by_id,
                self.graph_ptr(),
                mgp_vertex_id { as_int: id },
                self.memory_ptr()
            );
            if mgp_vertex_ptr.is_err() {
                return Err(Error::UnableToFindVertexById);
            }
            Ok(Vertex::new(mgp_vertex_ptr.unwrap(), &self))
        }
    }

    /// Creates a new result record.
    ///
    /// Keep this object on the stack and add data that will be returned to Memgraph / client
    /// during/after the procedure call.
    pub fn result_record(&self) -> Result<ResultRecord> {
        ResultRecord::create(self)
    }

    /// Registers a new read procedure.
    ///
    /// * `proc_ptr` - Identifier of the top level C function that represents the procedure.
    /// * `name` - A string that will be registered as a procedure name inside Memgraph instance.
    /// * `required_arg_types` - An array of all [NamedType]s, each one define by name and an array
    ///    of [Type]s.
    /// * `optional_arg_types` - An array of all [OptionalNamedType]s, each one defined by name, an
    ///    array of [Type]s, and default value.
    /// * `result_field_types` - An array of all [NamedType]s, each one defined by name and an
    ///    array of [Type]s.
    pub fn add_read_procedure(
        &self,
        proc_ptr: extern "C" fn(*mut mgp_list, *mut mgp_graph, *mut mgp_result, *mut mgp_memory),
        name: &CStr,
        required_arg_types: &[NamedType],
        optional_arg_types: &[OptionalNamedType],
        result_field_types: &[NamedType],
    ) -> Result<()> {
        unsafe {
            let maybe_procedure = invoke_mgp_func!(
                *mut mgp_proc,
                ffi::mgp_module_add_read_procedure,
                self.module_ptr(),
                name.as_ptr(),
                Some(proc_ptr)
            );
            if maybe_procedure.is_err() {
                return Err(Error::UnableToRegisterReadProcedure);
            }
            let procedure = maybe_procedure.unwrap();

            for required_type in required_arg_types {
                let mgp_type = resolve_mgp_type(&required_type.types);
                if ffi::mgp_proc_add_arg(procedure, required_type.name.as_ptr(), mgp_type)
                    != mgp_error::MGP_ERROR_NO_ERROR
                {
                    return Err(Error::UnableToAddRequiredArguments);
                }
            }

            for optional_input in optional_arg_types {
                let mgp_type = resolve_mgp_type(&optional_input.types);

                if ffi::mgp_proc_add_opt_arg(
                    procedure,
                    optional_input.name.as_ptr(),
                    mgp_type,
                    optional_input.default.mgp_ptr(),
                ) != mgp_error::MGP_ERROR_NO_ERROR
                {
                    return Err(Error::UnableToAddOptionalArguments);
                }
            }

            for result_field in result_field_types {
                let mgp_type = resolve_mgp_type(&result_field.types);
                if result_field.deprecated {
                    if ffi::mgp_proc_add_deprecated_result(
                        procedure,
                        result_field.name.as_ptr(),
                        mgp_type,
                    ) != mgp_error::MGP_ERROR_NO_ERROR
                    {
                        return Err(Error::UnableToAddDeprecatedReturnType);
                    }
                } else if ffi::mgp_proc_add_result(procedure, result_field.name.as_ptr(), mgp_type)
                    != mgp_error::MGP_ERROR_NO_ERROR
                {
                    return Err(Error::UnableToAddReturnType);
                }
            }

            Ok(())
        }
    }

    /// Return `true` if the currently executing procedure should abort as soon as possible.
    ///
    /// Procedures which perform heavyweight processing run the risk of running too long and going
    /// over the query execution time limit. To prevent this, such procedures should periodically
    /// call this function at critical points in their code in order to determine whether they
    /// should abort or not. Note that this mechanism is purely cooperative and depends on the
    /// procedure doing the checking and aborting on its own.
    pub fn must_abort(&self) -> bool {
        unsafe { ffi::mgp_must_abort(self.graph_ptr()) != 0 }
    }
}

#[cfg(test)]
mod tests;
