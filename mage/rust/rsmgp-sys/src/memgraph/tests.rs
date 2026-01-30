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

use c_str_macro::c_str;
use serial_test::serial;

use super::*;
use crate::define_type;
use crate::mgp::mock_ffi::*;
use crate::testing::alloc::*;
use crate::{mock_mgp_once, with_dummy};

#[test]
#[serial]
fn test_vertices_iterator() {
    mock_mgp_once!(mgp_graph_iter_vertices_context, |_, _, _| {
        mgp_error::MGP_ERROR_UNABLE_TO_ALLOCATE
    });

    with_dummy!(|memgraph: &Memgraph| {
        let value = memgraph.vertices_iter();
        assert!(value.is_err());
    });
}

#[test]
#[serial]
fn test_vertex_by_id() {
    mock_mgp_once!(mgp_graph_get_vertex_by_id_context, |_, _, _, _| {
        mgp_error::MGP_ERROR_UNABLE_TO_ALLOCATE
    });

    with_dummy!(|memgraph: &Memgraph| {
        let value = memgraph.vertex_by_id(0);
        assert!(value.is_err());
    });
}

#[no_mangle]
extern "C" fn dummy_c_func(
    _: *mut mgp_list,
    _: *mut mgp_graph,
    _: *mut mgp_result,
    _: *mut mgp_memory,
) {
}

macro_rules! mock_mgp_type_once {
    ($c_func_name:ident) => {
        mock_mgp_once!($c_func_name, |type_ptr_ptr| unsafe {
            (*type_ptr_ptr) = alloc_mgp_type();
            mgp_error::MGP_ERROR_NO_ERROR
        });
    };
}
#[test]
#[serial]
fn test_add_read_procedure() {
    mock_mgp_once!(
        mgp_module_add_read_procedure_context,
        |_, _, _, proc_ptr_ptr| unsafe {
            (*proc_ptr_ptr) = alloc_mgp_proc();
            mgp_error::MGP_ERROR_NO_ERROR
        }
    );
    let ctx_any = mgp_type_any_context();
    ctx_any.expect().times(3).returning(|type_ptr_ptr| unsafe {
        (*type_ptr_ptr) = alloc_mgp_type();
        mgp_error::MGP_ERROR_NO_ERROR
    });
    mock_mgp_type_once!(mgp_type_bool_context);
    mock_mgp_type_once!(mgp_type_number_context);
    mock_mgp_type_once!(mgp_type_int_context);
    mock_mgp_type_once!(mgp_type_float_context);
    mock_mgp_type_once!(mgp_type_string_context);
    mock_mgp_type_once!(mgp_type_map_context);
    mock_mgp_type_once!(mgp_type_node_context);
    mock_mgp_type_once!(mgp_type_relationship_context);
    mock_mgp_type_once!(mgp_type_path_context);
    mock_mgp_once!(mgp_type_nullable_context, |_, type_ptr_ptr| unsafe {
        (*type_ptr_ptr) = alloc_mgp_type();
        mgp_error::MGP_ERROR_NO_ERROR
    });
    mock_mgp_once!(mgp_type_list_context, |_, type_ptr_ptr| unsafe {
        (*type_ptr_ptr) = alloc_mgp_type();
        mgp_error::MGP_ERROR_NO_ERROR
    });
    let ctx_add_result = mgp_proc_add_result_context();
    ctx_add_result
        .expect()
        .times(12)
        .returning(|_, _, _| mgp_error::MGP_ERROR_NO_ERROR);

    with_dummy!(|memgraph: &Memgraph| {
        assert!(memgraph
            .add_read_procedure(
                dummy_c_func,
                c_str!("dummy_c_func"),
                &[],
                &[],
                &[
                    define_type!("any", Type::Any),
                    define_type!("bool", Type::Bool),
                    define_type!("number", Type::Number),
                    define_type!("int", Type::Int),
                    define_type!("double", Type::Double),
                    define_type!("string", Type::String),
                    define_type!("map", Type::Map),
                    define_type!("vertex", Type::Vertex),
                    define_type!("edge", Type::Edge),
                    define_type!("path", Type::Path),
                    define_type!("nullable", Type::Nullable, Type::Any),
                    define_type!("list", Type::List, Type::Any),
                ],
            )
            .is_ok());
    });
}
