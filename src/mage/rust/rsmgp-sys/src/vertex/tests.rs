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
use std::ffi::{CStr, CString};
use std::ptr::null_mut;

use super::*;
use crate::memgraph::Memgraph;
use crate::mgp::mock_ffi::*;
use crate::{mock_mgp_once, with_dummy};

#[test]
#[serial]
fn test_id() {
    mock_mgp_once!(mgp_vertex_get_id_context, |_, vertex_id_ptr| unsafe {
        (*vertex_id_ptr).as_int = 72;
        mgp_error::MGP_ERROR_NO_ERROR
    });

    with_dummy!(Vertex, |vertex: &Vertex| {
        assert_eq!(vertex.id(), 72);
    });
}

#[test]
#[serial]
fn test_labels_count() {
    mock_mgp_once!(mgp_vertex_labels_count_context, |_, labels_count| unsafe {
        (*labels_count) = 2;
        mgp_error::MGP_ERROR_NO_ERROR
    });

    with_dummy!(Vertex, |vertex: &Vertex| {
        assert_eq!(vertex.labels_count().unwrap(), 2);
    });
}

#[test]
#[serial]
fn test_has_label() {
    mock_mgp_once!(
        mgp_vertex_has_label_context,
        |vertex, label, result| unsafe {
            assert_eq!(vertex, null_mut());
            assert_eq!(CStr::from_ptr(label.name), c_str!("labela"));
            (*result) = 1;
            mgp_error::MGP_ERROR_NO_ERROR
        }
    );

    with_dummy!(Vertex, |vertex: &Vertex| {
        assert_eq!(vertex.has_label(c_str!("labela")).unwrap(), true);
    });
}

#[test]
#[serial]
fn test_label_at() {
    let test_label = CString::new("test");
    mock_mgp_once!(
        mgp_vertex_label_at_context,
        move |vertex, _, result| unsafe {
            assert_eq!(vertex, null_mut());
            (*result).name = test_label.as_ref().unwrap().as_ptr();
            mgp_error::MGP_ERROR_NO_ERROR
        }
    );

    with_dummy!(Vertex, |vertex: &Vertex| {
        assert_eq!(vertex.label_at(5).unwrap(), CString::new("test").unwrap());
    });
}

#[test]
#[serial]
fn test_property() {
    mock_mgp_once!(
        mgp_vertex_get_property_context,
        move |vertex, prop_name, memory, _| {
            assert_eq!(vertex, null_mut());
            assert_eq!(prop_name, c_str!("test").as_ptr());
            assert_eq!(memory, null_mut());
            mgp_error::MGP_ERROR_UNABLE_TO_ALLOCATE
        }
    );

    with_dummy!(Vertex, |vertex: &Vertex| {
        assert_eq!(
            vertex.property(c_str!("test")).err().unwrap(),
            Error::UnableToGetVertexProperty
        );
    });
}

#[test]
#[serial]
fn test_properties() {
    mock_mgp_once!(mgp_vertex_iter_properties_context, |_, _, _| {
        mgp_error::MGP_ERROR_UNABLE_TO_ALLOCATE
    });

    with_dummy!(Vertex, |vertex: &Vertex| {
        let iter = vertex.properties();
        assert!(iter.is_err());
    });
}

#[test]
#[serial]
fn test_in_edges() {
    mock_mgp_once!(mgp_vertex_iter_in_edges_context, |_, _, _| {
        mgp_error::MGP_ERROR_UNABLE_TO_ALLOCATE
    });

    with_dummy!(Vertex, |vertex: &Vertex| {
        let iter = vertex.in_edges();
        assert!(iter.is_err());
    });
}

#[test]
#[serial]
fn test_out_edges() {
    mock_mgp_once!(mgp_vertex_iter_out_edges_context, |_, _, _| {
        mgp_error::MGP_ERROR_UNABLE_TO_ALLOCATE
    });

    with_dummy!(Vertex, |vertex: &Vertex| {
        let iter = vertex.out_edges();
        assert!(iter.is_err());
    });
}
