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

use serial_test::serial;

use super::*;
use crate::mgp::mock_ffi::*;
use crate::{mock_mgp_once, with_dummy};

#[test]
#[serial]
fn test_mgp_copy() {
    mock_mgp_once!(mgp_path_copy_context, |_, _, _| {
        mgp_error::MGP_ERROR_UNABLE_TO_ALLOCATE
    });

    with_dummy!(|memgraph: &Memgraph| {
        unsafe {
            let path = Path::mgp_copy(std::ptr::null_mut(), &memgraph);
            assert!(path.is_err());
        }
    });
}

#[test]
#[serial]
fn test_mgp_ptr() {
    with_dummy!(Path, |path: &Path| {
        let ptr = path.mgp_ptr();
        assert!(ptr.is_null());
    });
}

#[test]
#[serial]
fn test_size() {
    mock_mgp_once!(mgp_path_size_context, |_, size_ptr| unsafe {
        (*size_ptr) = 2;
        mgp_error::MGP_ERROR_NO_ERROR
    });

    with_dummy!(Path, |path: &Path| {
        assert_eq!(path.size(), 2);
    });
}

#[test]
#[serial]
fn test_make_with_start() {
    mock_mgp_once!(mgp_path_make_with_start_context, |_, _, _| {
        mgp_error::MGP_ERROR_UNABLE_TO_ALLOCATE
    });

    with_dummy!(|memgraph: &Memgraph| {
        let vertex = Vertex::new(std::ptr::null_mut(), &memgraph);
        assert!(Path::make_with_start(&vertex, &memgraph).is_err());
    });
}

#[test]
#[serial]
fn test_expand() {
    mock_mgp_once!(mgp_path_expand_context, |_, _| {
        mgp_error::MGP_ERROR_UNABLE_TO_ALLOCATE
    });

    with_dummy!(|memgraph: &Memgraph| {
        let edge = Edge::new(std::ptr::null_mut(), &memgraph);
        let path = Path::new(std::ptr::null_mut(), &memgraph);
        assert!(path.expand(&edge).is_err());
    });
}

#[test]
#[serial]
fn test_vertex_at() {
    mock_mgp_once!(mgp_path_vertex_at_context, |_, _, _| {
        mgp_error::MGP_ERROR_OUT_OF_RANGE
    });

    with_dummy!(Path, |path: &Path| {
        assert!(path.vertex_at(0).is_err());
    });
}

#[test]
#[serial]
fn test_edge_at() {
    mock_mgp_once!(mgp_path_edge_at_context, |_, _, _| {
        mgp_error::MGP_ERROR_OUT_OF_RANGE
    });

    with_dummy!(Path, |path: &Path| {
        assert!(path.edge_at(0).is_err());
    });
}
