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
use crate::memgraph::Memgraph;
use crate::mgp::mock_ffi::*;
use crate::testing::alloc::*;
use crate::{mock_mgp_once, with_dummy};
use libc::{c_void, free};

#[test]
#[serial]
fn test_mgp_copy() {
    mock_mgp_once!(mgp_list_size_context, |_, size_ptr| unsafe {
        (*size_ptr) = 1;
        mgp_error::MGP_ERROR_NO_ERROR
    });
    mock_mgp_once!(mgp_list_make_empty_context, |_, _, list_ptr_ptr| unsafe {
        (*list_ptr_ptr) = alloc_mgp_list();
        mgp_error::MGP_ERROR_NO_ERROR
    });
    mock_mgp_once!(mgp_list_at_context, |_, _, _| {
        mgp_error::MGP_ERROR_NO_ERROR
    });
    mock_mgp_once!(mgp_list_append_context, |_, _| {
        mgp_error::MGP_ERROR_UNABLE_TO_ALLOCATE
    });
    mock_mgp_once!(mgp_list_destroy_context, |ptr| unsafe {
        free(ptr as *mut c_void);
    });

    with_dummy!(|memgraph: &Memgraph| {
        unsafe {
            let value = List::mgp_copy(std::ptr::null_mut(), &memgraph);
            assert!(value.is_err());
        }
    });
}

#[test]
#[serial]
fn test_make_empty() {
    mock_mgp_once!(mgp_list_make_empty_context, |_, _, _| {
        mgp_error::MGP_ERROR_UNABLE_TO_ALLOCATE
    });

    with_dummy!(|memgraph: &Memgraph| {
        let value = List::make_empty(0, &memgraph);
        assert!(value.is_err());
    });
}

#[test]
#[serial]
fn test_append() {
    mock_mgp_once!(mgp_value_make_null_context, |_, value_ptr_ptr| unsafe {
        (*value_ptr_ptr) = alloc_mgp_value();
        mgp_error::MGP_ERROR_NO_ERROR
    });
    mock_mgp_once!(mgp_list_append_context, |_, _| {
        mgp_error::MGP_ERROR_INSUFFICIENT_BUFFER
    });
    mock_mgp_once!(mgp_value_destroy_context, |ptr| unsafe {
        free(ptr as *mut c_void);
    });

    with_dummy!(List, |list: &List| {
        assert!(list.append(&Value::Null).is_err());
    });
}

#[test]
#[serial]
fn test_append_extend() {
    mock_mgp_once!(mgp_value_make_null_context, |_, value_ptr_ptr| unsafe {
        (*value_ptr_ptr) = alloc_mgp_value();
        mgp_error::MGP_ERROR_NO_ERROR
    });
    mock_mgp_once!(mgp_list_append_extend_context, |_, _| {
        mgp_error::MGP_ERROR_INSUFFICIENT_BUFFER
    });
    mock_mgp_once!(mgp_value_destroy_context, |ptr| unsafe {
        free(ptr as *mut c_void);
    });

    with_dummy!(List, |list: &List| {
        assert!(list.append_extend(&Value::Null).is_err());
    });
}

#[test]
#[serial]
fn test_size() {
    mock_mgp_once!(mgp_list_size_context, |_, size_ptr| unsafe {
        (*size_ptr) = 3;
        mgp_error::MGP_ERROR_NO_ERROR
    });

    with_dummy!(List, |list: &List| {
        assert_eq!(list.size(), 3);
    });
}

#[test]
#[serial]
fn test_capacity() {
    mock_mgp_once!(mgp_list_capacity_context, |_, capacity_ptr| unsafe {
        (*capacity_ptr) = 42;
        mgp_error::MGP_ERROR_NO_ERROR
    });

    with_dummy!(List, |list: &List| {
        assert_eq!(list.capacity(), 42);
    });
}

#[test]
#[serial]
fn test_value_at() {
    mock_mgp_once!(mgp_list_at_context, |_, _, _| {
        mgp_error::MGP_ERROR_OUT_OF_RANGE
    });

    with_dummy!(List, |list: &List| {
        assert!(list.value_at(0).is_err());
    });
}

#[test]
#[serial]
fn test_empty_list_iter() {
    mock_mgp_once!(mgp_list_size_context, |_, size_ptr| unsafe {
        (*size_ptr) = 0;
        mgp_error::MGP_ERROR_NO_ERROR
    });

    with_dummy!(List, |list: &List| {
        let iter = list.iter();
        assert!(iter.is_ok());
        let value = iter.unwrap().next();
        assert!(value.is_none());
    });
}
