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
use std::ptr::null_mut;

use super::*;
use crate::mgp::mock_ffi::*;
use crate::testing::alloc::*;
use crate::{mock_mgp_once, with_dummy};
use libc::{c_void, free};

#[test]
#[serial]
fn test_make_empty() {
    mock_mgp_once!(mgp_map_make_empty_context, |_, _| {
        mgp_error::MGP_ERROR_UNABLE_TO_ALLOCATE
    });

    with_dummy!(|memgraph: &Memgraph| {
        let value = Map::make_empty(&memgraph);
        assert!(value.is_err());
    });
}

#[test]
#[serial]
fn test_mgp_copy() {
    mock_mgp_once!(mgp_map_make_empty_context, |_, map_ptr_ptr| unsafe {
        (*map_ptr_ptr) = alloc_mgp_map();
        mgp_error::MGP_ERROR_NO_ERROR
    });
    mock_mgp_once!(mgp_map_iter_items_context, |_, _, iter_ptr_ptr| unsafe {
        (*iter_ptr_ptr) = alloc_mgp_map_items_iterator();
        mgp_error::MGP_ERROR_NO_ERROR
    });
    mock_mgp_once!(
        mgp_map_items_iterator_get_context,
        |_, item_ptr_ptr| unsafe {
            (*item_ptr_ptr) = null_mut();
            mgp_error::MGP_ERROR_NO_ERROR
        }
    );
    mock_mgp_once!(mgp_map_destroy_context, |ptr| unsafe {
        free(ptr as *mut c_void);
    });
    mock_mgp_once!(mgp_map_items_iterator_destroy_context, |ptr| unsafe {
        free(ptr as *mut c_void);
    });

    with_dummy!(|memgraph: &Memgraph| {
        unsafe {
            let value = Map::mgp_copy(null_mut(), &memgraph);
            assert!(value.is_ok());
        }
    });
}

#[test]
#[serial]
fn test_insert() {
    mock_mgp_once!(mgp_value_make_null_context, |_, value_ptr_ptr| unsafe {
        (*value_ptr_ptr) = alloc_mgp_value();
        mgp_error::MGP_ERROR_NO_ERROR
    });
    mock_mgp_once!(mgp_map_insert_context, |_, _, _| {
        mgp_error::MGP_ERROR_KEY_ALREADY_EXISTS
    });
    mock_mgp_once!(mgp_value_destroy_context, |ptr| unsafe {
        free(ptr as *mut c_void);
    });

    with_dummy!(Map, |map: &Map| {
        let value = Value::Null;
        assert!(map.insert(c_str!("key"), &value).is_err());
    });
}

#[test]
#[serial]
fn test_size() {
    mock_mgp_once!(mgp_map_size_context, |_, size_ptr| unsafe {
        (*size_ptr) = 3;
        mgp_error::MGP_ERROR_NO_ERROR
    });

    with_dummy!(Map, |map: &Map| {
        let value = map.size();
        assert_eq!(value, 3);
    });
}

#[test]
#[serial]
fn test_at() {
    mock_mgp_once!(mgp_map_at_context, |_, _, value_ptr_ptr| unsafe {
        (*value_ptr_ptr) = null_mut();
        mgp_error::MGP_ERROR_NO_ERROR
    });

    with_dummy!(Map, |map: &Map| {
        let value = map.at(c_str!("key"));
        assert!(value.is_err());
    });
}

#[test]
#[serial]
fn test_empty_map_iter() {
    mock_mgp_once!(mgp_map_iter_items_context, |_, _, _| {
        mgp_error::MGP_ERROR_UNABLE_TO_ALLOCATE
    });

    with_dummy!(Map, |map: &Map| {
        let iter = map.iter();
        assert!(iter.is_err());
    });
}
