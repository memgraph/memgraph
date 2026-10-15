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
use crate::memgraph::Memgraph;
use crate::mgp::mock_ffi::*;
use crate::testing::alloc::*;
use crate::{mock_mgp_once, with_dummy};
use libc::{c_void, free};

#[test]
#[serial]
fn test_create_record() {
    mock_mgp_once!(mgp_result_new_record_context, |_, _| {
        mgp_error::MGP_ERROR_UNABLE_TO_ALLOCATE
    });

    with_dummy!(|memgraph: &Memgraph| {
        let result_record = ResultRecord::create(&memgraph);
        assert!(result_record.is_err());
    });
}

macro_rules! mock_mgp_value_make_with_mem {
    ($c_func_name:ident) => {
        mock_mgp_once!($c_func_name, |_, _, value_ptr_ptr| unsafe {
            (*value_ptr_ptr) = alloc_mgp_value();
            mgp_error::MGP_ERROR_NO_ERROR
        });
    };
}

macro_rules! mock_mgp_value_make_without_mem {
    ($c_func_name:ident) => {
        mock_mgp_once!($c_func_name, |_, value_ptr_ptr| unsafe {
            (*value_ptr_ptr) = alloc_mgp_value();
            mgp_error::MGP_ERROR_NO_ERROR
        });
    };
}

#[test]
#[serial]
fn test_insert_value() {
    // TODO(antaljanosbenjamin) Try to free the independently allocated types (list, map, etc)
    mock_mgp_once!(mgp_value_make_null_context, |_, value_ptr_ptr| unsafe {
        (*value_ptr_ptr) = alloc_mgp_value();
        mgp_error::MGP_ERROR_NO_ERROR
    });
    mock_mgp_value_make_with_mem!(mgp_value_make_bool_context);
    mock_mgp_value_make_with_mem!(mgp_value_make_int_context);
    mock_mgp_value_make_with_mem!(mgp_value_make_double_context);
    mock_mgp_value_make_with_mem!(mgp_value_make_string_context);

    mock_mgp_once!(mgp_list_size_context, |_, size_ptr| unsafe {
        (*size_ptr) = 0;
        mgp_error::MGP_ERROR_NO_ERROR
    });
    mock_mgp_once!(mgp_list_make_empty_context, |_, _, list_ptr_ptr| unsafe {
        (*list_ptr_ptr) = alloc_mgp_list();
        mgp_error::MGP_ERROR_NO_ERROR
    });
    mock_mgp_value_make_without_mem!(mgp_value_make_list_context);

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
    mock_mgp_value_make_without_mem!(mgp_value_make_map_context);
    mock_mgp_once!(mgp_map_items_iterator_destroy_context, |ptr| unsafe {
        free(ptr as *mut c_void);
    });

    mock_mgp_once!(mgp_vertex_copy_context, |_, _, vertex_ptr_ptr| unsafe {
        (*vertex_ptr_ptr) = alloc_mgp_vertex();
        mgp_error::MGP_ERROR_NO_ERROR
    });
    mock_mgp_value_make_without_mem!(mgp_value_make_vertex_context);

    mock_mgp_once!(mgp_edge_copy_context, |_, _, edge_ptr_ptr| unsafe {
        (*edge_ptr_ptr) = alloc_mgp_edge();
        mgp_error::MGP_ERROR_NO_ERROR
    });
    mock_mgp_value_make_without_mem!(mgp_value_make_edge_context);

    mock_mgp_once!(mgp_path_copy_context, |_, _, path_ptr_ptr| unsafe {
        (*path_ptr_ptr) = alloc_mgp_path();
        mgp_error::MGP_ERROR_NO_ERROR
    });
    mock_mgp_value_make_without_mem!(mgp_value_make_path_context);

    mock_mgp_once!(
        mgp_date_from_parameters_context,
        |_, _, date_ptr_ptr| unsafe {
            (*date_ptr_ptr) = alloc_mgp_date();
            mgp_error::MGP_ERROR_NO_ERROR
        }
    );
    mock_mgp_value_make_without_mem!(mgp_value_make_date_context);

    mock_mgp_once!(
        mgp_local_time_from_parameters_context,
        |_, _, local_time_ptr_ptr| unsafe {
            (*local_time_ptr_ptr) = alloc_mgp_local_time();
            mgp_error::MGP_ERROR_NO_ERROR
        }
    );
    mock_mgp_value_make_without_mem!(mgp_value_make_local_time_context);

    mock_mgp_once!(
        mgp_local_date_time_from_parameters_context,
        |_, _, local_date_time_ptr_ptr| unsafe {
            (*local_date_time_ptr_ptr) = alloc_mgp_local_date_time();
            mgp_error::MGP_ERROR_NO_ERROR
        }
    );
    mock_mgp_value_make_without_mem!(mgp_value_make_local_date_time_context);

    mock_mgp_once!(
        mgp_duration_from_microseconds_context,
        |_, _, duration_ptr_ptr| unsafe {
            (*duration_ptr_ptr) = alloc_mgp_duration();
            mgp_error::MGP_ERROR_NO_ERROR
        }
    );
    mock_mgp_value_make_without_mem!(mgp_value_make_duration_context);

    mock_mgp_once!(mgp_result_new_record_context, |_, record_ptr_ptr| unsafe {
        (*record_ptr_ptr) = alloc_mgp_result_record();
        mgp_error::MGP_ERROR_NO_ERROR
    });
    let ctx_insert = mgp_result_record_insert_context();
    ctx_insert
        .expect()
        .times(14)
        .returning(|_, _, _| mgp_error::MGP_ERROR_UNABLE_TO_ALLOCATE);
    let ctx_destroy = mgp_value_destroy_context();
    ctx_destroy.expect().times(14).returning(|ptr| unsafe {
        free(ptr as *mut c_void);
    });

    with_dummy!(|memgraph: &Memgraph| {
        let result_record = ResultRecord::create(&memgraph).unwrap();
        assert_eq!(
            result_record.insert_null(c_str!("field")).err().unwrap(),
            Error::UnableToInsertResultValue
        );
        assert_eq!(
            result_record
                .insert_bool(c_str!("field"), true)
                .err()
                .unwrap(),
            Error::UnableToInsertResultValue
        );
        assert_eq!(
            result_record.insert_int(c_str!("field"), 1).err().unwrap(),
            Error::UnableToInsertResultValue
        );
        assert_eq!(
            result_record
                .insert_double(c_str!("field"), 0.1)
                .err()
                .unwrap(),
            Error::UnableToInsertResultValue
        );
        assert_eq!(
            result_record
                .insert_string(c_str!("field"), c_str!("string"))
                .err()
                .unwrap(),
            Error::UnableToInsertResultValue
        );
        let list = List::new(null_mut(), &memgraph);
        assert_eq!(
            result_record
                .insert_list(c_str!("field"), &list)
                .err()
                .unwrap(),
            Error::UnableToInsertResultValue
        );
        let map = Map::new(null_mut(), &memgraph);
        assert_eq!(
            result_record
                .insert_map(c_str!("field"), &map)
                .err()
                .unwrap(),
            Error::UnableToInsertResultValue
        );
        let vertex = Vertex::new(null_mut(), &memgraph);
        assert_eq!(
            result_record
                .insert_vertex(c_str!("field"), &vertex)
                .err()
                .unwrap(),
            Error::UnableToInsertResultValue
        );
        let edge = Edge::new(null_mut(), &memgraph);
        assert_eq!(
            result_record
                .insert_edge(c_str!("field"), &edge)
                .err()
                .unwrap(),
            Error::UnableToInsertResultValue
        );
        let path = Path::new(null_mut(), &memgraph);
        assert_eq!(
            result_record
                .insert_path(c_str!("field"), &path)
                .err()
                .unwrap(),
            Error::UnableToInsertResultValue
        );

        let naive_date = chrono::NaiveDate::from_num_days_from_ce(0);
        assert_eq!(
            result_record
                .insert_date(c_str!("field"), &naive_date)
                .err()
                .unwrap(),
            Error::UnableToInsertResultValue
        );
        let naive_time = chrono::NaiveTime::from_num_seconds_from_midnight(0, 0);
        assert_eq!(
            result_record
                .insert_local_time(c_str!("field"), &naive_time)
                .err()
                .unwrap(),
            Error::UnableToInsertResultValue
        );
        let naive_date_time = chrono::NaiveDateTime::from_timestamp(0, 0);
        assert_eq!(
            result_record
                .insert_local_date_time(c_str!("field"), &naive_date_time)
                .err()
                .unwrap(),
            Error::UnableToInsertResultValue
        );
        let duration = chrono::Duration::microseconds(0);
        assert_eq!(
            result_record
                .insert_duration(c_str!("field"), &duration)
                .err()
                .unwrap(),
            Error::UnableToInsertResultValue
        );
    });
}
