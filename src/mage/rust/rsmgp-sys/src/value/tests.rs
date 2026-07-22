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
use std::ffi::CStr;
use std::os::raw::c_char;
use std::ptr::null_mut;

use super::*;
use crate::memgraph::Memgraph;
use crate::mgp::mock_ffi::*;
use crate::{mock_mgp_once, with_dummy};

#[test]
#[serial]
fn test_make_null_mgp_value() {
    mock_mgp_once!(mgp_value_make_null_context, |_, _| {
        mgp_error::MGP_ERROR_UNABLE_TO_ALLOCATE
    });

    with_dummy!(|memgraph: &Memgraph| {
        let value = MgpValue::make_null(&memgraph);
        assert!(value.is_err());
    });
}

#[test]
#[serial]
fn test_make_false_bool_mgp_value() {
    mock_mgp_once!(mgp_value_make_bool_context, |value, _, _| {
        assert_eq!(value, 0);
        mgp_error::MGP_ERROR_UNABLE_TO_ALLOCATE
    });

    with_dummy!(|memgraph: &Memgraph| {
        let value = MgpValue::make_bool(false, &memgraph);
        assert!(value.is_err());
    });
}

#[test]
#[serial]
fn test_convert_false_bool_mgp_value() {
    mock_mgp_once!(mgp_value_get_bool_context, |_, out| {
        unsafe {
            *out = 0;
        }
        mgp_error::MGP_ERROR_NO_ERROR
    });
    mock_mgp_once!(mgp_value_get_type_context, |_, out| {
        unsafe {
            *out = mgp_value_type::MGP_VALUE_TYPE_BOOL;
        }
        mgp_error::MGP_ERROR_NO_ERROR
    });

    with_dummy!(|memgraph: &Memgraph| {
        unsafe {
            let res = mgp_raw_value_to_value(null_mut(), &memgraph);
            match res {
                Ok(value) => match value {
                    Value::Bool(value) => assert!(value == false, "Wrong boolean value"),
                    _ => {
                        assert!(false, "Value is not a Bool")
                    }
                },
                _ => {
                    assert!(false, "Failed to convert raw value")
                }
            }
        }
    });
}

#[test]
#[serial]
fn test_make_true_bool_mgp_value() {
    mock_mgp_once!(mgp_value_make_bool_context, |value, _, _| {
        assert_eq!(value, 1);
        mgp_error::MGP_ERROR_UNABLE_TO_ALLOCATE
    });

    with_dummy!(|memgraph: &Memgraph| {
        let value = MgpValue::make_bool(true, &memgraph);
        assert!(value.is_err());
    });
}

#[test]
#[serial]
fn test_convert_true_bool_mgp_value() {
    mock_mgp_once!(mgp_value_get_bool_context, |_, out| {
        unsafe {
            *out = 1;
        }
        mgp_error::MGP_ERROR_NO_ERROR
    });
    mock_mgp_once!(mgp_value_get_type_context, |_, out| {
        unsafe {
            *out = mgp_value_type::MGP_VALUE_TYPE_BOOL;
        }
        mgp_error::MGP_ERROR_NO_ERROR
    });

    with_dummy!(|memgraph: &Memgraph| {
        unsafe {
            let res = mgp_raw_value_to_value(null_mut(), &memgraph);
            match res {
                Ok(value) => match value {
                    Value::Bool(value) => assert!(value == true, "Wrong boolean value"),
                    _ => {
                        assert!(false, "Value is not a Bool")
                    }
                },
                _ => {
                    assert!(false, "Failed to convert raw value")
                }
            }
        }
    });
}

#[test]
#[serial]
fn test_make_int_mgp_value() {
    mock_mgp_once!(mgp_value_make_int_context, |value, _, _| {
        assert_eq!(value, 100);
        mgp_error::MGP_ERROR_UNABLE_TO_ALLOCATE
    });

    with_dummy!(|memgraph: &Memgraph| {
        let value = MgpValue::make_int(100, &memgraph);
        assert!(value.is_err());
    });
}

#[test]
#[serial]
fn test_convert_int_mgp_value() {
    mock_mgp_once!(mgp_value_get_int_context, |_, out| {
        unsafe {
            *out = 123;
        }
        mgp_error::MGP_ERROR_NO_ERROR
    });
    mock_mgp_once!(mgp_value_get_type_context, |_, out| {
        unsafe {
            *out = mgp_value_type::MGP_VALUE_TYPE_INT;
        }
        mgp_error::MGP_ERROR_NO_ERROR
    });

    with_dummy!(|memgraph: &Memgraph| {
        unsafe {
            let res = mgp_raw_value_to_value(null_mut(), &memgraph);
            match res {
                Ok(value) => match value {
                    Value::Int(value) => assert!(value == 123, "Wrong integer value"),
                    _ => {
                        assert!(false, "Value is not an Int")
                    }
                },
                _ => {
                    assert!(false, "Failed to convert raw value")
                }
            }
        }
    });
}

#[test]
#[serial]
fn test_make_double_mgp_value() {
    mock_mgp_once!(mgp_value_make_double_context, |value, _, _| {
        assert_eq!(value, 0.0);
        mgp_error::MGP_ERROR_UNABLE_TO_ALLOCATE
    });

    with_dummy!(|memgraph: &Memgraph| {
        let value = MgpValue::make_double(0.0, &memgraph);
        assert!(value.is_err());
    });
}

#[test]
#[serial]
fn test_convert_double_mgp_value() {
    mock_mgp_once!(mgp_value_get_double_context, |_, out| {
        unsafe {
            *out = 1.23;
        }
        mgp_error::MGP_ERROR_NO_ERROR
    });
    mock_mgp_once!(mgp_value_get_type_context, |_, out| {
        unsafe {
            *out = mgp_value_type::MGP_VALUE_TYPE_DOUBLE;
        }
        mgp_error::MGP_ERROR_NO_ERROR
    });

    with_dummy!(|memgraph: &Memgraph| {
        unsafe {
            let res = mgp_raw_value_to_value(null_mut(), &memgraph);
            match res {
                Ok(value) => match value {
                    Value::Float(value) => assert!(value == 1.23, "Wrong double value"),
                    _ => {
                        assert!(false, "Value is not a Double")
                    }
                },
                _ => {
                    assert!(false, "Failed to convert raw value")
                }
            }
        }
    });
}

#[test]
#[serial]
fn test_make_string_mgp_value() {
    mock_mgp_once!(mgp_value_make_string_context, |value, _, _| unsafe {
        assert_eq!(CStr::from_ptr(value), c_str!("test"));
        mgp_error::MGP_ERROR_UNABLE_TO_ALLOCATE
    });

    with_dummy!(|memgraph: &Memgraph| {
        let value = MgpValue::make_string(c_str!("test"), &memgraph);
        assert!(value.is_err());
    });
}

#[test]
#[serial]
fn test_convert_string_mgp_value() {
    let cstr = CString::new("a string").expect("CString::new failed");
    mock_mgp_once!(mgp_value_get_string_context, move |_, out| {
        unsafe {
            *out = cstr.as_ptr() as *const c_char;
        }
        mgp_error::MGP_ERROR_NO_ERROR
    });
    mock_mgp_once!(mgp_value_get_type_context, |_, out| {
        unsafe {
            *out = mgp_value_type::MGP_VALUE_TYPE_STRING;
        }
        mgp_error::MGP_ERROR_NO_ERROR
    });

    with_dummy!(|memgraph: &Memgraph| {
        unsafe {
            let res = mgp_raw_value_to_value(null_mut(), &memgraph);
            match res {
                Ok(value) => match value {
                    Value::String(value) => match value.to_str() {
                        Ok(vs) => match vs {
                            "a string" => {}
                            _ => assert!(false, "Wrong string value"),
                        },
                        _ => {
                            assert!(false, "Value is not a String")
                        }
                    },
                    _ => {
                        assert!(false, "Value is not a String")
                    }
                },
                _ => {
                    assert!(false, "Failed to convert raw value")
                }
            }
        }
    });
}

#[test]
#[serial]
fn test_make_list_mgp_value() {
    mock_mgp_once!(mgp_list_size_context, |_, size_ptr| unsafe {
        (*size_ptr) = 0;
        mgp_error::MGP_ERROR_NO_ERROR
    });
    mock_mgp_once!(mgp_list_make_empty_context, |_, _, _| {
        mgp_error::MGP_ERROR_NO_ERROR
    });
    mock_mgp_once!(mgp_value_make_list_context, |_, _| {
        mgp_error::MGP_ERROR_UNABLE_TO_ALLOCATE
    });

    with_dummy!(|memgraph: &Memgraph| {
        let list = List::new(null_mut(), &memgraph);
        let value = MgpValue::make_list(&list, &memgraph);
        assert!(value.is_err());
    });
}

#[test]
#[serial]
fn test_make_map_mgp_value() {
    mock_mgp_once!(mgp_map_make_empty_context, |_, _| {
        mgp_error::MGP_ERROR_NO_ERROR
    });
    mock_mgp_once!(mgp_map_iter_items_context, |_, _, _| {
        mgp_error::MGP_ERROR_NO_ERROR
    });
    mock_mgp_once!(mgp_map_items_iterator_get_context, |_, _| {
        mgp_error::MGP_ERROR_NO_ERROR
    });
    mock_mgp_once!(mgp_value_make_map_context, |_, _| {
        mgp_error::MGP_ERROR_UNABLE_TO_ALLOCATE
    });

    with_dummy!(|memgraph: &Memgraph| {
        let map = Map::new(null_mut(), &memgraph);
        let value = MgpValue::make_map(&map, &memgraph);
        assert!(value.is_err());
    });
}

#[test]
#[serial]
fn test_make_vertex_mgp_value() {
    mock_mgp_once!(mgp_vertex_copy_context, |_, _, _| {
        mgp_error::MGP_ERROR_UNABLE_TO_ALLOCATE
    });

    with_dummy!(|memgraph: &Memgraph| {
        let vertex = Vertex::new(null_mut(), &memgraph);
        let value = MgpValue::make_vertex(&vertex, &memgraph);
        assert!(value.is_err());
    });
}

#[test]
#[serial]
fn test_make_edge_mgp_value() {
    mock_mgp_once!(mgp_edge_copy_context, |_, _, _| {
        mgp_error::MGP_ERROR_UNABLE_TO_ALLOCATE
    });

    with_dummy!(|memgraph: &Memgraph| {
        let edge = Edge::new(null_mut(), &memgraph);
        let value = MgpValue::make_edge(&edge, &memgraph);
        assert!(value.is_err());
    });
}

#[test]
#[serial]
fn test_make_path_mgp_value() {
    mock_mgp_once!(mgp_path_copy_context, |_, _, _| {
        mgp_error::MGP_ERROR_UNABLE_TO_ALLOCATE
    });

    with_dummy!(|memgraph: &Memgraph| {
        let path = Path::new(null_mut(), &memgraph);
        let value = MgpValue::make_path(&path, &memgraph);
        assert!(value.is_err());
    });
}

macro_rules! mock_mgp_value_is {
    ($c_func_name:ident, $value:expr) => {
        mock_mgp_once!(
            $c_func_name,
            |_, result: *mut ::std::os::raw::c_int| unsafe {
                (*result) = $value;
                mgp_error::MGP_ERROR_NO_ERROR
            }
        );
    };
}

#[test]
#[serial]
fn test_mgp_value_for_the_right_type() {
    mock_mgp_value_is!(mgp_value_is_null_context, 1);
    mock_mgp_value_is!(mgp_value_is_bool_context, 1);
    mock_mgp_value_is!(mgp_value_is_int_context, 1);
    mock_mgp_value_is!(mgp_value_is_double_context, 1);
    mock_mgp_value_is!(mgp_value_is_string_context, 1);
    mock_mgp_value_is!(mgp_value_is_list_context, 1);
    mock_mgp_value_is!(mgp_value_is_map_context, 1);
    mock_mgp_value_is!(mgp_value_is_vertex_context, 1);
    mock_mgp_value_is!(mgp_value_is_edge_context, 1);
    mock_mgp_value_is!(mgp_value_is_path_context, 1);

    with_dummy!(|memgraph: &Memgraph| {
        let value = MgpValue::new(null_mut(), &memgraph);
        assert!(value.is_null());
        assert!(value.is_bool());
        assert!(value.is_int());
        assert!(value.is_double());
        assert!(value.is_string());
        assert!(value.is_list());
        assert!(value.is_map());
        assert!(value.is_vertex());
        assert!(value.is_edge());
        assert!(value.is_path());
    });
}

#[test]
#[serial]
fn test_mgp_value_for_the_wrong_type() {
    mock_mgp_value_is!(mgp_value_is_null_context, 0);
    mock_mgp_value_is!(mgp_value_is_bool_context, 0);
    mock_mgp_value_is!(mgp_value_is_int_context, 0);
    mock_mgp_value_is!(mgp_value_is_double_context, 0);
    mock_mgp_value_is!(mgp_value_is_string_context, 0);
    mock_mgp_value_is!(mgp_value_is_list_context, 0);
    mock_mgp_value_is!(mgp_value_is_map_context, 0);
    mock_mgp_value_is!(mgp_value_is_vertex_context, 0);
    mock_mgp_value_is!(mgp_value_is_edge_context, 0);
    mock_mgp_value_is!(mgp_value_is_path_context, 0);

    with_dummy!(|memgraph: &Memgraph| {
        let value = MgpValue::new(null_mut(), &memgraph);
        assert!(!value.is_null());
        assert!(!value.is_bool());
        assert!(!value.is_int());
        assert!(!value.is_double());
        assert!(!value.is_string());
        assert!(!value.is_list());
        assert!(!value.is_map());
        assert!(!value.is_vertex());
        assert!(!value.is_edge());
        assert!(!value.is_path());
    });
}

#[test]
#[serial]
fn test_to_mgp_value() {
    mock_mgp_once!(mgp_value_make_null_context, |_, _| {
        mgp_error::MGP_ERROR_UNABLE_TO_ALLOCATE
    });

    with_dummy!(|memgraph: &Memgraph| {
        let value = Value::Null;
        let mgp_value = value.to_mgp_value(&memgraph);
        assert!(mgp_value.is_err());
    });
}
