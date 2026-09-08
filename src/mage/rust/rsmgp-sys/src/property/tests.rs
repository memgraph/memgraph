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
use std::ptr::null_mut;

use super::*;
use crate::mgp::mock_ffi::*;
use crate::{mock_mgp_once, with_dummy};

#[test]
#[serial]
fn test_properties_iterator() {
    let property_getter = |_, prop_ptr_ptr: *mut *mut mgp_property| unsafe {
        (*prop_ptr_ptr) = null_mut();
        mgp_error::MGP_ERROR_NO_ERROR
    };
    mock_mgp_once!(mgp_properties_iterator_get_context, property_getter);
    mock_mgp_once!(mgp_properties_iterator_next_context, property_getter);

    with_dummy!(|memgraph: &Memgraph| {
        let mut iterator = PropertiesIterator::new(null_mut(), &memgraph);

        let value_1 = iterator.next();
        assert!(value_1.is_none());

        let value_2 = iterator.next();
        assert!(value_2.is_none());
    });
}
