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
//! All related to the property graph property (data key-value pair).

use std::ffi::CString;

use crate::memgraph::*;
use crate::mgp::*;
use crate::value::*;
// Required here, if not present tests linking fails.
#[double]
use crate::mgp::ffi;
use mockall_double::double;

/// All related to the property graph property (data key-value pair).
///
/// Property is used in the following contexts:
///   * return Property from [PropertiesIterator]
///   * return Property directly from [crate::vertex::Vertex] or [crate::edge::Edge].
///
/// Property owns [CString] and [Value] because the underlying C string or value could be deleted
/// during the lifetime of the property. In other words, Property stores copies of underlying name
/// and value.
pub struct Property {
    pub name: CString,
    pub value: Value,
}

pub struct PropertiesIterator {
    ptr: *mut mgp_properties_iterator,
    is_first: bool,
    memgraph: Memgraph,
}

impl PropertiesIterator {
    pub(crate) fn new(
        ptr: *mut mgp_properties_iterator,
        memgraph: &Memgraph,
    ) -> PropertiesIterator {
        #[cfg(not(test))]
        assert!(
            !ptr.is_null(),
            "Unable to create properties iterator because the given pointer is null."
        );

        PropertiesIterator {
            ptr,
            is_first: true,
            memgraph: memgraph.clone(),
        }
    }
}

impl Drop for PropertiesIterator {
    fn drop(&mut self) {
        unsafe {
            if !self.ptr.is_null() {
                ffi::mgp_properties_iterator_destroy(self.ptr);
            }
        }
    }
}

impl Iterator for PropertiesIterator {
    type Item = Property;

    fn next(&mut self) -> Option<Property> {
        unsafe {
            let data = if self.is_first {
                self.is_first = false;
                invoke_mgp_func!(
                    *mut mgp_property,
                    ffi::mgp_properties_iterator_get,
                    self.ptr
                )
                .unwrap()
            } else {
                invoke_mgp_func!(
                    *mut mgp_property,
                    ffi::mgp_properties_iterator_next,
                    self.ptr
                )
                .expect("Unable to provide next property. Iteration problem.")
            };

            if data.is_null() {
                None
            } else {
                // Unwrap/panic is I think the only option here because if something fails the
                // whole procedure should be stopped. Returning empty Option is not an option
                // because it's not correct. The same applies in case of the property value.
                let data_ref = data.as_ref().unwrap();
                Some(Property {
                    name: match create_cstring(data_ref.name) {
                        Ok(v) => v,
                        Err(_) => panic!("Unable to provide next property. Name creation problem."),
                    },
                    value: match mgp_raw_value_to_value(data_ref.value, &self.memgraph) {
                        Ok(v) => v,
                        Err(_) => panic!("Unable to provide next property. Value create problem."),
                    },
                })
            }
        }
    }
}

#[cfg(test)]
mod tests;
