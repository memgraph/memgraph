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
//! All related to the map (dictionary) datatype.

use std::ffi::{CStr, CString};

use crate::memgraph::*;
use crate::mgp::*;
use crate::result::*;
use crate::value::*;
// Required here, if not present tests linking fails.
#[double]
use crate::mgp::ffi;
use mockall_double::double;

pub struct Map {
    ptr: *mut mgp_map,
    memgraph: Memgraph,
}

impl Drop for Map {
    fn drop(&mut self) {
        unsafe {
            if !self.ptr.is_null() {
                ffi::mgp_map_destroy(self.ptr);
            }
        }
    }
}

/// [MapItem] has public fields because they are user facing object + they are easier to access.
pub struct MapItem {
    pub key: CString,
    pub value: Value,
}

pub struct MapIterator {
    ptr: *mut mgp_map_items_iterator,
    is_first: bool,
    memgraph: Memgraph,
}

impl MapIterator {
    pub(crate) fn new(ptr: *mut mgp_map_items_iterator, memgraph: &Memgraph) -> MapIterator {
        MapIterator {
            ptr,
            is_first: true,
            memgraph: memgraph.clone(),
        }
    }
}

impl Drop for MapIterator {
    fn drop(&mut self) {
        unsafe {
            if !self.ptr.is_null() {
                ffi::mgp_map_items_iterator_destroy(self.ptr);
            }
        }
    }
}

impl Iterator for MapIterator {
    type Item = MapItem;

    fn next(&mut self) -> Option<MapItem> {
        unsafe {
            let data = if self.is_first {
                self.is_first = false;
                invoke_mgp_func!(*mut mgp_map_item, ffi::mgp_map_items_iterator_get, self.ptr)
                    .unwrap()
            } else {
                invoke_mgp_func!(
                    *mut mgp_map_item,
                    ffi::mgp_map_items_iterator_next,
                    self.ptr
                )
                .unwrap()
            };

            if data.is_null() {
                None
            } else {
                let mgp_map_item_key =
                    invoke_mgp_func!(*const ::std::os::raw::c_char, ffi::mgp_map_item_key, data)
                        .unwrap();
                let mgp_map_item_value =
                    invoke_mgp_func!(*mut mgp_value, ffi::mgp_map_item_value, data).unwrap();
                let key = match create_cstring(mgp_map_item_key) {
                    Ok(v) => v,
                    Err(_) => panic!("Unable to create map item key."),
                };
                let value = match mgp_raw_value_to_value(mgp_map_item_value, &self.memgraph) {
                    Ok(v) => v,
                    Err(_) => panic!("Unable to create map item value."),
                };
                Some(MapItem { key, value })
            }
        }
    }
}

impl Map {
    pub(crate) fn new(ptr: *mut mgp_map, memgraph: &Memgraph) -> Map {
        #[cfg(not(test))]
        assert!(
            !ptr.is_null(),
            "Unable to create map because the given pointer is null."
        );

        Map {
            ptr,
            memgraph: memgraph.clone(),
        }
    }

    pub(crate) unsafe fn mgp_copy(ptr: *mut mgp_map, memgraph: &Memgraph) -> Result<Map> {
        #[cfg(not(test))]
        assert!(
            !ptr.is_null(),
            "Unable to create map copy because the given pointer is null."
        );

        let mgp_map_copy = Map::new(
            invoke_mgp_func_with_res!(
                *mut mgp_map,
                Error::UnableToCopyMap,
                ffi::mgp_map_make_empty,
                memgraph.memory_ptr()
            )?,
            &memgraph,
        );
        let map_iterator = MapIterator::new(
            invoke_mgp_func_with_res!(
                *mut mgp_map_items_iterator,
                Error::UnableToCopyMap,
                ffi::mgp_map_iter_items,
                ptr,
                memgraph.memory_ptr()
            )?,
            &memgraph,
        );
        for item in map_iterator {
            let mgp_value = item.value.to_mgp_value(&memgraph)?;
            invoke_void_mgp_func_with_res!(
                Error::UnableToCopyMap,
                ffi::mgp_map_insert,
                mgp_map_copy.ptr,
                item.key.as_ptr(),
                mgp_value.mgp_ptr()
            )?
        }
        Ok(mgp_map_copy)
    }

    pub fn make_empty(memgraph: &Memgraph) -> Result<Map> {
        unsafe {
            Ok(Map::new(
                invoke_mgp_func_with_res!(
                    *mut mgp_map,
                    Error::UnableToCreateEmptyMap,
                    ffi::mgp_map_make_empty,
                    memgraph.memory_ptr()
                )?,
                &memgraph,
            ))
        }
    }

    pub fn insert(&self, key: &CStr, value: &Value) -> Result<()> {
        unsafe {
            let mgp_value = value.to_mgp_value(&self.memgraph)?;
            invoke_void_mgp_func_with_res!(
                Error::UnableToInsertMapValue,
                ffi::mgp_map_insert,
                self.ptr,
                key.as_ptr(),
                mgp_value.mgp_ptr()
            )?;

            Ok(())
        }
    }

    pub fn size(&self) -> u64 {
        unsafe { invoke_mgp_func!(u64, ffi::mgp_map_size, self.ptr).unwrap() }
    }

    pub fn at(&self, key: &CStr) -> Result<Value> {
        unsafe {
            let result = invoke_mgp_func_with_res!(
                *mut mgp_value,
                Error::UnableToAccessMapValue,
                ffi::mgp_map_at,
                self.ptr,
                key.as_ptr()
            )?;
            if result.is_null() {
                Err(Error::UnableToAccessMapValue)
            } else {
                mgp_raw_value_to_value(result, &self.memgraph)
            }
        }
    }

    pub fn iter(&self) -> Result<MapIterator> {
        unsafe {
            let mgp_iterator = invoke_mgp_func_with_res!(
                *mut mgp_map_items_iterator,
                Error::UnableToCreateMapIterator,
                ffi::mgp_map_iter_items,
                self.ptr,
                self.memgraph.memory_ptr()
            )?;
            Ok(MapIterator::new(mgp_iterator, &self.memgraph))
        }
    }

    pub(crate) fn mgp_ptr(&self) -> *mut mgp_map {
        self.ptr
    }

    pub(crate) fn set_mgp_ptr(&mut self, new_ptr: *mut mgp_map) {
        self.ptr = new_ptr;
    }
}

#[cfg(test)]
mod tests;
