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
//! All related to the list datatype.

use crate::memgraph::*;
use crate::mgp::*;
use crate::result::*;
use crate::value::*;
// Required here, if not present tests linking fails.
#[double]
use crate::mgp::ffi;
use mockall_double::double;

// NOTE: Not possible to implement [std::iter::IntoIterator] because the [ListIterator] holds the
// [List] reference which needs the lifetime specifier.
pub struct List {
    ptr: *mut mgp_list,
    memgraph: Memgraph,
}

impl Drop for List {
    fn drop(&mut self) {
        unsafe {
            if !self.ptr.is_null() {
                ffi::mgp_list_destroy(self.ptr);
            }
        }
    }
}

pub struct ListIterator<'a> {
    list: &'a List,
    position: u64,
}

impl<'a> Iterator for ListIterator<'a> {
    type Item = Value;

    fn next(&mut self) -> Option<Value> {
        if self.position >= self.list.size() {
            return None;
        }
        let value = match self.list.value_at(self.position) {
            Ok(v) => v,
            Err(_) => panic!("Unable to access the next list value."),
        };
        self.position += 1;
        Some(value)
    }
}

impl List {
    pub(crate) fn new(ptr: *mut mgp_list, memgraph: &Memgraph) -> List {
        #[cfg(not(test))]
        assert!(
            !ptr.is_null(),
            "Unable to create list because the given pointer is null."
        );

        List {
            ptr,
            memgraph: memgraph.clone(),
        }
    }

    pub fn make_empty(capacity: u64, memgraph: &Memgraph) -> Result<List> {
        unsafe {
            let mgp_ptr = invoke_mgp_func_with_res!(
                *mut mgp_list,
                Error::UnableToCreateEmptyList,
                ffi::mgp_list_make_empty,
                capacity,
                memgraph.memory_ptr()
            )?;
            Ok(List::new(mgp_ptr, &memgraph))
        }
    }

    /// Creates a new List based on [mgp_list].
    pub(crate) unsafe fn mgp_copy(ptr: *mut mgp_list, memgraph: &Memgraph) -> Result<List> {
        #[cfg(not(test))]
        assert!(
            !ptr.is_null(),
            "Unable to create list copy because the given pointer is null."
        );

        let size = invoke_mgp_func!(u64, ffi::mgp_list_size, ptr).unwrap();
        let mgp_copy = List::make_empty(size, memgraph)?;
        for index in 0..size {
            let mgp_value = invoke_mgp_func!(*mut mgp_value, ffi::mgp_list_at, ptr, index).unwrap();
            invoke_void_mgp_func_with_res!(
                Error::UnableToCopyList,
                ffi::mgp_list_append,
                mgp_copy.ptr,
                mgp_value
            )?
        }
        Ok(mgp_copy)
    }

    pub fn copy(&self) -> Result<List> {
        unsafe { List::mgp_copy(self.ptr, &self.memgraph) }
    }

    /// Appends value to the list, but if there is no place, returns an error.
    pub fn append(&self, value: &Value) -> Result<()> {
        unsafe {
            let mgp_value = value.to_mgp_value(&self.memgraph)?;
            invoke_void_mgp_func_with_res!(
                Error::UnableToAppendListValue,
                ffi::mgp_list_append,
                self.ptr,
                mgp_value.mgp_ptr()
            )?;
            Ok(())
        }
    }

    /// In case of a capacity change, the previously contained elements will move in
    /// memory and any references to them will be invalid.
    pub fn append_extend(&self, value: &Value) -> Result<()> {
        unsafe {
            let mgp_value = value.to_mgp_value(&self.memgraph)?;
            invoke_void_mgp_func_with_res!(
                Error::UnableToAppendExtendListValue,
                ffi::mgp_list_append_extend,
                self.ptr,
                mgp_value.mgp_ptr()
            )?;
            Ok(())
        }
    }

    pub fn size(&self) -> u64 {
        unsafe { invoke_mgp_func!(u64, ffi::mgp_list_size, self.ptr).unwrap() }
    }

    pub fn capacity(&self) -> u64 {
        unsafe { invoke_mgp_func!(u64, ffi::mgp_list_capacity, self.ptr).unwrap() }
    }

    /// Always copies the underlying value because in case of the capacity change any references
    /// would become invalid.
    pub fn value_at(&self, index: u64) -> Result<Value> {
        unsafe {
            let c_value = invoke_mgp_func_with_res!(
                *mut mgp_value,
                Error::UnableToAccessListValueByIndex,
                ffi::mgp_list_at,
                self.ptr,
                index
            )?;
            mgp_raw_value_to_value(c_value, &self.memgraph)
        }
    }

    pub fn iter(&self) -> Result<ListIterator> {
        Ok(ListIterator {
            list: self,
            position: 0,
        })
    }

    pub(crate) fn mgp_ptr(&self) -> *mut mgp_list {
        self.ptr
    }

    pub(crate) fn set_mgp_ptr(&mut self, new_ptr: *mut mgp_list) {
        self.ptr = new_ptr;
    }
}

#[cfg(test)]
mod tests;
