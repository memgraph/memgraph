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
//! All edge (relationship) related.

use std::ffi::{CStr, CString};

use crate::memgraph::*;
use crate::mgp::*;
use crate::property::*;
use crate::result::*;
use crate::value::*;
use crate::vertex::Vertex;
// Required here, if not present tests linking fails.
#[double]
use crate::mgp::ffi;
use mockall_double::double;

pub struct EdgesIterator {
    ptr: *mut mgp_edges_iterator,
    is_first: bool,
    memgraph: Memgraph,
}

impl EdgesIterator {
    pub(crate) fn new(ptr: *mut mgp_edges_iterator, memgraph: &Memgraph) -> EdgesIterator {
        #[cfg(not(test))]
        assert!(
            !ptr.is_null(),
            "Unable to create edges iterator because the given pointer is null."
        );

        EdgesIterator {
            ptr,
            is_first: true,
            memgraph: memgraph.clone(),
        }
    }
}

impl Drop for EdgesIterator {
    fn drop(&mut self) {
        unsafe {
            if !self.ptr.is_null() {
                ffi::mgp_edges_iterator_destroy(self.ptr);
            }
        }
    }
}

impl Iterator for EdgesIterator {
    type Item = Edge;

    fn next(&mut self) -> Option<Edge> {
        unsafe {
            let data = if self.is_first {
                self.is_first = false;
                invoke_mgp_func!(*mut mgp_edge, ffi::mgp_edges_iterator_get, self.ptr).unwrap()
            } else {
                invoke_mgp_func!(*mut mgp_edge, ffi::mgp_edges_iterator_next, self.ptr)
                    .expect("Unable to get next edge during edges iteration.")
            };

            if data.is_null() {
                None
            } else {
                Some(match Edge::mgp_copy(data, &self.memgraph) {
                    Ok(v) => v,
                    Err(_) => panic!("Unable to create edge during edges iteration."),
                })
            }
        }
    }
}

pub struct Edge {
    ptr: *mut mgp_edge,
    memgraph: Memgraph,
}

impl Drop for Edge {
    fn drop(&mut self) {
        unsafe {
            if !self.ptr.is_null() {
                ffi::mgp_edge_destroy(self.ptr);
            }
        }
    }
}

impl Edge {
    pub(crate) fn new(ptr: *mut mgp_edge, memgraph: &Memgraph) -> Edge {
        #[cfg(not(test))]
        assert!(
            !ptr.is_null(),
            "Unable to create edge because the given pointer is null."
        );

        Edge {
            ptr,
            memgraph: memgraph.clone(),
        }
    }

    /// Creates a new Edge based on [mgp_edge].
    pub(crate) unsafe fn mgp_copy(ptr: *mut mgp_edge, memgraph: &Memgraph) -> Result<Edge> {
        #[cfg(not(test))]
        assert!(
            !ptr.is_null(),
            "Unable to create edge copy because the given pointer is null."
        );

        let mgp_copy = invoke_mgp_func_with_res!(
            *mut mgp_edge,
            Error::UnableToCopyEdge,
            ffi::mgp_edge_copy,
            ptr,
            memgraph.memory_ptr()
        )?;
        Ok(Edge::new(mgp_copy, &memgraph))
    }

    /// Returns the underlying [mgp_edge] pointer.
    pub(crate) fn mgp_ptr(&self) -> *mut mgp_edge {
        self.ptr
    }

    pub(crate) fn set_mgp_ptr(&mut self, new_ptr: *mut mgp_edge) {
        self.ptr = new_ptr;
    }

    pub fn copy(&self) -> Result<Edge> {
        unsafe { Edge::mgp_copy(self.ptr, &self.memgraph) }
    }

    pub fn id(&self) -> i64 {
        unsafe {
            invoke_mgp_func!(mgp_edge_id, ffi::mgp_edge_get_id, self.ptr)
                .unwrap()
                .as_int
        }
    }

    pub fn edge_type(&self) -> Result<CString> {
        unsafe {
            let mgp_edge_type = invoke_mgp_func_with_res!(
                mgp_edge_type,
                Error::UnableToCreateCString,
                ffi::mgp_edge_get_type,
                self.ptr
            )?;
            create_cstring(mgp_edge_type.name)
        }
    }

    pub fn from_vertex(&self) -> Result<Vertex> {
        unsafe {
            let mgp_vertex =
                invoke_mgp_func!(*mut mgp_vertex, ffi::mgp_edge_get_from, self.ptr).unwrap();
            Vertex::mgp_copy(mgp_vertex, &self.memgraph)
        }
    }

    pub fn to_vertex(&self) -> Result<Vertex> {
        unsafe {
            let mgp_vertex =
                invoke_mgp_func!(*mut mgp_vertex, ffi::mgp_edge_get_to, self.ptr).unwrap();
            Vertex::mgp_copy(mgp_vertex, &self.memgraph)
        }
    }

    pub fn property(&self, name: &CStr) -> Result<Property> {
        unsafe {
            let mgp_value = invoke_mgp_func!(
                *mut mgp_value,
                ffi::mgp_edge_get_property,
                self.ptr,
                name.as_ptr(),
                self.memgraph.memory_ptr()
            );
            match mgp_value {
                Ok(_) => (),
                Err(MgpError::UnableToAllocate) => {
                    return Err(Error::UnableToReturnEdgePropertyValueAllocationError)
                }
                Err(MgpError::DeletedObject) => {
                    return Err(Error::UnableToReturnEdgePropertyDeletedObjectError)
                }
                Err(_) => panic!("Unexpected error code is returned from MGP API"),
            }
            let value = match MgpValue::new(mgp_value.unwrap(), &self.memgraph).to_value() {
                Ok(v) => v,
                Err(_) => return Err(Error::UnableToReturnEdgePropertyValueCreationError),
            };
            match CString::new(name.to_bytes()) {
                Ok(c_string) => Ok(Property {
                    name: c_string,
                    value,
                }),
                Err(_) => Err(Error::UnableToReturnEdgePropertyNameAllocationError),
            }
        }
    }

    pub fn properties(&self) -> Result<PropertiesIterator> {
        unsafe {
            let mgp_iterator = invoke_mgp_func_with_res!(
                *mut mgp_properties_iterator,
                Error::UnableToReturnEdgePropertiesIterator,
                ffi::mgp_edge_iter_properties,
                self.ptr,
                self.memgraph.memory_ptr()
            )?;
            Ok(PropertiesIterator::new(mgp_iterator, &self.memgraph))
        }
    }
}

#[cfg(test)]
mod tests;
