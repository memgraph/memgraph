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
//! All related to a graph path.

use crate::edge::*;
use crate::memgraph::*;
use crate::mgp::*;
use crate::result::*;
use crate::vertex::*;
// Required here, if not present tests linking fails.
#[double]
use crate::mgp::ffi;
use mockall_double::double;

pub struct Path {
    ptr: *mut mgp_path,
    memgraph: Memgraph,
}
impl Drop for Path {
    fn drop(&mut self) {
        unsafe {
            if !self.ptr.is_null() {
                ffi::mgp_path_destroy(self.ptr);
            }
        }
    }
}

impl Path {
    pub(crate) fn new(ptr: *mut mgp_path, memgraph: &Memgraph) -> Path {
        #[cfg(not(test))]
        assert!(
            !ptr.is_null(),
            "Unable to create path because the given pointer is null."
        );

        Path {
            ptr,
            memgraph: memgraph.clone(),
        }
    }

    /// Creates a new Path based on [mgp_path].
    pub(crate) unsafe fn mgp_copy(mgp_path: *mut mgp_path, memgraph: &Memgraph) -> Result<Path> {
        #[cfg(not(test))]
        assert!(
            !mgp_path.is_null(),
            "Unable to make path copy because the given pointer is null."
        );

        let mgp_copy = invoke_mgp_func_with_res!(
            *mut mgp_path,
            Error::UnableToCopyPath,
            ffi::mgp_path_copy,
            mgp_path,
            memgraph.memory_ptr()
        )?;
        Ok(Path::new(mgp_copy, &memgraph))
    }

    /// Returns the underlying [mgp_path] pointer.
    pub(crate) fn mgp_ptr(&self) -> *mut mgp_path {
        self.ptr
    }
    pub(crate) fn set_mgp_ptr(&mut self, new_ptr: *mut mgp_path) {
        self.ptr = new_ptr;
    }

    pub fn size(&self) -> u64 {
        unsafe { invoke_mgp_func!(u64, ffi::mgp_path_size, self.ptr).unwrap() }
    }

    /// Makes a new [Path] based on the starting [Vertex] object.
    pub fn make_with_start(vertex: &Vertex, memgraph: &Memgraph) -> Result<Path> {
        unsafe {
            let mgp_path = invoke_mgp_func_with_res!(
                *mut mgp_path,
                Error::UnableToCreatePathWithStartVertex,
                ffi::mgp_path_make_with_start,
                vertex.mgp_ptr(),
                memgraph.memory_ptr()
            )?;
            Ok(Path::new(mgp_path, &memgraph))
        }
    }

    /// Fails if the current last vertex in the path is not part of the given edge or if there is
    /// no memory to expand the path.
    pub fn expand(&self, edge: &Edge) -> Result<()> {
        unsafe {
            invoke_void_mgp_func_with_res!(
                Error::UnableToExpandPath,
                ffi::mgp_path_expand,
                self.ptr,
                edge.mgp_ptr()
            )?;
            Ok(())
        }
    }

    pub fn vertex_at(&self, index: u64) -> Result<Vertex> {
        unsafe {
            let mgp_vertex = invoke_mgp_func_with_res!(
                *mut mgp_vertex,
                Error::OutOfBoundPathVertexIndex,
                ffi::mgp_path_vertex_at,
                self.ptr,
                index
            )?;
            Vertex::mgp_copy(mgp_vertex, &self.memgraph)
        }
    }

    pub fn edge_at(&self, index: u64) -> Result<Edge> {
        unsafe {
            let mgp_edge = invoke_mgp_func_with_res!(
                *mut mgp_edge,
                Error::OutOfBoundPathVertexIndex,
                ffi::mgp_path_edge_at,
                self.ptr,
                index
            )?;
            Edge::mgp_copy(mgp_edge, &self.memgraph)
        }
    }
}

#[cfg(test)]
mod tests;
