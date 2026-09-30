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

#[cfg(test)]
pub mod alloc {
    use libc::malloc;
    use std::mem::size_of;

    use crate::mgp::*;

    pub(crate) unsafe fn alloc_mgp_type() -> *mut mgp_type {
        malloc(size_of::<mgp_type>()) as *mut mgp_type
    }

    pub(crate) unsafe fn alloc_mgp_value() -> *mut mgp_value {
        malloc(size_of::<mgp_value>()) as *mut mgp_value
    }

    pub(crate) unsafe fn alloc_mgp_list() -> *mut mgp_list {
        malloc(size_of::<mgp_list>()) as *mut mgp_list
    }

    pub(crate) unsafe fn alloc_mgp_map() -> *mut mgp_map {
        malloc(size_of::<mgp_map>()) as *mut mgp_map
    }

    pub(crate) unsafe fn alloc_mgp_map_items_iterator() -> *mut mgp_map_items_iterator {
        malloc(size_of::<mgp_map_items_iterator>()) as *mut mgp_map_items_iterator
    }

    pub(crate) unsafe fn alloc_mgp_vertex() -> *mut mgp_vertex {
        malloc(size_of::<mgp_vertex>()) as *mut mgp_vertex
    }

    pub(crate) unsafe fn alloc_mgp_edge() -> *mut mgp_edge {
        malloc(size_of::<mgp_edge>()) as *mut mgp_edge
    }

    pub(crate) unsafe fn alloc_mgp_path() -> *mut mgp_path {
        malloc(size_of::<mgp_path>()) as *mut mgp_path
    }

    pub(crate) unsafe fn alloc_mgp_date() -> *mut mgp_date {
        malloc(size_of::<mgp_date>()) as *mut mgp_date
    }

    pub(crate) unsafe fn alloc_mgp_local_time() -> *mut mgp_local_time {
        malloc(size_of::<mgp_local_time>()) as *mut mgp_local_time
    }

    pub(crate) unsafe fn alloc_mgp_local_date_time() -> *mut mgp_local_date_time {
        malloc(size_of::<mgp_local_date_time>()) as *mut mgp_local_date_time
    }

    pub(crate) unsafe fn alloc_mgp_duration() -> *mut mgp_duration {
        malloc(size_of::<mgp_duration>()) as *mut mgp_duration
    }

    pub(crate) unsafe fn alloc_mgp_proc() -> *mut mgp_proc {
        malloc(size_of::<mgp_proc>()) as *mut mgp_proc
    }

    pub(crate) unsafe fn alloc_mgp_result_record() -> *mut mgp_result_record {
        malloc(size_of::<mgp_result_record>()) as *mut mgp_result_record
    }

    #[macro_export]
    macro_rules! mock_mgp_once {
        ($c_func_name:ident, $rs_return_func:expr) => {
            let $c_func_name = $c_func_name();
            $c_func_name.expect().times(1).returning($rs_return_func);
        };
    }

    #[macro_export]
    macro_rules! with_dummy {
        ($rs_test_func:expr) => {
            let memgraph = Memgraph::new_default();
            $rs_test_func(&memgraph);
        };

        (List, $rs_test_func:expr) => {
            let memgraph = Memgraph::new_default();
            let list = List::new(std::ptr::null_mut(), &memgraph);
            $rs_test_func(&list);
        };

        (Map, $rs_test_func:expr) => {
            let memgraph = Memgraph::new_default();
            let map = Map::new(std::ptr::null_mut(), &memgraph);
            $rs_test_func(&map);
        };

        (Vertex, $rs_test_func:expr) => {
            let memgraph = Memgraph::new_default();
            let vertex = Vertex::new(std::ptr::null_mut(), &memgraph);
            $rs_test_func(&vertex);
        };

        (Edge, $rs_test_func:expr) => {
            let memgraph = Memgraph::new_default();
            let edge = Edge::new(std::ptr::null_mut(), &memgraph);
            $rs_test_func(&edge);
        };

        (Path, $rs_test_func:expr) => {
            let memgraph = Memgraph::new_default();
            let path = Path::new(std::ptr::null_mut(), &memgraph);
            $rs_test_func(&path);
        };

        (Date, $rs_test_func:expr) => {
            let date = Date::new(std::ptr::null_mut());
            $rs_test_func(&date);
        };

        (LocalTime, $rs_test_func:expr) => {
            let local_time = LocalTime::new(std::ptr::null_mut());
            $rs_test_func(&local_time);
        };

        (LocalDateTime, $rs_test_func:expr) => {
            let local_date_time = LocalDateTime::new(std::ptr::null_mut());
            $rs_test_func(&local_date_time);
        };

        (Duration, $rs_test_func:expr) => {
            let duration = Duration::new(std::ptr::null_mut());
            $rs_test_func(&duration);
        };
    }
}
