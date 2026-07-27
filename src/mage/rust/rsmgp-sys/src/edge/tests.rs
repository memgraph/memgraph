use c_str_macro::c_str;
use serial_test::serial;
use std::ptr::null_mut;

use super::*;
use crate::memgraph::Memgraph;
use crate::mgp::mgp_error;
use crate::mgp::mock_ffi::*;
use crate::{mock_mgp_once, with_dummy};

#[test]
#[serial]
fn test_mgp_copy() {
    mock_mgp_once!(mgp_edge_copy_context, |_, _, _| {
        mgp_error::MGP_ERROR_UNABLE_TO_ALLOCATE
    });

    with_dummy!(|memgraph: &Memgraph| {
        unsafe {
            let value = Edge::mgp_copy(null_mut(), &memgraph);
            assert!(value.is_err());
        }
    });
}

#[test]
#[serial]
fn test_id() {
    mock_mgp_once!(mgp_edge_get_id_context, |_, edge_id_ptr| unsafe {
        (*edge_id_ptr).as_int = 1;
        mgp_error::MGP_ERROR_NO_ERROR
    });

    with_dummy!(Edge, |edge: &Edge| {
        assert_eq!(edge.id(), 1);
    });
}

#[test]
#[serial]
fn test_edge_type() {
    let edge_type = CString::new("type").unwrap();
    mock_mgp_once!(mgp_edge_get_type_context, move |_, edge_type_ptr| unsafe {
        (*edge_type_ptr).name = edge_type.as_ptr();
        mgp_error::MGP_ERROR_NO_ERROR
    });

    with_dummy!(Edge, |edge: &Edge| {
        let value = edge.edge_type().unwrap();
        assert_eq!(value, CString::new("type").unwrap());
    });
}

#[test]
#[serial]
fn test_from_vertex() {
    mock_mgp_once!(mgp_edge_get_from_context, |_, _| {
        mgp_error::MGP_ERROR_NO_ERROR
    });
    mock_mgp_once!(mgp_vertex_copy_context, |_, _, _| {
        mgp_error::MGP_ERROR_UNABLE_TO_ALLOCATE
    });

    with_dummy!(Edge, |edge: &Edge| {
        assert!(edge.from_vertex().is_err());
    });
}

#[test]
#[serial]
fn test_to_vertex() {
    mock_mgp_once!(mgp_edge_get_to_context, |_, _| {
        mgp_error::MGP_ERROR_NO_ERROR
    });
    mock_mgp_once!(mgp_vertex_copy_context, |_, _, _| {
        mgp_error::MGP_ERROR_UNABLE_TO_ALLOCATE
    });

    with_dummy!(Edge, |edge: &Edge| {
        assert!(edge.to_vertex().is_err());
    });
}

#[test]
#[serial]
fn test_property() {
    mock_mgp_once!(mgp_edge_get_property_context, |_, _, _, _| {
        mgp_error::MGP_ERROR_UNABLE_TO_ALLOCATE
    });

    with_dummy!(Edge, |edge: &Edge| {
        assert!(edge.property(c_str!("prop")).is_err());
    });
}

#[test]
#[serial]
fn test_properties_iterator() {
    mock_mgp_once!(mgp_edge_iter_properties_context, |_, _, _| {
        mgp_error::MGP_ERROR_UNABLE_TO_ALLOCATE
    });

    with_dummy!(Edge, |edge: &Edge| {
        assert!(edge.properties().is_err());
    });
}

#[test]
#[serial]
fn test_edges_iterator() {
    let edge_getter = |_, edge_ptr: *mut *mut mgp_edge| unsafe {
        (*edge_ptr) = null_mut();
        mgp_error::MGP_ERROR_NO_ERROR
    };
    mock_mgp_once!(mgp_edges_iterator_get_context, edge_getter);
    mock_mgp_once!(mgp_edges_iterator_next_context, edge_getter);

    with_dummy!(|memgraph: &Memgraph| {
        let mut iterator = EdgesIterator::new(null_mut(), &memgraph);
        assert!(iterator.next().is_none());
        assert!(iterator.next().is_none());
    });
}
