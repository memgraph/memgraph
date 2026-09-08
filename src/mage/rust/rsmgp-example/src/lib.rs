use c_str_macro::c_str;
use rsmgp_sys::list::*;
use rsmgp_sys::memgraph::*;
use rsmgp_sys::mgp::*;
use rsmgp_sys::property::*;
use rsmgp_sys::result::*;
use rsmgp_sys::rsmgp::*;
use rsmgp_sys::value::*;
use rsmgp_sys::{close_module, define_optional_type, define_procedure, define_type, init_module};
use std::ffi::CString;
use std::os::raw::c_int;
use std::panic;

init_module!(|memgraph: &Memgraph| -> Result<()> {
    memgraph.add_read_procedure(
        test_procedure,
        c_str!("test_procedure"),
        &[],
        &[],
        &[
            define_type!("labels_count", Type::Int),
            define_type!("has_L3_label", Type::Bool),
            define_type!("first_label", Type::String),
            define_type!("name_property", Type::String),
            define_type!("properties_string", Type::String),
            define_type!("first_edge_type", Type::String),
            define_type!("list", Type::List, Type::Int),
        ],
    )?;

    memgraph.add_read_procedure(
        basic,
        c_str!("basic"),
        &[define_type!("input_string", Type::String)],
        &[define_optional_type!(
            "optional_input_int",
            &MgpValue::make_int(0, &memgraph)?,
            Type::Int
        )],
        &[
            define_type!("output_string", Type::String),
            define_type!("output_int", Type::Int),
        ],
    )?;

    Ok(())
});

define_procedure!(basic, |memgraph: &Memgraph| -> Result<()> {
    // This procedure just forwards the input parameters as procedure results.
    let result = memgraph.result_record()?;
    let args = memgraph.args()?;
    let input_string = args.value_at(0)?;
    let input_int = args.value_at(1)?;
    result.insert_mgp_value(
        c_str!("output_string"),
        &input_string.to_mgp_value(&memgraph)?,
    )?;
    result.insert_mgp_value(c_str!("output_int"), &input_int.to_mgp_value(&memgraph)?)?;
    Ok(())
});

define_procedure!(test_procedure, |memgraph: &Memgraph| -> Result<()> {
    for mgp_vertex in memgraph.vertices_iter()? {
        let result = memgraph.result_record()?;

        let mut properties: Vec<Property> = mgp_vertex.properties()?.collect();
        properties.sort_by(|a, b| {
            let a_name = a.name.to_str().unwrap();
            let b_name = b.name.to_str().unwrap();
            a_name.cmp(&b_name)
        });
        let properties_string = properties
            .iter()
            .map(|prop| {
                let prop_name = prop.name.to_str().unwrap();
                if let Value::Int(value) = prop.value {
                    format!("{}: {}", prop_name, value)
                } else if let Value::String(value) = &prop.value {
                    format!("{}: {}", prop_name, value.to_str().unwrap())
                } else if let Value::Float(value) = prop.value {
                    format!("{}: {}", prop_name, value)
                } else {
                    format!("{}: <complex type>", prop_name)
                }
            })
            .collect::<Vec<String>>()
            .join(", ");
        result.insert_string(
            c_str!("properties_string"),
            CString::new(properties_string.into_bytes())
                .unwrap()
                .as_c_str(),
        )?;

        let labels_count = mgp_vertex.labels_count()?;
        result.insert_int(c_str!("labels_count"), labels_count as i64)?;
        if labels_count > 0 {
            result.insert_string(c_str!("first_label"), &mgp_vertex.label_at(0)?)?;
        } else {
            result.insert_string(c_str!("first_label"), c_str!(""))?;
        }

        let name_property = mgp_vertex.property(c_str!("name"))?.value;
        if let Value::Null = name_property {
            result.insert_string(c_str!("name_property"), c_str!("unknown"))?;
        } else if let Value::String(value) = name_property {
            result.insert_string(c_str!("name_property"), &value)?;
        } else {
            result.insert_string(c_str!("name_property"), c_str!("not null and not string"))?
        }

        result.insert_bool(c_str!("has_L3_label"), mgp_vertex.has_label(c_str!("L3"))?)?;

        match mgp_vertex.out_edges()?.next() {
            Some(edge) => {
                let edge_type = edge.edge_type()?;
                result.insert_string(c_str!("first_edge_type"), &edge_type)?;
            }
            None => {
                result.insert_string(c_str!("first_edge_type"), c_str!("unknown_edge_type"))?;
            }
        }

        let list_property = mgp_vertex.property(c_str!("list"))?.value;
        match list_property {
            Value::List(list) => result.insert_list(c_str!("list"), &list)?,
            Value::Null => result.insert_list(c_str!("list"), &List::make_empty(5, &memgraph)?)?,
            _ => (),
        }
    }
    Ok(())
});

close_module!(|| -> Result<()> { Ok(()) });
