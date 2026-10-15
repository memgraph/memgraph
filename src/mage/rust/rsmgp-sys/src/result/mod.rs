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
//! Simplifies returning results to Memgraph and then to the client.

use chrono::{NaiveDate, NaiveDateTime, NaiveTime};
use snafu::Snafu;
use std::ffi::CStr;

use crate::edge::*;
use crate::list::*;
use crate::map::*;
use crate::memgraph::*;
use crate::mgp::*;
use crate::path::*;
use crate::value::*;
use crate::vertex::*;
// Required here, if not present tests linking fails.
#[double]
use crate::mgp::ffi;
use mockall_double::double;

pub struct ResultRecord {
    ptr: *mut mgp_result_record,
    memgraph: Memgraph,
}

impl ResultRecord {
    pub fn create(memgraph: &Memgraph) -> Result<ResultRecord> {
        unsafe {
            let mgp_ptr = invoke_mgp_func_with_res!(
                *mut mgp_result_record,
                Error::UnableToCreateResultRecord,
                ffi::mgp_result_new_record,
                memgraph.result_ptr()
            )?;
            Ok(ResultRecord {
                ptr: mgp_ptr,
                memgraph: memgraph.clone(),
            })
        }
    }

    pub fn insert_mgp_value(&self, field: &CStr, value: &MgpValue) -> Result<()> {
        unsafe {
            invoke_void_mgp_func_with_res!(
                Error::UnableToInsertResultValue,
                ffi::mgp_result_record_insert,
                self.ptr,
                field.as_ptr(),
                value.mgp_ptr()
            )?;
            Ok(())
        }
    }

    pub fn insert_null(&self, field: &CStr) -> Result<()> {
        self.insert_mgp_value(field, &MgpValue::make_null(&self.memgraph)?)
    }

    pub fn insert_bool(&self, field: &CStr, value: bool) -> Result<()> {
        self.insert_mgp_value(field, &MgpValue::make_bool(value, &self.memgraph)?)
    }

    pub fn insert_int(&self, field: &CStr, value: i64) -> Result<()> {
        self.insert_mgp_value(field, &MgpValue::make_int(value, &self.memgraph)?)
    }

    pub fn insert_double(&self, field: &CStr, value: f64) -> Result<()> {
        self.insert_mgp_value(field, &MgpValue::make_double(value, &self.memgraph)?)
    }

    pub fn insert_string(&self, field: &CStr, value: &CStr) -> Result<()> {
        self.insert_mgp_value(field, &MgpValue::make_string(value, &self.memgraph)?)
    }

    pub fn insert_list(&self, field: &CStr, value: &List) -> Result<()> {
        self.insert_mgp_value(field, &MgpValue::make_list(value, &self.memgraph)?)
    }

    pub fn insert_map(&self, field: &CStr, value: &Map) -> Result<()> {
        self.insert_mgp_value(field, &MgpValue::make_map(value, &self.memgraph)?)
    }

    pub fn insert_vertex(&self, field: &CStr, value: &Vertex) -> Result<()> {
        self.insert_mgp_value(field, &MgpValue::make_vertex(value, &self.memgraph)?)
    }

    pub fn insert_edge(&self, field: &CStr, value: &Edge) -> Result<()> {
        self.insert_mgp_value(field, &MgpValue::make_edge(value, &self.memgraph)?)
    }

    pub fn insert_path(&self, field: &CStr, value: &Path) -> Result<()> {
        self.insert_mgp_value(field, &MgpValue::make_path(value, &self.memgraph)?)
    }

    pub fn insert_date(&self, field: &CStr, value: &NaiveDate) -> Result<()> {
        self.insert_mgp_value(field, &MgpValue::make_date(value, &self.memgraph)?)
    }

    pub fn insert_local_time(&self, field: &CStr, value: &NaiveTime) -> Result<()> {
        self.insert_mgp_value(field, &MgpValue::make_local_time(value, &self.memgraph)?)
    }

    pub fn insert_local_date_time(&self, field: &CStr, value: &NaiveDateTime) -> Result<()> {
        self.insert_mgp_value(
            field,
            &MgpValue::make_local_date_time(value, &self.memgraph)?,
        )
    }

    pub fn insert_duration(&self, field: &CStr, value: &chrono::Duration) -> Result<()> {
        self.insert_mgp_value(field, &MgpValue::make_duration(value, &self.memgraph)?)
    }
}

#[derive(Debug, PartialEq, Snafu)]
#[snafu(visibility = "pub")]
pub enum Error {
    // DATE
    #[snafu(display("Unable to create date from NaiveDate."))]
    UnableToCreateDateFromNaiveDate,

    // DURATION
    #[snafu(display("Unable to create duration from chrono::Duration."))]
    UnableToCreateDurationFromChronoDuration,

    // EDGE
    #[snafu(display("Unable to copy edge."))]
    UnableToCopyEdge,

    #[snafu(display("Unable to return edge property because of value allocation error."))]
    UnableToReturnEdgePropertyValueAllocationError,

    #[snafu(display("Unable to return edge property because of value creation error."))]
    UnableToReturnEdgePropertyValueCreationError,

    #[snafu(display("Unable to return edge property because of name allocation error."))]
    UnableToReturnEdgePropertyNameAllocationError,

    #[snafu(display("Unable to return edge property because the edge is deleted."))]
    UnableToReturnEdgePropertyDeletedObjectError,

    #[snafu(display("Unable to return edge properties iterator."))]
    UnableToReturnEdgePropertiesIterator,

    // LIST
    #[snafu(display("Unable to create empty list."))]
    UnableToCreateEmptyList,

    #[snafu(display("Unable to copy list."))]
    UnableToCopyList,

    #[snafu(display("Unable to append list value."))]
    UnableToAppendListValue,

    #[snafu(display("Unable to append extend list value."))]
    UnableToAppendExtendListValue,

    #[snafu(display("Unable to access list value by index."))]
    UnableToAccessListValueByIndex,

    // LOCALTIME
    #[snafu(display("Unable to create local time from NaiveTime."))]
    UnableToCreateLocalTimeFromNaiveTime,

    // LOCALDATETIME
    #[snafu(display("Unable to create local date time from NaiveDateTime."))]
    UnableToCreateLocalDateTimeFromNaiveDateTime,

    // MAP
    #[snafu(display("Unable to copy map."))]
    UnableToCopyMap,

    #[snafu(display("Unable to create empty map."))]
    UnableToCreateEmptyMap,

    #[snafu(display("Unable to insert map value."))]
    UnableToInsertMapValue,

    #[snafu(display("Unable to access map value."))]
    UnableToAccessMapValue,

    #[snafu(display("Unable to create map iterator."))]
    UnableToCreateMapIterator,

    // MEMGRAPH
    #[snafu(display("Unable to create graph vertices iterator."))]
    UnableToCreateGraphVerticesIterator,

    #[snafu(display("Unable to find vertex by id."))]
    UnableToFindVertexById,

    #[snafu(display("Unable to register read procedure."))]
    UnableToRegisterReadProcedure,

    #[snafu(display("Unable to add required arguments."))]
    UnableToAddRequiredArguments,

    #[snafu(display("Unable to add optional arguments."))]
    UnableToAddOptionalArguments,

    #[snafu(display("Unable to add return type."))]
    UnableToAddReturnType,

    #[snafu(display("Unable to add deprecated return type."))]
    UnableToAddDeprecatedReturnType,

    // PATH
    #[snafu(display("Unable to copy path."))]
    UnableToCopyPath,

    #[snafu(display("Out of bound path vertex index."))]
    OutOfBoundPathVertexIndex,

    #[snafu(display("Out of bound path edge index."))]
    OutOfBoundPathEdgeIndex,

    #[snafu(display("Unable to create path with start Vertex."))]
    UnableToCreatePathWithStartVertex,

    #[snafu(display(
        "Unable to expand path because of not matching vertex value or lack of memory."
    ))]
    UnableToExpandPath,

    // RESULT
    #[snafu(display("Unable to create result record."))]
    UnableToCreateResultRecord,

    #[snafu(display("Unable to insert result record."))]
    UnableToInsertResultValue,

    // VALUE
    #[snafu(display("Unable to create new CString."))]
    UnableToCreateCString,

    #[snafu(display("Unable to make null value."))]
    UnableToMakeNullValue,

    #[snafu(display("Unable to make bool value."))]
    UnableToMakeBoolValue,

    #[snafu(display("Unable to make integer value."))]
    UnableToMakeIntegerValue,

    #[snafu(display("Unable to make double value."))]
    UnableToMakeDoubleValue,

    #[snafu(display("Unable to make Memgraph compatible string value."))]
    UnableToMakeMemgraphStringValue,

    #[snafu(display("Unable to make list value."))]
    UnableToMakeListValue,

    #[snafu(display("Unable to make map value."))]
    UnableToMakeMapValue,

    #[snafu(display("Unable to make vertex value."))]
    UnableToMakeVertexValue,

    #[snafu(display("Unable to make edge value."))]
    UnableToMakeEdgeValue,

    #[snafu(display("Unable to make path value."))]
    UnableToMakePathValue,

    #[snafu(display("Unable to make new Value::String."))]
    UnableToMakeValueString,

    #[snafu(display("Unable to make date value."))]
    UnableToMakeDateValue,

    #[snafu(display("Unable to make local time value."))]
    UnableToMakeLocalTimeValue,

    #[snafu(display("Unable to make local date time value."))]
    UnableToMakeLocalDateTimeValue,

    #[snafu(display("Unable to make duration value."))]
    UnableToMakeDurationValue,

    // VERTEX
    #[snafu(display("Unable to copy vertex."))]
    UnableToCopyVertex,

    #[snafu(display("Out of bound label index."))]
    OutOfBoundLabelIndexError,

    #[snafu(display("Unable to get vertex property."))]
    UnableToGetVertexProperty,

    #[snafu(display("Unable to return vertex property because of make name error."))]
    UnableToReturnVertexPropertyMakeNameEror,

    #[snafu(display("Unable to return vertex properties iterator."))]
    UnableToReturnVertexPropertiesIterator,

    #[snafu(display("Unable to return vertex in_edges iterator."))]
    UnableToReturnVertexInEdgesIterator,

    #[snafu(display("Unable to return vertex out_edges iterator."))]
    UnableToReturnVertexOutEdgesIterator,

    #[snafu(display("Unable to return vertex labels count because the vertex is deleted."))]
    UnableToReturnVertexLabelsCountDeletedObjectError,

    #[snafu(display("Unable to return vertex labels count because the vertex is deleted."))]
    UnableToReturnVertexLabelDeletedObjectError,

    #[snafu(display("Unable to check if vertex has a label."))]
    UnableToCheckVertexHasLabel,
}

/// A result type holding [Error] by default.
pub type Result<T, E = Error> = std::result::Result<T, E>;

#[cfg(test)]
mod tests;
