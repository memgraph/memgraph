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

use crate::memgraph::*;
#[double]
use crate::mgp::ffi;
use crate::mgp::*;
use crate::result::*;
use chrono::Timelike;
use chrono::{Datelike, NaiveDate, NaiveDateTime, NaiveTime};
use mockall_double::double;

const MINIMUM_YEAR: i32 = 0;
const MAXIMUM_YEAR: i32 = 9999;
const MICROS_PER_SECOND: i64 = 1_000_000;
const NANOS_PER_MILLIS: u32 = 1_000_000;
const NANOS_PER_MICROS: u32 = 1_000;
const MICROS_PER_MILLIS: u32 = 1_000;

pub(crate) struct Date {
    ptr: *mut mgp_date,
}

impl Drop for Date {
    fn drop(&mut self) {
        unsafe {
            if !self.ptr.is_null() {
                ffi::mgp_date_destroy(self.ptr);
            }
        }
    }
}

impl Date {
    pub(crate) fn new(ptr: *mut mgp_date) -> Date {
        #[cfg(not(test))]
        assert!(
            !ptr.is_null(),
            "Unable to create date because the given pointer is null."
        );

        Date { ptr }
    }

    pub fn from_naive_date(from: &NaiveDate, memgraph: &Memgraph) -> Result<Date> {
        let year = from.year();
        if year < MINIMUM_YEAR || year > MAXIMUM_YEAR {
            return Err(Error::UnableToCreateDateFromNaiveDate);
        }
        let mut date_params = mgp_date_parameters {
            year: from.year(),
            month: from.month() as i32,
            day: from.day() as i32,
        };
        unsafe {
            let date = Date::new(invoke_mgp_func_with_res!(
                *mut mgp_date,
                Error::UnableToCreateDateFromNaiveDate,
                ffi::mgp_date_from_parameters,
                &mut date_params,
                memgraph.memory_ptr()
            )?);
            Ok(date)
        }
    }

    pub fn to_naive_date(&self) -> NaiveDate {
        NaiveDate::from_ymd_opt(self.year(), self.month(), self.day())
            .expect("Invalid date parameters from Memgraph")
    }

    pub fn mgp_ptr(&self) -> *mut mgp_date {
        self.ptr
    }
    pub fn set_mgp_ptr(&mut self, new_ptr: *mut mgp_date) {
        self.ptr = new_ptr;
    }

    pub fn year(&self) -> i32 {
        unsafe { invoke_mgp_func!(i32, ffi::mgp_date_get_year, self.ptr).unwrap() }
    }

    pub fn month(&self) -> u32 {
        unsafe { invoke_mgp_func!(i32, ffi::mgp_date_get_month, self.ptr).unwrap() as u32 }
    }

    pub fn day(&self) -> u32 {
        unsafe { invoke_mgp_func!(i32, ffi::mgp_date_get_day, self.ptr).unwrap() as u32 }
    }
}

fn create_naive_time(
    hour: u32,
    minute: u32,
    second: u32,
    millisecond: u32,
    microsecond: u32,
) -> NaiveTime {
    NaiveTime::from_hms_micro_opt(
        hour,
        minute,
        second,
        millisecond * MICROS_PER_MILLIS + microsecond,
    )
    .expect("Invalid time parameters from Memgraph")
}

fn create_mgp_local_time_parameters(from: &NaiveTime) -> mgp_local_time_parameters {
    let eliminate_leap_seconds = |nanos: u32| {
        const NANOS_PER_SECONDS: u32 = 1_000_000_000;
        nanos % NANOS_PER_SECONDS
    };

    let nanoseconds = eliminate_leap_seconds(from.nanosecond());
    mgp_local_time_parameters {
        hour: from.hour() as i32,
        minute: from.minute() as i32,
        second: from.second() as i32,
        millisecond: (nanoseconds / NANOS_PER_MILLIS) as i32,
        microsecond: (nanoseconds % NANOS_PER_MILLIS / NANOS_PER_MICROS) as i32,
    }
}

pub(crate) struct LocalTime {
    ptr: *mut mgp_local_time,
}

impl Drop for LocalTime {
    fn drop(&mut self) {
        unsafe {
            if !self.ptr.is_null() {
                ffi::mgp_local_time_destroy(self.ptr);
            }
        }
    }
}

#[allow(dead_code)]
impl LocalTime {
    pub(crate) fn new(ptr: *mut mgp_local_time) -> LocalTime {
        #[cfg(not(test))]
        assert!(
            !ptr.is_null(),
            "Unable to create local time because the given pointer is null."
        );

        LocalTime { ptr }
    }

    pub fn from_naive_time(from: &NaiveTime, memgraph: &Memgraph) -> Result<LocalTime> {
        let mut local_time_params = create_mgp_local_time_parameters(&from);

        unsafe {
            let local_time = LocalTime::new(invoke_mgp_func_with_res!(
                *mut mgp_local_time,
                Error::UnableToCreateLocalTimeFromNaiveTime,
                ffi::mgp_local_time_from_parameters,
                &mut local_time_params,
                memgraph.memory_ptr()
            )?);
            Ok(local_time)
        }
    }

    pub fn to_naive_time(&self) -> NaiveTime {
        // Ideally this function should use NaiveTime::from_hms_nano, but because the issue with
        // the LocalTime::minute method it cannot be used.
        let timestamp = self.timestamp();
        let seconds = (timestamp / MICROS_PER_SECOND) as u32;
        let micros = (timestamp % MICROS_PER_SECOND) as u32;
        NaiveTime::from_num_seconds_from_midnight_opt(seconds, micros * NANOS_PER_MICROS)
            .expect("Invalid time parameters from Memgraph")
    }

    pub fn mgp_ptr(&self) -> *mut mgp_local_time {
        self.ptr
    }
    pub fn set_mgp_ptr(&mut self, new_ptr: *mut mgp_local_time) {
        self.ptr = new_ptr;
    }

    pub fn hour(&self) -> u32 {
        unsafe { invoke_mgp_func!(i32, ffi::mgp_local_time_get_hour, self.ptr).unwrap() as u32 }
    }

    pub fn minute(&self) -> u32 {
        // As of Memgraph 2.0.1 there is a bug in the C API of mgp_local_time, which prevents the
        // usage of mgp_local_time_get_minute. Therefore this function cannot be used until the bug
        // is fixed.
        unsafe { invoke_mgp_func!(i32, ffi::mgp_local_time_get_minute, self.ptr).unwrap() as u32 }
    }

    pub fn second(&self) -> u32 {
        unsafe { invoke_mgp_func!(i32, ffi::mgp_local_time_get_second, self.ptr).unwrap() as u32 }
    }

    pub fn millisecond(&self) -> u32 {
        unsafe {
            invoke_mgp_func!(i32, ffi::mgp_local_time_get_millisecond, self.ptr).unwrap() as u32
        }
    }

    pub fn microsecond(&self) -> u32 {
        unsafe {
            invoke_mgp_func!(i32, ffi::mgp_local_time_get_microsecond, self.ptr).unwrap() as u32
        }
    }

    pub fn timestamp(&self) -> i64 {
        unsafe { invoke_mgp_func!(i64, ffi::mgp_local_time_timestamp, self.ptr).unwrap() }
    }
}

pub(crate) struct LocalDateTime {
    ptr: *mut mgp_local_date_time,
}

impl Drop for LocalDateTime {
    fn drop(&mut self) {
        unsafe {
            if !self.ptr.is_null() {
                ffi::mgp_local_date_time_destroy(self.ptr);
            }
        }
    }
}

impl LocalDateTime {
    pub(crate) fn new(ptr: *mut mgp_local_date_time) -> LocalDateTime {
        #[cfg(not(test))]
        assert!(
            !ptr.is_null(),
            "Unable to create local date time because the given pointer is null."
        );

        LocalDateTime { ptr }
    }

    pub fn from_naive_date_time(
        from: &NaiveDateTime,
        memgraph: &Memgraph,
    ) -> Result<LocalDateTime> {
        let mut date_params = mgp_date_parameters {
            year: from.year(),
            month: from.month() as i32,
            day: from.day() as i32,
        };
        let mut local_time_params = create_mgp_local_time_parameters(&from.time());
        let mut local_date_time_params = mgp_local_date_time_parameters {
            date_parameters: &mut date_params,
            local_time_parameters: &mut local_time_params,
        };

        unsafe {
            let local_date_time = LocalDateTime::new(invoke_mgp_func_with_res!(
                *mut mgp_local_date_time,
                Error::UnableToCreateLocalDateTimeFromNaiveDateTime,
                ffi::mgp_local_date_time_from_parameters,
                &mut local_date_time_params,
                memgraph.memory_ptr()
            )?);
            Ok(local_date_time)
        }
    }

    pub fn to_naive_date_time(&self) -> NaiveDateTime {
        NaiveDate::from_ymd_opt(self.year(), self.month(), self.day())
            .expect("Invalid date parameters from Memgraph")
            .and_time(create_naive_time(
                self.hour(),
                self.minute(),
                self.second(),
                self.millisecond(),
                self.microsecond(),
            ))
    }

    pub fn mgp_ptr(&self) -> *mut mgp_local_date_time {
        self.ptr
    }
    pub fn set_mgp_ptr(&mut self, new_ptr: *mut mgp_local_date_time) {
        self.ptr = new_ptr;
    }

    pub fn year(&self) -> i32 {
        unsafe { invoke_mgp_func!(i32, ffi::mgp_local_date_time_get_year, self.ptr).unwrap() }
    }

    pub fn month(&self) -> u32 {
        unsafe {
            invoke_mgp_func!(i32, ffi::mgp_local_date_time_get_month, self.ptr).unwrap() as u32
        }
    }

    pub fn day(&self) -> u32 {
        unsafe { invoke_mgp_func!(i32, ffi::mgp_local_date_time_get_day, self.ptr).unwrap() as u32 }
    }

    pub fn hour(&self) -> u32 {
        unsafe {
            invoke_mgp_func!(i32, ffi::mgp_local_date_time_get_hour, self.ptr).unwrap() as u32
        }
    }

    pub fn minute(&self) -> u32 {
        unsafe {
            invoke_mgp_func!(i32, ffi::mgp_local_date_time_get_minute, self.ptr).unwrap() as u32
        }
    }

    pub fn second(&self) -> u32 {
        unsafe {
            invoke_mgp_func!(i32, ffi::mgp_local_date_time_get_second, self.ptr).unwrap() as u32
        }
    }

    pub fn millisecond(&self) -> u32 {
        unsafe {
            invoke_mgp_func!(i32, ffi::mgp_local_date_time_get_millisecond, self.ptr).unwrap()
                as u32
        }
    }

    pub fn microsecond(&self) -> u32 {
        unsafe {
            invoke_mgp_func!(i32, ffi::mgp_local_date_time_get_microsecond, self.ptr).unwrap()
                as u32
        }
    }
}

pub(crate) struct Duration {
    ptr: *mut mgp_duration,
}

impl Drop for Duration {
    fn drop(&mut self) {
        unsafe {
            if !self.ptr.is_null() {
                ffi::mgp_duration_destroy(self.ptr);
            }
        }
    }
}

impl Duration {
    pub(crate) fn new(ptr: *mut mgp_duration) -> Duration {
        #[cfg(not(test))]
        assert!(
            !ptr.is_null(),
            "Unable to create duration because the given pointer is null."
        );

        Duration { ptr }
    }

    pub fn from_chrono_duration(from: &chrono::Duration, memgraph: &Memgraph) -> Result<Duration> {
        unsafe {
            let duration = Duration::new(invoke_mgp_func_with_res!(
                *mut mgp_duration,
                Error::UnableToCreateDurationFromChronoDuration,
                ffi::mgp_duration_from_microseconds,
                from.num_microseconds()
                    .ok_or(Error::UnableToCreateDurationFromChronoDuration)?,
                memgraph.memory_ptr()
            )?);
            Ok(duration)
        }
    }

    pub fn to_chrono_duration(&self) -> chrono::Duration {
        chrono::Duration::microseconds(self.microseconds())
    }

    pub fn mgp_ptr(&self) -> *mut mgp_duration {
        self.ptr
    }
    pub fn set_mgp_ptr(&mut self, new_ptr: *mut mgp_duration) {
        self.ptr = new_ptr;
    }

    pub fn microseconds(&self) -> i64 {
        unsafe { invoke_mgp_func!(i64, ffi::mgp_duration_get_microseconds, self.ptr).unwrap() }
    }
}

#[cfg(test)]
mod tests;
