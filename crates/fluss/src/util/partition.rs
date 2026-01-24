// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Utils for partition.

#![allow(dead_code)]

use crate::error::{Error, Result};
use crate::metadata::{DataType, PartitionSpec, ResolvedPartitionSpec, TablePath};
use crate::row::{Date, Datum, Time, TimestampLtz, TimestampNtz};
use jiff::ToSpan;
use jiff::Zoned;
use jiff::civil::DateTime;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AutoPartitionTimeUnit {
    Year,
    Quarter,
    Month,
    Day,
    Hour,
}

pub fn validate_partition_spec(
    table_path: &TablePath,
    partition_keys: &[String],
    partition_spec: &PartitionSpec,
    is_create: bool,
) -> Result<()> {
    let partition_spec_map = partition_spec.get_spec_map();
    if partition_keys.len() != partition_spec_map.len() {
        return Err(Error::InvalidPartition {
            message: format!(
                "PartitionSpec size is not equal to partition keys size for partitioned table {}.",
                table_path
            ),
        });
    }

    let mut reordered_partition_values: Vec<&str> = Vec::with_capacity(partition_keys.len());
    for partition_key in partition_keys {
        if let Some(value) = partition_spec_map.get(partition_key) {
            reordered_partition_values.push(value);
        } else {
            return Err(Error::InvalidPartition {
                message: format!(
                    "PartitionSpec {} does not contain partition key '{}' for partitioned table {}.",
                    partition_spec, partition_key, table_path
                ),
            });
        }
    }

    validate_partition_values(&reordered_partition_values, is_create)
}

fn validate_partition_values(partition_values: &[&str], is_create: bool) -> Result<()> {
    for value in partition_values {
        let invalid_name_error = TablePath::detect_invalid_name(value);
        let prefix_error = if is_create {
            TablePath::validate_prefix(value)
        } else {
            None
        };

        if invalid_name_error.is_some() || prefix_error.is_some() {
            let error_msg = invalid_name_error.unwrap_or_else(|| prefix_error.unwrap());
            return Err(Error::InvalidPartition {
                message: format!("The partition value {} is invalid: {}", value, error_msg),
            });
        }
    }
    Ok(())
}

/// Generate [`ResolvedPartitionSpec`] for auto partition in server. When we auto creating a
/// partition, we need to first generate a [`ResolvedPartitionSpec`].
///
/// The value is the formatted time with the specified time unit.
pub fn generate_auto_partition(
    partition_keys: Vec<String>,
    current: &Zoned,
    offset: i32,
    time_unit: AutoPartitionTimeUnit,
) -> ResolvedPartitionSpec {
    let auto_partition_field_spec = generate_auto_partition_time(current, offset, time_unit);
    ResolvedPartitionSpec::from_partition_name(partition_keys, &auto_partition_field_spec)
}

pub fn generate_auto_partition_time(
    current: &Zoned,
    offset: i32,
    time_unit: AutoPartitionTimeUnit,
) -> String {
    match time_unit {
        AutoPartitionTimeUnit::Year => {
            let adjusted = current
                .checked_add(jiff::Span::new().years(offset))
                .expect("year overflow");
            format!("{}", adjusted.year())
        }
        AutoPartitionTimeUnit::Quarter => {
            let adjusted = current
                .checked_add(jiff::Span::new().months(offset * 3))
                .expect("quarter overflow");
            let quarter = (adjusted.month() as i32 - 1) / 3 + 1;
            format!("{}{}", adjusted.year(), quarter)
        }
        AutoPartitionTimeUnit::Month => {
            let adjusted = current
                .checked_add(jiff::Span::new().months(offset))
                .expect("month overflow");
            format!("{}{:02}", adjusted.year(), adjusted.month())
        }
        AutoPartitionTimeUnit::Day => {
            let adjusted = current
                .checked_add(jiff::Span::new().days(offset))
                .expect("day overflow");
            format!(
                "{}{:02}{:02}",
                adjusted.year(),
                adjusted.month(),
                adjusted.day()
            )
        }
        AutoPartitionTimeUnit::Hour => {
            let adjusted = current
                .checked_add(jiff::Span::new().hours(offset))
                .expect("hour overflow");
            format!(
                "{}{:02}{:02}{:02}",
                adjusted.year(),
                adjusted.month(),
                adjusted.day(),
                adjusted.hour()
            )
        }
    }
}

fn hex_string(bytes: &[u8]) -> String {
    let mut hex = String::with_capacity(bytes.len() * 2);
    for &b in bytes {
        let h = format!("{:x}", b);
        if h.len() == 1 {
            hex.push('0');
        }
        hex.push_str(&h);
    }
    hex
}

fn reformat_float(value: f32) -> String {
    if value.is_nan() {
        "NaN".to_string()
    } else if value.is_infinite() {
        if value > 0.0 {
            "Inf".to_string()
        } else {
            "-Inf".to_string()
        }
    } else {
        value.to_string().replace('.', "_")
    }
}

fn reformat_double(value: f64) -> String {
    if value.is_nan() {
        "NaN".to_string()
    } else if value.is_infinite() {
        if value > 0.0 {
            "Inf".to_string()
        } else {
            "-Inf".to_string()
        }
    } else {
        value.to_string().replace('.', "_")
    }
}

const UNIX_EPOCH_DATE: jiff::civil::Date = jiff::civil::date(1970, 1, 1);

fn day_to_string(days: i32) -> String {
    let date = UNIX_EPOCH_DATE + days.days();
    format!("{:04}-{:02}-{:02}", date.year(), date.month(), date.day())
}

fn date_to_string(date: Date) -> String {
    day_to_string(date.get_inner())
}

const NANOS_PER_MILLIS: i64 = 1_000_000;
const MILLIS_PER_SECOND: i64 = 1_000;
const MILLIS_PER_MINUTE: i64 = 60 * MILLIS_PER_SECOND;
const MILLIS_PER_HOUR: i64 = 60 * MILLIS_PER_MINUTE;

fn milli_to_string(milli: i32) -> String {
    let hour = milli.div_euclid(MILLIS_PER_HOUR as i32);
    let min = milli
        .rem_euclid(MILLIS_PER_HOUR as i32)
        .div_euclid(MILLIS_PER_MINUTE as i32);
    let sec = milli
        .rem_euclid(MILLIS_PER_MINUTE as i32)
        .div_euclid(MILLIS_PER_SECOND as i32);
    let ms = milli.rem_euclid(MILLIS_PER_SECOND as i32);

    format!("{:02}-{:02}-{:02}_{:03}", hour, min, sec, ms)
}

fn time_to_string(time: Time) -> String {
    milli_to_string(time.get_inner())
}

/// Always add nanoseconds whether TimestampNtz and TimestampLtz are compact or not.
fn timestamp_ntz_to_string(ts: TimestampNtz) -> String {
    let millis = ts.get_millisecond();
    let nano_of_milli = ts.get_nano_of_millisecond();

    let total_nanos = (millis % MILLIS_PER_SECOND) * NANOS_PER_MILLIS + (nano_of_milli as i64);
    let total_secs = millis / MILLIS_PER_SECOND;

    let epoch = jiff::Timestamp::UNIX_EPOCH;
    let ts_jiff = epoch + jiff::Span::new().seconds(total_secs);
    let dt = ts_jiff.to_zoned(jiff::tz::TimeZone::UTC).datetime();

    format_date_time(total_nanos, dt)
}

fn timestamp_ltz_to_string(ts: TimestampLtz) -> String {
    let millis = ts.get_epoch_millisecond();
    let nano_of_milli = ts.get_nano_of_millisecond();

    let total_nanos = (millis % MILLIS_PER_SECOND) * NANOS_PER_MILLIS + (nano_of_milli as i64);
    let total_secs = millis / MILLIS_PER_SECOND;

    let epoch = jiff::Timestamp::UNIX_EPOCH;
    let ts_jiff = epoch + jiff::Span::new().seconds(total_secs);
    let dt = ts_jiff.to_zoned(jiff::tz::TimeZone::UTC).datetime();

    format_date_time(total_nanos, dt)
}

fn format_date_time(total_nanos: i64, dt: DateTime) -> String {
    if total_nanos > 0 {
        format!(
            "{:04}-{:02}-{:02}-{:02}-{:02}-{:02}_{}",
            dt.year(),
            dt.month(),
            dt.day(),
            dt.hour(),
            dt.minute(),
            dt.second(),
            total_nanos
        )
    } else {
        format!(
            "{:04}-{:02}-{:02}-{:02}-{:02}-{:02}",
            dt.year(),
            dt.month(),
            dt.day(),
            dt.hour(),
            dt.minute(),
            dt.second(),
        )
    }
}

/// Converts a Datum value to its string representation for partition naming.
pub fn convert_value_to_string(value: &Datum, data_type: &DataType) -> String {
    match (value, data_type) {
        (Datum::String(s), DataType::Char(_) | DataType::String(_)) => s.to_string(),
        (Datum::Bool(b), DataType::Boolean(_)) => b.to_string(),
        (Datum::Blob(bytes), DataType::Binary(_) | DataType::Bytes(_)) => hex_string(bytes),
        (Datum::Int8(v), DataType::TinyInt(_)) => v.to_string(),
        (Datum::Int16(v), DataType::SmallInt(_)) => v.to_string(),
        (Datum::Int32(v), DataType::Int(_)) => v.to_string(),
        (Datum::Int64(v), DataType::BigInt(_)) => v.to_string(),
        (Datum::Date(d), DataType::Date(_)) => date_to_string(*d),
        (Datum::Time(t), DataType::Time(_)) => time_to_string(*t),
        (Datum::Float32(f), DataType::Float(_)) => reformat_float(f.into_inner()),
        (Datum::Float64(f), DataType::Double(_)) => reformat_double(f.into_inner()),
        (Datum::TimestampLtz(ts), DataType::TimestampLTz(_)) => timestamp_ltz_to_string(*ts),
        (Datum::TimestampNtz(ts), DataType::Timestamp(_)) => timestamp_ntz_to_string(*ts),
        _ => panic!(
            "Unsupported data type for partition key: {:?}, value: {:?}",
            data_type, value
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metadata::{
        BigIntType, BinaryType, BooleanType, BytesType, CharType, DateType, DoubleType, FloatType,
        IntType, SmallIntType, StringType, TimeType, TimestampLTzType, TimestampType, TinyIntType,
    };
    use crate::row::{Date, Time, TimestampLtz, TimestampNtz};
    use std::borrow::Cow;

    use crate::metadata::TablePath;

    #[test]
    fn test_validate_partition_values() {
        // Test invalid character '$'
        let result = validate_partition_values(&["$1", "2"], true);
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("The partition value $1 is invalid"));
        assert!(err_msg.contains(
            "'$1' contains one or more characters other than ASCII alphanumerics, '_' and '-'"
        ));

        // Test invalid character '?'
        let result = validate_partition_values(&["?1", "2"], false);
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("The partition value ?1 is invalid"));
        assert!(err_msg.contains(
            "'?1' contains one or more characters other than ASCII alphanumerics, '_' and '-'"
        ));

        // Test reserved prefix '__' with is_create=true
        let result = validate_partition_values(&["__p1", "2"], true);
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("The partition value __p1 is invalid"));
        assert!(err_msg.contains("'__' is not allowed as prefix, since it is reserved for internal databases/internal tables/internal partitions in Fluss server"));

        // Test reserved prefix '__' with is_create=false (should pass)
        let result = validate_partition_values(&["__p1", "2"], false);
        assert!(result.is_ok());

        // Test validate_partition_spec with mismatched size
        let table_path = TablePath::new("test_db".to_string(), "test_table".to_string());
        let partition_keys = vec!["b".to_string()];
        let partition_spec = PartitionSpec::new(std::collections::HashMap::new());
        let result = validate_partition_spec(&table_path, &partition_keys, &partition_spec, true);
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("PartitionSpec size is not equal to partition keys size for partitioned table test_db.test_table"));
    }

    #[test]
    fn test_generate_auto_partition_name() {
        use jiff::civil::date;
        use jiff::tz::TimeZone;

        // LocalDateTime of 2024-11-11 11:11 with UTC-8 timezone
        let tz = TimeZone::get("Etc/GMT+8").expect("timezone");
        let zoned = date(2024, 11, 11)
            .at(11, 11, 0, 0)
            .to_zoned(tz)
            .expect("Zoned datetime creation failed");

        // for year
        test_generate_auto_partition_name_for(
            &zoned,
            AutoPartitionTimeUnit::Year,
            &[-1, 0, 1, 2, 3],
            &["2023", "2024", "2025", "2026", "2027"],
        );

        // for quarter
        test_generate_auto_partition_name_for(
            &zoned,
            AutoPartitionTimeUnit::Quarter,
            &[-1, 0, 1, 2, 3],
            &["20243", "20244", "20251", "20252", "20253"],
        );

        // for month
        test_generate_auto_partition_name_for(
            &zoned,
            AutoPartitionTimeUnit::Month,
            &[-1, 0, 1, 2, 3],
            &["202410", "202411", "202412", "202501", "202502"],
        );

        // for day
        test_generate_auto_partition_name_for(
            &zoned,
            AutoPartitionTimeUnit::Day,
            &[-1, 0, 1, 2, 3, 20],
            &[
                "20241110", "20241111", "20241112", "20241113", "20241114", "20241201",
            ],
        );

        // for hour
        test_generate_auto_partition_name_for(
            &zoned,
            AutoPartitionTimeUnit::Hour,
            &[-2, -1, 0, 1, 2, 3, 13],
            &[
                "2024111109",
                "2024111110",
                "2024111111",
                "2024111112",
                "2024111113",
                "2024111114",
                "2024111200",
            ],
        );
    }

    fn test_generate_auto_partition_name_for(
        zoned: &Zoned,
        time_unit: AutoPartitionTimeUnit,
        offsets: &[i32],
        expected: &[&str],
    ) {
        for (i, offset) in offsets.iter().enumerate() {
            let resolved_partition_spec =
                generate_auto_partition(vec!["dt".to_string()], zoned, *offset, time_unit);
            assert_eq!(
                resolved_partition_spec.get_partition_name(),
                expected[i],
                "{:?} offset {} failed",
                time_unit,
                offset
            );
        }
    }

    #[test]
    fn test_string() {
        let datum = Datum::String(Cow::Borrowed("Fluss"));

        let to_string_result =
            convert_value_to_string(&datum, &DataType::String(StringType::new()));
        assert_eq!(to_string_result, "Fluss");
        let detect_invalid = TablePath::detect_invalid_name(&to_string_result);
        assert!(detect_invalid.is_none());
    }

    #[test]
    fn test_char() {
        let datum = Datum::String(Cow::Borrowed("F"));

        let to_string_result = convert_value_to_string(&datum, &DataType::Char(CharType::new(1)));
        assert_eq!(to_string_result, "F");
        let detect_invalid = TablePath::detect_invalid_name(&to_string_result);
        assert!(detect_invalid.is_none());
    }

    #[test]
    fn test_boolean() {
        let datum = Datum::Bool(true);

        let to_string_result =
            convert_value_to_string(&datum, &DataType::Boolean(BooleanType::new()));
        assert_eq!(to_string_result, "true");
        let detect_invalid = TablePath::detect_invalid_name(&to_string_result);
        assert!(detect_invalid.is_none());
    }

    #[test]
    fn test_byte() {
        let datum = Datum::Blob(Cow::Borrowed(&[0x10, 0x20, 0x30, 0x40, 0x50, 0xFF]));

        let to_string_result = convert_value_to_string(&datum, &DataType::Bytes(BytesType::new()));
        assert_eq!(to_string_result, "1020304050ff");
        let detect_invalid = TablePath::detect_invalid_name(&to_string_result);
        assert!(detect_invalid.is_none());
    }

    #[test]
    fn test_binary() {
        let datum = Datum::Blob(Cow::Borrowed(&[0x10, 0x20, 0x30, 0x40, 0x50, 0xFF]));

        let to_string_result =
            convert_value_to_string(&datum, &DataType::Binary(BinaryType::new(6)));
        assert_eq!(to_string_result, "1020304050ff");
        let detect_invalid = TablePath::detect_invalid_name(&to_string_result);
        assert!(detect_invalid.is_none());
    }

    #[test]
    fn test_tiny_int() {
        let datum = Datum::Int8(100);

        let to_string_result =
            convert_value_to_string(&datum, &DataType::TinyInt(TinyIntType::new()));
        assert_eq!(to_string_result, "100");
        let detect_invalid = TablePath::detect_invalid_name(&to_string_result);
        assert!(detect_invalid.is_none());
    }

    #[test]
    fn test_small_int() {
        let datum = Datum::Int16(-32760);

        let to_string_result =
            convert_value_to_string(&datum, &DataType::SmallInt(SmallIntType::new()));
        assert_eq!(to_string_result, "-32760");
        let detect_invalid = TablePath::detect_invalid_name(&to_string_result);
        assert!(detect_invalid.is_none());
    }

    #[test]
    fn test_int() {
        let datum = Datum::Int32(299000);

        let to_string_result = convert_value_to_string(&datum, &DataType::Int(IntType::new()));
        assert_eq!(to_string_result, "299000");
        let detect_invalid = TablePath::detect_invalid_name(&to_string_result);
        assert!(detect_invalid.is_none());
    }

    #[test]
    fn test_big_int() {
        let datum = Datum::Int64(1748662955428);

        let to_string_result =
            convert_value_to_string(&datum, &DataType::BigInt(BigIntType::new()));
        assert_eq!(to_string_result, "1748662955428");
        let detect_invalid = TablePath::detect_invalid_name(&to_string_result);
        assert!(detect_invalid.is_none());
    }

    #[test]
    fn test_date() {
        let datum = Datum::Date(Date::new(20235));

        let to_string_result = convert_value_to_string(&datum, &DataType::Date(DateType::new()));
        assert_eq!(to_string_result, "2025-05-27");
        let detect_invalid = TablePath::detect_invalid_name(&to_string_result);
        assert!(detect_invalid.is_none());
    }

    #[test]
    fn test_time() {
        let datum = Datum::Time(Time::new(5402199));

        let to_string_result =
            convert_value_to_string(&datum, &DataType::Time(TimeType::new(3).unwrap()));
        assert_eq!(to_string_result, "01-30-02_199");
        let detect_invalid = TablePath::detect_invalid_name(&to_string_result);
        assert!(detect_invalid.is_none());
    }

    #[test]
    fn test_float() {
        let datum = Datum::Float32(5.73.into());

        let to_string_result = convert_value_to_string(&datum, &DataType::Float(FloatType::new()));
        assert_eq!(to_string_result, "5_73");
        let detect_invalid = TablePath::detect_invalid_name(&to_string_result);
        assert!(detect_invalid.is_none());

        let datum = Datum::Float32(f32::NAN.into());
        assert_eq!(
            convert_value_to_string(&datum, &DataType::Float(FloatType::new())),
            "NaN"
        );

        let datum = Datum::Float32(f32::INFINITY.into());
        assert_eq!(
            convert_value_to_string(&datum, &DataType::Float(FloatType::new())),
            "Inf"
        );

        let datum = Datum::Float32(f32::NEG_INFINITY.into());
        assert_eq!(
            convert_value_to_string(&datum, &DataType::Float(FloatType::new())),
            "-Inf"
        );
    }

    #[test]
    fn test_double() {
        let datum = Datum::Float64(5.73737.into());

        let to_string_result =
            convert_value_to_string(&datum, &DataType::Double(DoubleType::new()));
        assert_eq!(to_string_result, "5_73737");
        let detect_invalid = TablePath::detect_invalid_name(&to_string_result);
        assert!(detect_invalid.is_none());

        let datum = Datum::Float64(f64::NAN.into());
        assert_eq!(
            convert_value_to_string(&datum, &DataType::Double(DoubleType::new())),
            "NaN"
        );

        let datum = Datum::Float64(f64::INFINITY.into());
        assert_eq!(
            convert_value_to_string(&datum, &DataType::Double(DoubleType::new())),
            "Inf"
        );

        let datum = Datum::Float64(f64::NEG_INFINITY.into());
        assert_eq!(
            convert_value_to_string(&datum, &DataType::Double(DoubleType::new())),
            "-Inf"
        );
    }

    #[test]
    fn test_timestamp_ntz() {
        let datum = Datum::TimestampNtz(
            TimestampNtz::from_millis_nanos(1748662955428, 99988)
                .expect("TimestampNtz init failed"),
        );

        let to_string_result =
            convert_value_to_string(&datum, &DataType::Timestamp(TimestampType::new(9).unwrap()));
        assert_eq!(to_string_result, "2025-05-31-03-42-35_428099988");
        let detect_invalid = TablePath::detect_invalid_name(&to_string_result);
        assert!(detect_invalid.is_none());
    }

    #[test]
    fn test_timestamp_ltz() {
        let datum = Datum::TimestampLtz(
            TimestampLtz::from_millis_nanos(1748662955428, 99988)
                .expect("TimestampLtz init failed"),
        );

        let to_string_result = convert_value_to_string(
            &datum,
            &DataType::TimestampLTz(TimestampLTzType::new(9).unwrap()),
        );
        assert_eq!(to_string_result, "2025-05-31-03-42-35_428099988");
        let detect_invalid = TablePath::detect_invalid_name(&to_string_result);
        assert!(detect_invalid.is_none());
    }
}
