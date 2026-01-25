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

use crate::error::Error::IllegalArgument;
use crate::error::Result;
use crate::metadata::{DataType, ResolvedPartitionSpec, RowType};
use crate::row::InternalRow;
use crate::row::field_getter::FieldGetter;
use crate::util::partition;

/// A getter to get partition name from a row.
#[allow(dead_code)]
pub struct PartitionGetter {
    partition_keys: Vec<String>,
    partitions: Vec<(DataType, FieldGetter)>,
}

#[allow(dead_code)]
impl PartitionGetter {
    pub fn new(row_type: &RowType, partition_keys: Vec<String>) -> Result<Self> {
        let mut partitions = Vec::with_capacity(partition_keys.len());

        for partition_key in &partition_keys {
            if let Some(partition_col_index) = row_type.get_field_index(partition_key.as_str()) {
                let data_type = row_type
                    .fields()
                    .get(partition_col_index)
                    .unwrap()
                    .data_type
                    .clone();
                let field_getter = FieldGetter::create(&data_type, partition_col_index);

                partitions.push((data_type, field_getter));
            } else {
                return Err(IllegalArgument {
                    message: format!(
                        "The partition column {} is not in the row {}.",
                        partition_key, row_type
                    ),
                });
            };
        }

        Ok(Self {
            partition_keys,
            partitions,
        })
    }

    pub fn get_partition(&self, row: &dyn InternalRow) -> Result<String> {
        self.get_partition_spec(row)
            .map(|ps| ps.get_partition_name())
    }

    pub fn get_partition_spec(&self, row: &dyn InternalRow) -> Result<ResolvedPartitionSpec<'_>> {
        let mut partition_values = Vec::with_capacity(self.partitions.len());

        for (data_type, field_getter) in &self.partitions {
            let value = field_getter.get_field(row);
            if value.is_null() {
                return Err(IllegalArgument {
                    message: "Partition value shouldn't be null.".to_string(),
                });
            }
            partition_values.push(partition::convert_value_to_string(&value, data_type)?);
        }

        ResolvedPartitionSpec::new(self.partition_keys.as_slice(), partition_values)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metadata::{DataField, IntType, StringType};
    use crate::row::{Datum, GenericRow};

    #[test]
    fn test_partition_getter_single_key() {
        let row_type = RowType::new(vec![
            DataField::new("id".to_string(), DataType::Int(IntType::new()), None),
            DataField::new(
                "region".to_string(),
                DataType::String(StringType::new()),
                None,
            ),
        ]);

        let getter =
            PartitionGetter::new(&row_type, vec!["region".to_string()]).expect("should succeed");

        let row = GenericRow::from_data(vec![Datum::Int32(42), Datum::from("US")]);
        let partition_name = getter.get_partition(&row).expect("should succeed");
        assert_eq!(partition_name, "US");
    }

    #[test]
    fn test_partition_getter_multiple_keys() {
        let row_type = RowType::new(vec![
            DataField::new("id".to_string(), DataType::Int(IntType::new()), None),
            DataField::new(
                "date".to_string(),
                DataType::String(StringType::new()),
                None,
            ),
            DataField::new(
                "region".to_string(),
                DataType::String(StringType::new()),
                None,
            ),
        ]);

        let getter =
            PartitionGetter::new(&row_type, vec!["date".to_string(), "region".to_string()])
                .expect("should succeed");

        let row = GenericRow::from_data(vec![
            Datum::Int32(42),
            Datum::from("2024-01-15"),
            Datum::from("US"),
        ]);
        let partition_name = getter.get_partition(&row).expect("should succeed");
        assert_eq!(partition_name, "2024-01-15$US");
    }

    #[test]
    fn test_partition_getter_invalid_column() {
        let row_type = RowType::new(vec![DataField::new(
            "id".to_string(),
            DataType::Int(IntType::new()),
            None,
        )]);

        let result = PartitionGetter::new(&row_type, vec!["nonexistent".to_string()]);
        assert!(result.is_err());
    }

    #[test]
    fn test_partition_getter_null_value() {
        let row_type = RowType::new(vec![
            DataField::new("id".to_string(), DataType::Int(IntType::new()), None),
            DataField::new(
                "region".to_string(),
                DataType::String(StringType::new()),
                None,
            ),
        ]);

        let getter =
            PartitionGetter::new(&row_type, vec!["region".to_string()]).expect("should succeed");

        let row = GenericRow::from_data(vec![Datum::Int32(42), Datum::Null]);
        let result = getter.get_partition(&row);
        assert!(result.is_err());
    }

    #[test]
    fn test_get_partition_spec() {
        let row_type = RowType::new(vec![
            DataField::new("id".to_string(), DataType::Int(IntType::new()), None),
            DataField::new(
                "date".to_string(),
                DataType::String(StringType::new()),
                None,
            ),
            DataField::new(
                "region".to_string(),
                DataType::String(StringType::new()),
                None,
            ),
        ]);

        let getter =
            PartitionGetter::new(&row_type, vec!["date".to_string(), "region".to_string()])
                .expect("should succeed");

        let row = GenericRow::from_data(vec![
            Datum::Int32(42),
            Datum::from("2024-01-15"),
            Datum::from("US"),
        ]);
        let spec = getter.get_partition_spec(&row).expect("should succeed");

        assert_eq!(spec.get_partition_keys(), &["date", "region"]);
        assert_eq!(spec.get_partition_values(), &["2024-01-15", "US"]);
        assert_eq!(spec.get_partition_name(), "2024-01-15$US");
    }
}
