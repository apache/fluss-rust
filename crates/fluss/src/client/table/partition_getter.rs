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
use crate::metadata::{DataType, RowType};
use crate::row::field_getter::FieldGetter;

#[allow(dead_code)]
pub struct PartitionGetter<'a> {
    partitions: Vec<(&'a String, &'a DataType, FieldGetter)>,
}

#[allow(dead_code)]
impl<'a> PartitionGetter<'a> {
    pub fn new(row_type: &'a RowType, partition_keys: &'a Vec<String>) -> Result<Self> {
        let mut partitions = Vec::with_capacity(partition_keys.len());

        for partition_key in partition_keys {
            if let Some(partition_col_index) = row_type.get_field_index(partition_key.as_str()) {
                let data_type = &row_type
                    .fields()
                    .get(partition_col_index)
                    .unwrap()
                    .data_type;
                let field_getter = FieldGetter::create(data_type, partition_col_index);

                partitions.push((partition_key, data_type, field_getter));
            } else {
                return Err(IllegalArgument {
                    message: format!(
                        "The partition column {partition_key} is not in the row {row_type}."
                    ),
                });
            };
        }

        Ok(Self { partitions })
    }
}
