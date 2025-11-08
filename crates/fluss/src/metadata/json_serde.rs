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

use crate::error::Error::{InvalidTableError, JsonSerdeError};
use crate::error::Result;
use crate::metadata::datatype::{DataType, DataTypes, TimeType, TimestampType, TimestampLTzType};
use crate::metadata::table::{Column, Schema, TableDescriptor};
use serde_json::{Value, json};
use std::collections::HashMap;

pub trait JsonSerde: Sized {
    fn serialize_json(&self) -> Result<Value>;

    fn deserialize_json(node: &Value) -> Result<Self>;
}

impl DataType {
    pub fn to_type_root(&self) -> &str {
        match &self {
            DataType::Boolean(_) => "BOOLEAN",
            DataType::TinyInt(_) => "TINYINT",
            DataType::SmallInt(_) => "SMALLINT",
            DataType::Int(_) => "INTEGER",
            DataType::BigInt(_) => "BIGINT",
            DataType::Float(_) => "FLOAT",
            DataType::Double(_) => "DOUBLE",
            DataType::Char(_) => "CHAR",
            DataType::String(_) => "STRING",
            DataType::Decimal(_) => "DECIMAL",
            DataType::Date(_) => "DATE",
            DataType::Time(_) => "TIME_WITHOUT_TIME_ZONE",
            DataType::Timestamp(_) => "TIMESTAMP_WITHOUT_TIME_ZONE",
            DataType::TimestampLTz(_) => "TIMESTAMP_WITH_LOCAL_TIME_ZONE",
            DataType::Bytes(_) => "BYTES",
            DataType::Binary(_) => "BINARY",
            DataType::Array(_) => "ARRAY",
            DataType::Map(_) => "MAP",
            DataType::Row(_) => "ROW",
        }
    }
}

impl DataType {
    const FIELD_NAME_TYPE_NAME: &'static str = "type";
    const FIELD_NAME_NULLABLE: &'static str = "nullable";
    const FIELD_NAME_LENGTH: &'static str = "length";
    #[allow(dead_code)]
    const FIELD_NAME_PRECISION: &'static str = "precision";
    #[allow(dead_code)]
    const FILED_NAME_SCALE: &'static str = "scale";
    #[allow(dead_code)]
    const FIELD_NAME_ELEMENT_TYPE: &'static str = "element_type";
    #[allow(dead_code)]
    const FIELD_NAME_KEY_TYPE: &'static str = "key_type";
    #[allow(dead_code)]
    const FIELD_NAME_VALUE_TYPE: &'static str = "value_type";
    #[allow(dead_code)]
    const FIELD_NAME_FIELDS: &'static str = "fields";
    #[allow(dead_code)]
    const FIELD_NAME_FIELD_NAME: &'static str = "name";
    // ROW
    #[allow(dead_code)]
    const FIELD_NAME_FIELD_TYPE: &'static str = "field_type";
    #[allow(dead_code)]
    const FIELD_NAME_FIELD_DESCRIPTION: &'static str = "description";
}

impl JsonSerde for DataType {
    fn serialize_json(&self) -> Result<Value> {
        let mut obj = serde_json::Map::new();

        obj.insert(
            Self::FIELD_NAME_TYPE_NAME.to_string(),
            json!(Self::to_type_root(self)),
        );
        if !self.is_nullable() {
            obj.insert(Self::FIELD_NAME_NULLABLE.to_string(), json!(false));
        }

        match &self {
            DataType::Boolean(_)
            | DataType::TinyInt(_)
            | DataType::SmallInt(_)
            | DataType::Int(_)
            | DataType::BigInt(_)
            | DataType::Float(_)
            | DataType::Double(_)
            | DataType::String(_)
            | DataType::Bytes(_)
            | DataType::Date(_) => {
                // do nothing
            }
            DataType::Char(_type) => {
                obj.insert(Self::FIELD_NAME_LENGTH.to_string(), json!(_type.length()));
            }
            DataType::Binary(_type) => {
                obj.insert(Self::FIELD_NAME_LENGTH.to_string(), json!(_type.length()));
            }
            DataType::Decimal(_type) => {
                todo!()
            }

            DataType::Time(_type) => {
                obj.insert(Self::FIELD_NAME_PRECISION.to_string(), json!(_type.precision()));
            }
            DataType::Timestamp(_type) => {
                obj.insert(Self::FIELD_NAME_PRECISION.to_string(), json!(_type.precision()));
            }
            DataType::TimestampLTz(_type) => {
                obj.insert(Self::FIELD_NAME_PRECISION.to_string(), json!(_type.precision()));
            }
            DataType::Array(_type) => todo!(),
            DataType::Map(_type) => todo!(),
            DataType::Row(_type) => todo!(),
        }
        Ok(Value::Object(obj))
    }

    fn deserialize_json(node: &Value) -> Result<Self> {
        let mut _is_nullable = true;
        let type_root = node
            .get(Self::FIELD_NAME_TYPE_NAME)
            .and_then(|v| v.as_str())
            .ok_or_else(|| {
                JsonSerdeError(format!(
                    "Couldn't find field {} while deserializing datatype.",
                    Self::FIELD_NAME_TYPE_NAME
                ))
            })?;

        let mut data_type = match type_root {
            "BOOLEAN" => DataTypes::boolean(),
            "TINYINT" => DataTypes::tinyint(),
            "SMALLINT" => DataTypes::smallint(),
            "INTEGER" => DataTypes::int(),
            "BIGINT" => DataTypes::bigint(),
            "FLOAT" => DataTypes::float(),
            "DOUBLE" => DataTypes::double(),
            "CHAR" => todo!(),
            "STRING" => DataTypes::string(),
            "DECIMAL" => todo!(),
            "DATE" => DataTypes::date(),
            "TIME_WITHOUT_TIME_ZONE" => {
                let precision = node
                    .get(Self::FIELD_NAME_PRECISION)
                    .and_then(|v| v.as_u64())
                    .map(|n| n as u32)
                    .unwrap_or(TimeType::DEFAULT_PRECISION);
                DataTypes::time_with_precision(precision)
            }
            "TIMESTAMP_WITHOUT_TIME_ZONE" => {
                let precision = node
                    .get(Self::FIELD_NAME_PRECISION)
                    .and_then(|v| v.as_u64())
                    .map(|n| n as u32)
                    .unwrap_or(TimestampType::DEFAULT_PRECISION);
                DataTypes::timestamp_with_precision(precision)
            }
            "TIMESTAMP_WITH_LOCAL_TIME_ZONE" => {
                let precision = node
                    .get(Self::FIELD_NAME_PRECISION)
                    .and_then(|v| v.as_u64())
                    .map(|n| n as u32)
                    .unwrap_or(TimestampLTzType::DEFAULT_PRECISION);
                DataTypes::timestamp_ltz_with_precision(precision)
            }
            "BYTES" => DataTypes::bytes(),
            "BINARY" => todo!(),
            "ARRAY" => todo!(),
            "MAP" => todo!(),
            "ROW" => todo!(),
            _ => return Err(JsonSerdeError(format!("Unknown type root: {type_root}"))),
        };

        if let Some(nullable) = node.get(Self::FIELD_NAME_NULLABLE) {
            let nullable_value = nullable.as_bool().unwrap_or(true);
            if !nullable_value {
                data_type = data_type.as_non_nullable();
            }
        }
        Ok(data_type)
    }
}

impl Column {
    const NAME: &'static str = "name";
    const DATA_TYPE: &'static str = "data_type";
    const COMMENT: &'static str = "comment";
}

impl JsonSerde for Column {
    fn serialize_json(&self) -> Result<Value> {
        let mut obj = serde_json::Map::new();

        // Common fields
        obj.insert(Self::NAME.to_string(), json!(self.name()));
        obj.insert(
            Self::DATA_TYPE.to_string(),
            self.data_type().serialize_json()?,
        );

        if let Some(comment) = &self.comment() {
            obj.insert(Self::COMMENT.to_string(), json!(comment));
        }

        Ok(Value::Object(obj))
    }

    fn deserialize_json(node: &Value) -> Result<Column> {
        let name = node
            .get(Self::NAME)
            .and_then(|v| v.as_str())
            .unwrap_or_else(|| panic!("{}", format!("Missing required field: {}", Self::NAME)))
            .to_string();

        let data_type_node = node.get(Self::DATA_TYPE).ok_or_else(|| {
            JsonSerdeError(format!("Missing required field: {}", Self::DATA_TYPE))
        })?;

        let data_type = DataType::deserialize_json(data_type_node)?;

        let mut column = Column::new(&name, data_type);

        if let Some(comment) = node.get(Self::COMMENT).and_then(|v| v.as_str()) {
            column = column.with_comment(comment);
        }

        Ok(column)
    }
}

impl Schema {
    const COLUMNS_NAME: &'static str = "columns";
    const PRIMARY_KEY_NAME: &'static str = "primary_key";
    const VERSION_KEY: &'static str = "version";
    const VERSION: u32 = 1;
}

impl JsonSerde for Schema {
    fn serialize_json(&self) -> Result<Value> {
        let mut obj = serde_json::Map::new();

        // Serialize version
        obj.insert(Self::VERSION_KEY.to_string(), json!(Self::VERSION));

        // Serialize columns
        let columns: Vec<Value> = self
            .columns()
            .iter()
            .map(|col| col.serialize_json())
            .collect::<Result<_>>()?;
        obj.insert(Self::COLUMNS_NAME.to_string(), json!(columns));

        // Serialize primary key if present
        if let Some(primary_key) = &self.primary_key() {
            let pk_values: Vec<Value> = primary_key
                .column_names()
                .iter()
                .map(|name| json!(name))
                .collect();
            obj.insert(Self::PRIMARY_KEY_NAME.to_string(), json!(pk_values));
        }
        Ok(Value::Object(obj))
    }

    fn deserialize_json(node: &Value) -> Result<Schema> {
        let columns_node = node
            .get(Self::COLUMNS_NAME)
            .ok_or_else(|| {
                JsonSerdeError(format!("Missing required field: {}", Self::COLUMNS_NAME))
            })?
            .as_array()
            .ok_or_else(|| JsonSerdeError(format!("{} should be an array", Self::COLUMNS_NAME)))?;

        let mut columns = Vec::with_capacity(columns_node.len());
        for col_node in columns_node {
            columns.push(Column::deserialize_json(col_node)?);
        }

        let mut schema_builder = Schema::builder().with_columns(columns);

        if let Some(pk_node) = node.get(Self::PRIMARY_KEY_NAME) {
            let pk_array = pk_node
                .as_array()
                .ok_or_else(|| InvalidTableError("Primary key is not an array".to_string()))?;

            let mut primary_keys = Vec::with_capacity(pk_array.len());
            for name_node in pk_array {
                primary_keys.push(
                    name_node
                        .as_str()
                        .ok_or_else(|| InvalidTableError("Primary key is not string".to_string()))?
                        .to_string(),
                );
            }

            schema_builder = schema_builder.primary_key(primary_keys);
        }

        schema_builder.build()
    }
}

impl TableDescriptor {
    const SCHEMA_NAME: &'static str = "schema";
    const COMMENT_NAME: &'static str = "comment";
    const PARTITION_KEY_NAME: &'static str = "partition_key";
    const BUCKET_KEY_NAME: &'static str = "bucket_key";
    const BUCKET_COUNT_NAME: &'static str = "bucket_count";
    const PROPERTIES_NAME: &'static str = "properties";
    const CUSTOM_PROPERTIES_NAME: &'static str = "custom_properties";
    const VERSION_KEY: &'static str = "version";
    const VERSION: u32 = 1;

    fn deserialize_properties(node: &Value) -> Result<HashMap<String, String>> {
        let obj = node
            .as_object()
            .ok_or_else(|| JsonSerdeError("Properties should be an object".to_string()))?;

        let mut properties = HashMap::with_capacity(obj.len());
        for (key, value) in obj {
            properties.insert(
                key.clone(),
                value
                    .as_str()
                    .ok_or_else(|| JsonSerdeError("Properties should be an object".to_string()))?
                    .to_owned(),
            );
        }

        Ok(properties)
    }
}

impl JsonSerde for TableDescriptor {
    fn serialize_json(&self) -> Result<Value> {
        let mut obj = serde_json::Map::new();

        // Serialize version
        obj.insert(Self::VERSION_KEY.to_string(), json!(Self::VERSION));

        // Serialize schema
        obj.insert(
            Self::SCHEMA_NAME.to_string(),
            self.schema().serialize_json()?,
        );

        // Serialize comment if present
        if let Some(comment) = &self.comment() {
            obj.insert(Self::COMMENT_NAME.to_string(), json!(comment));
        }

        // Serialize partition keys
        let partition_keys: Vec<Value> =
            self.partition_keys().iter().map(|key| json!(key)).collect();
        obj.insert(Self::PARTITION_KEY_NAME.to_string(), json!(partition_keys));

        // Serialize table distribution if present
        if let Some(dist) = &self.table_distribution() {
            let bucket_keys: Vec<Value> = dist.bucket_keys().iter().map(|key| json!(key)).collect();
            obj.insert(Self::BUCKET_KEY_NAME.to_string(), json!(bucket_keys));

            if let Some(count) = dist.bucket_count() {
                obj.insert(Self::BUCKET_COUNT_NAME.to_string(), json!(count));
            }
        }

        // Serialize properties
        obj.insert(Self::PROPERTIES_NAME.to_string(), json!(self.properties()));

        obj.insert(
            Self::CUSTOM_PROPERTIES_NAME.to_string(),
            json!(self.custom_properties()),
        );

        Ok(Value::Object(obj))
    }

    fn deserialize_json(node: &Value) -> Result<Self> {
        let mut builder = TableDescriptor::builder();

        // Deserialize schema
        let schema_node = node.get(Self::SCHEMA_NAME).ok_or_else(|| {
            JsonSerdeError(format!("Missing required field: {}", Self::SCHEMA_NAME))
        })?;
        let schema = Schema::deserialize_json(schema_node)?;
        builder = builder.schema(schema);

        // Deserialize comment if present
        if let Some(comment_node) = node.get(Self::COMMENT_NAME) {
            let comment = comment_node
                .as_str()
                .ok_or_else(|| {
                    JsonSerdeError(format!("{} should be a string", Self::COMMENT_NAME))
                })?
                .to_owned();
            builder = builder.comment(comment.as_str());
        }

        let partition_node = node
            .get(Self::PARTITION_KEY_NAME)
            .ok_or_else(|| {
                JsonSerdeError(format!(
                    "Missing required field: {}",
                    Self::PARTITION_KEY_NAME
                ))
            })?
            .as_array()
            .ok_or_else(|| {
                JsonSerdeError(format!("{} should be an array", Self::PARTITION_KEY_NAME))
            })?;

        let mut partition_keys = Vec::with_capacity(partition_node.len());
        for key_node in partition_node {
            partition_keys.push(
                key_node
                    .as_str()
                    .ok_or_else(|| {
                        JsonSerdeError(format!("{} should be a string", Self::PARTITION_KEY_NAME))
                    })?
                    .to_owned(),
            );
        }
        builder = builder.partitioned_by(partition_keys);

        let mut bucket_count = None;
        let mut bucket_keys = vec![];
        if let Some(bucket_key_node) = node.get(Self::BUCKET_KEY_NAME) {
            let bucket_key_node = bucket_key_node.as_array().ok_or_else(|| {
                JsonSerdeError(format!("{} should be an array", Self::BUCKET_COUNT_NAME))
            })?;

            for key_node in bucket_key_node {
                bucket_keys.push(
                    key_node
                        .as_str()
                        .ok_or_else(|| JsonSerdeError("Bucket key should be a string".to_string()))?
                        .to_owned(),
                );
            }
        }

        if let Some(bucket_count_node) = node.get(Self::BUCKET_COUNT_NAME) {
            bucket_count = bucket_count_node.as_u64().map(|n| n as i32);
        }

        if bucket_count.is_some() || !bucket_keys.is_empty() {
            builder = builder.distributed_by(bucket_count, bucket_keys);
        }

        // Deserialize properties
        let properties =
            Self::deserialize_properties(node.get(Self::PROPERTIES_NAME).ok_or_else(|| {
                JsonSerdeError(format!("Missing required field: {}", Self::PROPERTIES_NAME))
            })?)?;
        builder = builder.properties(properties);

        // Deserialize custom properties
        let custom_properties = Self::deserialize_properties(
            node.get(Self::CUSTOM_PROPERTIES_NAME).ok_or_else(|| {
                JsonSerdeError(format!(
                    "Missing required field: {}",
                    Self::CUSTOM_PROPERTIES_NAME
                ))
            })?,
        )?;
        builder = builder.custom_properties(custom_properties);

        builder.build()
    }
}
