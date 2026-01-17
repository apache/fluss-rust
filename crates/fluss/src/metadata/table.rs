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

use crate::compression::ArrowCompressionInfo;
use crate::error::Error::InvalidTableError;
use crate::error::{Error, Result};
use crate::metadata::DataLakeFormat;
use crate::metadata::datatype::{DataField, DataType, RowType};
use core::fmt;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::fmt::{Display, Formatter};
use strum_macros::EnumString;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Column {
    name: String,
    data_type: DataType,
    comment: Option<String>,
}

impl Column {
    pub fn new(name: &str, data_type: DataType) -> Self {
        Self {
            name: name.to_string(),
            data_type,
            comment: None,
        }
    }

    pub fn with_comment(mut self, comment: &str) -> Self {
        self.comment = Some(comment.to_string());
        self
    }

    pub fn with_data_type(&self, data_type: DataType) -> Self {
        Self {
            name: self.name.clone(),
            data_type: data_type.clone(),
            comment: self.comment.clone(),
        }
    }

    // Getters...
    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn data_type(&self) -> &DataType {
        &self.data_type
    }

    pub fn comment(&self) -> Option<&str> {
        self.comment.as_deref()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PrimaryKey {
    constraint_name: String,
    column_names: Vec<String>,
}

impl PrimaryKey {
    pub fn new(constraint_name: &str, column_names: Vec<String>) -> Self {
        Self {
            constraint_name: constraint_name.to_string(),
            column_names,
        }
    }

    // Getters...
    pub fn constraint_name(&self) -> &str {
        &self.constraint_name
    }

    pub fn column_names(&self) -> &[String] {
        &self.column_names
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Schema {
    columns: Vec<Column>,
    primary_key: Option<PrimaryKey>,
    // must be Row data type kind
    row_type: DataType,
}

impl Schema {
    pub fn empty() -> Result<Self> {
        Self::builder().build()
    }

    pub fn builder() -> SchemaBuilder {
        SchemaBuilder::new()
    }

    pub fn columns(&self) -> &[Column] {
        &self.columns
    }

    pub fn primary_key(&self) -> Option<&PrimaryKey> {
        self.primary_key.as_ref()
    }

    pub fn row_type(&self) -> &DataType {
        &self.row_type
    }

    pub fn primary_key_indexes(&self) -> Vec<usize> {
        self.primary_key
            .as_ref()
            .map(|pk| {
                pk.column_names
                    .iter()
                    .filter_map(|name| self.columns.iter().position(|c| &c.name == name))
                    .collect()
            })
            .unwrap_or_default()
    }

    pub fn primary_key_column_names(&self) -> Vec<&str> {
        self.primary_key
            .as_ref()
            .map(|pk| pk.column_names.iter().map(|s| s.as_str()).collect())
            .unwrap_or_default()
    }

    pub fn column_names(&self) -> Vec<&str> {
        self.columns.iter().map(|c| c.name.as_str()).collect()
    }
}

#[derive(Debug, Default)]
pub struct SchemaBuilder {
    columns: Vec<Column>,
    primary_key: Option<PrimaryKey>,
}

impl SchemaBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_row_type(mut self, row_type: &DataType) -> Self {
        match row_type {
            DataType::Row(row) => {
                for data_field in row.fields() {
                    self = self.column(&data_field.name, data_field.data_type.clone())
                }
                self
            }
            _ => {
                panic!("data type msut be row type")
            }
        }
    }

    pub fn column(mut self, name: &str, data_type: DataType) -> Self {
        self.columns.push(Column::new(name, data_type));
        self
    }

    pub fn with_columns(mut self, columns: Vec<Column>) -> Self {
        self.columns.extend_from_slice(columns.as_ref());
        self
    }

    pub fn with_comment(mut self, comment: &str) -> Self {
        if let Some(last) = self.columns.last_mut() {
            *last = last.clone().with_comment(comment);
        }
        self
    }

    pub fn primary_key(self, column_names: Vec<String>) -> Self {
        let constraint_name = format!("PK_{}", column_names.join("_"));
        self.primary_key_named(&constraint_name, column_names)
    }

    pub fn primary_key_named(mut self, constraint_name: &str, column_names: Vec<String>) -> Self {
        self.primary_key = Some(PrimaryKey::new(constraint_name, column_names));
        self
    }

    pub fn build(&mut self) -> Result<Schema> {
        let columns = Self::normalize_columns(&mut self.columns, self.primary_key.as_ref())?;

        let data_fields = columns
            .iter()
            .map(|c| DataField {
                name: c.name.clone(),
                data_type: c.data_type.clone(),
                description: c.comment.clone(),
            })
            .collect();

        Ok(Schema {
            columns,
            primary_key: self.primary_key.clone(),
            row_type: DataType::Row(RowType::new(data_fields)),
        })
    }

    fn normalize_columns(
        columns: &mut [Column],
        primary_key: Option<&PrimaryKey>,
    ) -> Result<Vec<Column>> {
        let names: Vec<_> = columns.iter().map(|c| &c.name).collect();
        if let Some(duplicates) = Self::find_duplicates(&names) {
            return Err(InvalidTableError {
                message: format!("Duplicate column names found: {duplicates:?}"),
            });
        }

        let Some(pk) = primary_key else {
            return Ok(columns.to_vec());
        };

        let pk_set: HashSet<_> = pk.column_names.iter().collect();
        let all_columns: HashSet<_> = columns.iter().map(|c| &c.name).collect();
        if !pk_set.is_subset(&all_columns) {
            return Err(InvalidTableError {
                message: format!("Primary key columns {pk_set:?} not found in schema"),
            });
        }

        Ok(columns
            .iter()
            .map(|col| {
                if pk_set.contains(&col.name) && col.data_type.is_nullable() {
                    col.with_data_type(col.data_type.as_non_nullable())
                } else {
                    col.clone()
                }
            })
            .collect())
    }

    fn find_duplicates<'a>(names: &'a [&String]) -> Option<HashSet<&'a String>> {
        let mut seen = HashSet::new();
        let mut duplicates = HashSet::new();

        for name in names {
            if !seen.insert(name) {
                duplicates.insert(*name);
            }
        }

        if duplicates.is_empty() {
            None
        } else {
            Some(duplicates)
        }
    }
}

/// distribution of table
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TableDistribution {
    bucket_count: Option<i32>,
    bucket_keys: Vec<String>,
}

impl TableDistribution {
    pub fn bucket_keys(&self) -> &[String] {
        &self.bucket_keys
    }

    pub fn bucket_count(&self) -> Option<i32> {
        self.bucket_count
    }
}

#[derive(Debug, Default)]
pub struct TableDescriptorBuilder {
    schema: Option<Schema>,
    properties: HashMap<String, String>,
    custom_properties: HashMap<String, String>,
    partition_keys: Vec<String>,
    comment: Option<String>,
    table_distribution: Option<TableDistribution>,
}

impl TableDescriptorBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn schema(mut self, schema: Schema) -> Self {
        self.schema = Some(schema);
        self
    }

    pub fn log_format(mut self, log_format: LogFormat) -> Self {
        self.properties
            .insert("table.log.format".to_string(), log_format.to_string());
        self
    }

    pub fn kv_format(mut self, kv_format: KvFormat) -> Self {
        self.properties
            .insert("table.kv.format".to_string(), kv_format.to_string());
        self
    }

    pub fn property<T: ToString>(mut self, key: &str, value: T) -> Self {
        self.properties.insert(key.to_string(), value.to_string());
        self
    }

    pub fn properties(mut self, properties: HashMap<String, String>) -> Self {
        self.properties.extend(properties);
        self
    }

    pub fn custom_property(mut self, key: &str, value: &str) -> Self {
        self.custom_properties
            .insert(key.to_string(), value.to_string());
        self
    }

    pub fn custom_properties(mut self, custom_properties: HashMap<String, String>) -> Self {
        self.custom_properties.extend(custom_properties);
        self
    }

    pub fn partitioned_by(mut self, partition_keys: Vec<String>) -> Self {
        self.partition_keys = partition_keys;
        self
    }

    pub fn distributed_by(mut self, bucket_count: Option<i32>, bucket_keys: Vec<String>) -> Self {
        self.table_distribution = Some(TableDistribution {
            bucket_count,
            bucket_keys,
        });
        self
    }

    pub fn comment(mut self, comment: &str) -> Self {
        self.comment = Some(comment.to_string());
        self
    }

    pub fn build(self) -> Result<TableDescriptor> {
        let schema = self.schema.expect("Schema must be set");
        let table_distribution = TableDescriptor::normalize_distribution(
            &schema,
            &self.partition_keys,
            self.table_distribution,
        )?;
        Ok(TableDescriptor {
            schema,
            comment: self.comment,
            partition_keys: self.partition_keys,
            table_distribution,
            properties: self.properties,
            custom_properties: self.custom_properties,
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TableDescriptor {
    schema: Schema,
    comment: Option<String>,
    partition_keys: Vec<String>,
    table_distribution: Option<TableDistribution>,
    properties: HashMap<String, String>,
    custom_properties: HashMap<String, String>,
}

impl TableDescriptor {
    pub fn builder() -> TableDescriptorBuilder {
        TableDescriptorBuilder::new()
    }

    pub fn schema(&self) -> &Schema {
        &self.schema
    }

    pub fn bucket_keys(&self) -> Vec<&str> {
        self.table_distribution
            .as_ref()
            .map(|td| td.bucket_keys.iter().map(|s| s.as_str()).collect())
            .unwrap_or_default()
    }

    pub fn is_default_bucket_key(&self) -> Result<bool> {
        if self.schema.primary_key().is_some() {
            Ok(self.bucket_keys()
                == Self::default_bucket_key_of_primary_key_table(
                    self.schema(),
                    &self.partition_keys,
                )?
                .iter()
                .map(|s| s.as_str())
                .collect::<Vec<_>>())
        } else {
            Ok(self.bucket_keys().is_empty())
        }
    }

    pub fn is_partitioned(&self) -> bool {
        !self.partition_keys.is_empty()
    }

    pub fn has_primary_key(&self) -> bool {
        self.schema.primary_key().is_some()
    }

    pub fn partition_keys(&self) -> &[String] {
        &self.partition_keys
    }

    pub fn table_distribution(&self) -> Option<&TableDistribution> {
        self.table_distribution.as_ref()
    }

    pub fn properties(&self) -> &HashMap<String, String> {
        &self.properties
    }

    pub fn custom_properties(&self) -> &HashMap<String, String> {
        &self.custom_properties
    }

    pub fn replication_factor(&self) -> Result<i32> {
        self.properties
            .get("table.replication.factor")
            .ok_or_else(|| InvalidTableError {
                message: "Replication factor is not set".to_string(),
            })?
            .parse()
            .map_err(|_e| InvalidTableError {
                message: "Replication factor can't be convert into int".to_string(),
            })
    }

    pub fn with_properties(&self, new_properties: HashMap<String, String>) -> Self {
        Self {
            properties: new_properties,
            ..self.clone()
        }
    }

    pub fn with_replication_factor(&self, new_replication_factor: i32) -> Self {
        let mut properties = self.properties.clone();
        properties.insert(
            "table.replication.factor".to_string(),
            new_replication_factor.to_string(),
        );
        self.with_properties(properties)
    }

    pub fn with_bucket_count(&self, new_bucket_count: i32) -> Self {
        Self {
            table_distribution: Some(TableDistribution {
                bucket_count: Some(new_bucket_count),
                bucket_keys: self
                    .table_distribution
                    .as_ref()
                    .map(|td| td.bucket_keys.clone())
                    .unwrap_or_default(),
            }),
            ..self.clone()
        }
    }

    pub fn comment(&self) -> Option<&str> {
        self.comment.as_deref()
    }

    fn default_bucket_key_of_primary_key_table(
        schema: &Schema,
        partition_keys: &[String],
    ) -> Result<Vec<String>> {
        let mut bucket_keys = schema
            .primary_key()
            .expect("Primary key must be set")
            .column_names()
            .to_vec();

        bucket_keys.retain(|k| !partition_keys.contains(k));

        if bucket_keys.is_empty() {
            return Err(Error::InvalidTableError {
                message: format!(
                    "Primary Key constraint {:?} should not be same with partition fields {:?}.",
                    schema.primary_key().unwrap().column_names(),
                    partition_keys
                ),
            });
        }

        Ok(bucket_keys)
    }

    fn normalize_distribution(
        schema: &Schema,
        partition_keys: &[String],
        origin_distribution: Option<TableDistribution>,
    ) -> Result<Option<TableDistribution>> {
        if let Some(distribution) = origin_distribution {
            if distribution
                .bucket_keys
                .iter()
                .any(|k| partition_keys.contains(k))
            {
                return Err(InvalidTableError {
                    message: format!(
                        "Bucket key {:?} shouldn't include any column in partition keys {:?}.",
                        distribution.bucket_keys, partition_keys
                    ),
                });
            }

            return if let Some(pk) = schema.primary_key() {
                if distribution.bucket_keys.is_empty() {
                    Ok(Some(TableDistribution {
                        bucket_count: distribution.bucket_count,
                        bucket_keys: Self::default_bucket_key_of_primary_key_table(
                            schema,
                            partition_keys,
                        )?,
                    }))
                } else {
                    let pk_columns: HashSet<_> = pk.column_names().iter().collect();
                    if !distribution
                        .bucket_keys
                        .iter()
                        .all(|k| pk_columns.contains(k))
                    {
                        return Err(InvalidTableError {
                            message: format!(
                                "Bucket keys must be a subset of primary keys excluding partition keys for primary-key tables. \
                                The primary keys are {:?}, the partition keys are {:?}, but the user-defined bucket keys are {:?}.",
                                pk.column_names(),
                                partition_keys,
                                distribution.bucket_keys
                            ),
                        });
                    }
                    Ok(Some(distribution))
                }
            } else {
                Ok(Some(distribution))
            };
        } else if schema.primary_key().is_some() {
            return Ok(Some(TableDistribution {
                bucket_count: None,
                bucket_keys: Self::default_bucket_key_of_primary_key_table(schema, partition_keys)?,
            }));
        }

        Ok(None)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum LogFormat {
    ARROW,
    INDEXED,
}

impl Display for LogFormat {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LogFormat::ARROW => {
                write!(f, "ARROW")?;
            }
            LogFormat::INDEXED => {
                write!(f, "INDEXED")?;
            }
        }
        Ok(())
    }
}

impl LogFormat {
    pub fn parse(s: &str) -> Result<Self> {
        match s.to_uppercase().as_str() {
            "ARROW" => Ok(LogFormat::ARROW),
            "INDEXED" => Ok(LogFormat::INDEXED),
            _ => Err(InvalidTableError {
                message: format!("Unknown log format: {s}"),
            }),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, EnumString)]
pub enum KvFormat {
    INDEXED,
    COMPACTED,
}

impl Display for KvFormat {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            KvFormat::COMPACTED => write!(f, "COMPACTED")?,
            KvFormat::INDEXED => write!(f, "INDEXED")?,
        }
        Ok(())
    }
}

impl KvFormat {
    pub fn parse(s: &str) -> Result<Self> {
        match s.to_uppercase().as_str() {
            "INDEXED" => Ok(KvFormat::INDEXED),
            "COMPACTED" => Ok(KvFormat::COMPACTED),
            _ => Err(Error::InvalidTableError {
                message: format!("Unknown kv format: {s}"),
            }),
        }
    }
}

#[derive(Debug, PartialEq, Eq, Hash, Clone, Serialize, Deserialize)]
pub struct TablePath {
    database: String,
    table: String,
}

impl Display for TablePath {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}.{}", self.database, self.table)
    }
}

impl TablePath {
    pub fn new(db: String, tbl: String) -> Self {
        TablePath {
            database: db,
            table: tbl,
        }
    }

    #[inline]
    pub fn database(&self) -> &str {
        &self.database
    }

    #[inline]
    pub fn table(&self) -> &str {
        &self.table
    }
}

#[derive(Debug, Clone)]
pub struct PhysicalTablePath {
    table_path: TablePath,
    #[allow(dead_code)]
    partition: Option<String>,
}

impl PhysicalTablePath {
    pub fn of(table_path: TablePath) -> Self {
        Self {
            table_path,
            partition: None,
        }
    }

    // TODO: support partition

    pub fn get_table_path(&self) -> &TablePath {
        &self.table_path
    }
}

#[derive(Debug, Clone)]
pub struct TableInfo {
    pub table_path: TablePath,
    pub table_id: i64,
    pub schema_id: i32,
    pub schema: Schema,
    pub row_type: DataType,
    pub primary_keys: Vec<String>,
    pub physical_primary_keys: Vec<String>,
    pub bucket_keys: Vec<String>,
    pub partition_keys: Vec<String>,
    pub num_buckets: i32,
    pub properties: HashMap<String, String>,
    pub table_config: TableConfig,
    pub custom_properties: HashMap<String, String>,
    pub comment: Option<String>,
    pub created_time: i64,
    pub modified_time: i64,
}

impl TableInfo {
    pub fn row_type(&self) -> &RowType {
        match &self.row_type {
            DataType::Row(row_type) => row_type,
            _ => panic!("should be a row type"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TableConfig {
    pub properties: HashMap<String, String>,
}

impl TableConfig {
    pub fn from_properties(properties: HashMap<String, String>) -> Self {
        TableConfig { properties }
    }

    pub fn get_arrow_compression_info(&self) -> Result<ArrowCompressionInfo> {
        ArrowCompressionInfo::from_conf(&self.properties)
    }

    /// Returns the data lake format if configured, or None if not set.
    pub fn get_datalake_format(&self) -> Result<Option<DataLakeFormat>> {
        self.properties
            .get("table.datalake.format")
            .map(|f| f.parse().map_err(Error::from))
            .transpose()
    }

    pub fn get_kv_format(&self) -> Result<KvFormat> {
        // TODO: Consolidate configurations logic, constants, defaults in a single place
        const DEFAULT_KV_FORMAT: &str = "COMPACTED";
        let kv_format = self
            .properties
            .get("table.kv.format")
            .map(String::as_str)
            .unwrap_or(DEFAULT_KV_FORMAT);
        kv_format.parse().map_err(Into::into)
    }
}

impl TableInfo {
    pub fn of(
        table_path: TablePath,
        table_id: i64,
        schema_id: i32,
        table_descriptor: TableDescriptor,
        created_time: i64,
        modified_time: i64,
    ) -> TableInfo {
        let TableDescriptor {
            schema,
            table_distribution,
            comment,
            partition_keys,
            properties,
            custom_properties,
        } = table_descriptor;
        let TableDistribution {
            bucket_count,
            bucket_keys,
        } = table_distribution.unwrap();
        TableInfo::new(
            table_path,
            table_id,
            schema_id,
            schema,
            bucket_keys,
            partition_keys,
            bucket_count.unwrap(),
            properties,
            custom_properties,
            comment,
            created_time,
            modified_time,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn new(
        table_path: TablePath,
        table_id: i64,
        schema_id: i32,
        schema: Schema,
        bucket_keys: Vec<String>,
        partition_keys: Vec<String>,
        num_buckets: i32,
        properties: HashMap<String, String>,
        custom_properties: HashMap<String, String>,
        comment: Option<String>,
        created_time: i64,
        modified_time: i64,
    ) -> Self {
        let row_type = schema.row_type.clone();
        let primary_keys: Vec<String> = schema
            .primary_key_column_names()
            .iter()
            .map(|col| (*col).to_string())
            .collect();
        let physical_primary_keys =
            Self::generate_physical_primary_key(&primary_keys, &partition_keys);
        let table_config = TableConfig::from_properties(properties.clone());

        TableInfo {
            table_path,
            table_id,
            schema_id,
            schema,
            row_type,
            primary_keys,
            physical_primary_keys,
            bucket_keys,
            partition_keys,
            num_buckets,
            properties,
            table_config,
            custom_properties,
            comment,
            created_time,
            modified_time,
        }
    }

    pub fn get_table_path(&self) -> &TablePath {
        &self.table_path
    }

    pub fn get_table_id(&self) -> i64 {
        self.table_id
    }

    pub fn get_schema_id(&self) -> i32 {
        self.schema_id
    }

    pub fn get_schema(&self) -> &Schema {
        &self.schema
    }

    pub fn get_row_type(&self) -> &DataType {
        &self.row_type
    }

    pub fn has_primary_key(&self) -> bool {
        !self.primary_keys.is_empty()
    }

    pub fn get_primary_keys(&self) -> &Vec<String> {
        &self.primary_keys
    }

    pub fn get_physical_primary_keys(&self) -> &[String] {
        &self.physical_primary_keys
    }

    pub fn has_bucket_key(&self) -> bool {
        !self.bucket_keys.is_empty()
    }

    pub fn is_default_bucket_key(&self) -> bool {
        if self.has_primary_key() {
            self.bucket_keys == self.physical_primary_keys
        } else {
            self.bucket_keys.is_empty()
        }
    }

    pub fn get_bucket_keys(&self) -> &[String] {
        &self.bucket_keys
    }

    pub fn is_partitioned(&self) -> bool {
        !self.partition_keys.is_empty()
    }

    pub fn is_auto_partitioned(&self) -> bool {
        self.is_partitioned() && todo!()
    }

    pub fn get_partition_keys(&self) -> &[String] {
        &self.partition_keys
    }

    pub fn get_num_buckets(&self) -> i32 {
        self.num_buckets
    }

    pub fn get_properties(&self) -> &HashMap<String, String> {
        &self.properties
    }

    pub fn get_table_config(&self) -> &TableConfig {
        &self.table_config
    }

    pub fn get_custom_properties(&self) -> &HashMap<String, String> {
        &self.custom_properties
    }

    pub fn get_comment(&self) -> Option<&str> {
        self.comment.as_deref()
    }

    pub fn get_created_time(&self) -> i64 {
        self.created_time
    }

    pub fn get_modified_time(&self) -> i64 {
        self.modified_time
    }

    pub fn to_table_descriptor(&self) -> Result<TableDescriptor> {
        let mut builder = TableDescriptor::builder()
            .schema(self.schema.clone())
            .partitioned_by(self.partition_keys.clone())
            .distributed_by(Some(self.num_buckets), self.bucket_keys.clone())
            .properties(self.properties.clone())
            .custom_properties(self.custom_properties.clone());

        if let Some(comment) = &self.comment {
            builder = builder.comment(&comment.clone());
        }

        builder.build()
    }

    fn generate_physical_primary_key(
        primary_keys: &[String],
        partition_keys: &[String],
    ) -> Vec<String> {
        primary_keys
            .iter()
            .filter(|pk| !partition_keys.contains(*pk))
            .cloned()
            .collect()
    }
}

impl fmt::Display for TableInfo {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "TableInfo{{ table_path={:?}, table_id={}, schema_id={}, schema={:?}, physical_primary_keys={:?}, bucket_keys={:?}, partition_keys={:?}, num_buckets={}, properties={:?}, custom_properties={:?}, comment={:?}, created_time={}, modified_time={} }}",
            self.table_path,
            self.table_id,
            self.schema_id,
            self.schema,
            self.physical_primary_keys,
            self.bucket_keys,
            self.partition_keys,
            self.num_buckets,
            self.properties,
            self.custom_properties,
            self.comment,
            self.created_time,
            self.modified_time
        )
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Hash, PartialEq, Eq)]
pub struct TableBucket {
    table_id: i64,
    partition_id: Option<i64>,
    bucket: i32,
}

impl TableBucket {
    pub fn new(table_id: i64, bucket: i32) -> Self {
        TableBucket {
            table_id,
            partition_id: None,
            bucket,
        }
    }

    pub fn table_id(&self) -> i64 {
        self.table_id
    }

    pub fn bucket_id(&self) -> i32 {
        self.bucket
    }

    pub fn partition_id(&self) -> Option<i64> {
        self.partition_id
    }
}

impl Display for TableBucket {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(partition_id) = self.partition_id {
            write!(
                f,
                "TableBucket(table_id={}, partition_id={}, bucket={})",
                self.table_id, partition_id, self.bucket
            )
        } else {
            write!(
                f,
                "TableBucket(table_id={}, bucket={})",
                self.table_id, self.bucket
            )
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LakeSnapshot {
    pub snapshot_id: i64,
    pub table_buckets_offset: HashMap<TableBucket, i64>,
}

impl LakeSnapshot {
    pub fn new(snapshot_id: i64, table_buckets_offset: HashMap<TableBucket, i64>) -> Self {
        Self {
            snapshot_id,
            table_buckets_offset,
        }
    }

    pub fn snapshot_id(&self) -> i64 {
        self.snapshot_id
    }

    pub fn table_buckets_offset(&self) -> &HashMap<TableBucket, i64> {
        &self.table_buckets_offset
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metadata::DataTypes;

    #[test]
    fn schema_builder_rejects_duplicate_columns() {
        let result = Schema::builder()
            .column("id", DataTypes::int())
            .column("id", DataTypes::string())
            .build();
        assert!(matches!(result, Err(Error::InvalidTableError { .. })));
    }

    #[test]
    fn primary_key_columns_become_non_nullable() {
        let schema = Schema::builder()
            .column("id", DataTypes::int())
            .column("name", DataTypes::string())
            .primary_key(vec!["id".to_string()])
            .build()
            .expect("schema");

        let id_col = schema.columns().iter().find(|c| c.name() == "id").unwrap();
        assert!(!id_col.data_type().is_nullable());
    }

    #[test]
    fn table_descriptor_defaults_bucket_keys_for_primary_key() {
        let schema = Schema::builder()
            .column("id", DataTypes::int())
            .column("p", DataTypes::string())
            .primary_key(vec!["id".to_string()])
            .build()
            .expect("schema");

        let descriptor = TableDescriptor::builder()
            .schema(schema)
            .partitioned_by(vec!["p".to_string()])
            .build()
            .expect("descriptor");

        assert_eq!(descriptor.bucket_keys(), vec!["id"]);
        assert!(descriptor.has_primary_key());
    }

    #[test]
    fn table_descriptor_rejects_bucket_keys_with_partition() {
        let schema = Schema::builder()
            .column("id", DataTypes::int())
            .column("p", DataTypes::string())
            .primary_key(vec!["id".to_string()])
            .build()
            .expect("schema");

        let result = TableDescriptor::builder()
            .schema(schema)
            .partitioned_by(vec!["p".to_string()])
            .distributed_by(Some(1), vec!["p".to_string()])
            .build();
        assert!(matches!(result, Err(Error::InvalidTableError { .. })));
    }

    #[test]
    fn replication_factor_errors_on_missing_or_invalid() {
        let schema = Schema::builder()
            .column("id", DataTypes::int())
            .build()
            .expect("schema");
        let descriptor = TableDescriptor::builder()
            .schema(schema)
            .distributed_by(Some(1), vec![])
            .build()
            .expect("descriptor");

        assert!(descriptor.replication_factor().is_err());

        let mut props = HashMap::new();
        props.insert("table.replication.factor".to_string(), "oops".to_string());
        let descriptor = descriptor.with_properties(props);
        assert!(descriptor.replication_factor().is_err());
    }

    #[test]
    fn table_info_round_trip_descriptor() {
        let schema = Schema::builder()
            .column("id", DataTypes::int())
            .primary_key(vec!["id".to_string()])
            .build()
            .expect("schema");
        let descriptor = TableDescriptor::builder()
            .schema(schema)
            .distributed_by(Some(3), vec![])
            .comment("tbl")
            .build()
            .expect("descriptor");

        let info = TableInfo::of(
            TablePath::new("db".to_string(), "tbl".to_string()),
            10,
            1,
            descriptor.clone(),
            0,
            0,
        );
        let round_trip = info.to_table_descriptor().expect("descriptor");
        assert_eq!(
            round_trip.bucket_keys(),
            info.bucket_keys
                .iter()
                .map(|s| s.as_str())
                .collect::<Vec<_>>()
        );
        assert_eq!(round_trip.comment(), Some("tbl"));
    }

    #[test]
    fn formats_table_path_and_table_bucket() {
        let table_path = TablePath::new("db".to_string(), "tbl".to_string());
        assert_eq!(table_path.database(), "db");
        assert_eq!(table_path.table(), "tbl");
        assert_eq!(format!("{table_path}"), "db.tbl");

        let bucket = TableBucket::new(10, 2);
        assert_eq!(bucket.table_id(), 10);
        assert_eq!(bucket.bucket_id(), 2);
        assert_eq!(format!("{bucket}"), "TableBucket(table_id=10, bucket=2)");
    }

    #[test]
    fn default_bucket_key_detection() {
        let schema = Schema::builder()
            .column("id", DataTypes::int())
            .primary_key(vec!["id".to_string()])
            .build()
            .expect("schema");
        let descriptor = TableDescriptor::builder()
            .schema(schema)
            .distributed_by(Some(2), vec![])
            .build()
            .expect("descriptor");

        assert!(descriptor.is_default_bucket_key().expect("default"));
    }

    #[test]
    fn log_format_and_kv_format_parsing() {
        assert_eq!(LogFormat::parse("arrow").unwrap(), LogFormat::ARROW);
        assert!(LogFormat::parse("unknown").is_err());

        assert_eq!(KvFormat::parse("indexed").unwrap(), KvFormat::INDEXED);
        assert!(KvFormat::parse("bad").is_err());
    }

    #[test]
    fn table_info_flags_and_replication_factor() {
        let schema = Schema::builder()
            .column("id", DataTypes::int())
            .column("p", DataTypes::string())
            .primary_key(vec!["id".to_string()])
            .build()
            .expect("schema");
        let descriptor = TableDescriptor::builder()
            .schema(schema)
            .partitioned_by(vec!["p".to_string()])
            .distributed_by(Some(2), vec![])
            .build()
            .expect("descriptor")
            .with_replication_factor(3);

        assert!(descriptor.is_partitioned());
        assert!(descriptor.has_primary_key());
        assert_eq!(descriptor.replication_factor().unwrap(), 3);

        let info = TableInfo::of(
            TablePath::new("db".to_string(), "tbl".to_string()),
            10,
            1,
            descriptor,
            0,
            0,
        );

        assert!(info.has_primary_key());
        assert!(info.has_bucket_key());
        assert!(info.is_partitioned());
        assert!(info.is_default_bucket_key());
        assert_eq!(info.get_physical_primary_keys(), &["id".to_string()]);
    }

    #[test]
    fn schema_primary_key_indexes_and_column_names() {
        let schema = Schema::builder()
            .column("id", DataTypes::int())
            .column("name", DataTypes::string())
            .primary_key(vec!["id".to_string()])
            .build()
            .expect("schema");

        assert_eq!(schema.primary_key_indexes(), vec![0]);
        assert_eq!(schema.primary_key_column_names(), vec!["id"]);
        assert_eq!(schema.column_names(), vec!["id", "name"]);
    }
}
