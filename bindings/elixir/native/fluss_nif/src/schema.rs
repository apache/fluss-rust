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

use crate::atoms::to_nif_err;
use fluss::metadata::{DataTypes, Schema, SchemaBuilder, TableDescriptor};
use rustler::{NifTaggedEnum, ResourceArc};
use std::sync::Mutex;

pub struct SchemaBuilderResource(pub Mutex<Option<SchemaBuilder>>);
pub struct SchemaResource(pub Schema);
pub struct TableDescriptorResource(pub TableDescriptor);

impl std::panic::RefUnwindSafe for SchemaBuilderResource {}
impl std::panic::RefUnwindSafe for SchemaResource {}
impl std::panic::RefUnwindSafe for TableDescriptorResource {}

#[rustler::resource_impl]
impl rustler::Resource for SchemaBuilderResource {}

#[rustler::resource_impl]
impl rustler::Resource for SchemaResource {}

#[rustler::resource_impl]
impl rustler::Resource for TableDescriptorResource {}

/// Fluss data type for NIF interop.
///
/// Simple types map to atoms: `:int`, `:string`, etc.
/// Parameterized types map to tuples: `{:decimal, 10, 2}`, `{:char, 20}`.
#[derive(NifTaggedEnum)]
pub enum DataType {
    Boolean,
    Tinyint,
    Smallint,
    Int,
    Bigint,
    Float,
    Double,
    String,
    Bytes,
    Date,
    Time,
    Timestamp,
    TimestampLtz,
    Decimal(u32, u32),
    Char(u32),
    Binary(usize),
}

fn to_fluss_type(dt: &DataType) -> fluss::metadata::DataType {
    match dt {
        DataType::Boolean => DataTypes::boolean(),
        DataType::Tinyint => DataTypes::tinyint(),
        DataType::Smallint => DataTypes::smallint(),
        DataType::Int => DataTypes::int(),
        DataType::Bigint => DataTypes::bigint(),
        DataType::Float => DataTypes::float(),
        DataType::Double => DataTypes::double(),
        DataType::String => DataTypes::string(),
        DataType::Bytes => DataTypes::bytes(),
        DataType::Date => DataTypes::date(),
        DataType::Time => DataTypes::time(),
        DataType::Timestamp => DataTypes::timestamp(),
        DataType::TimestampLtz => DataTypes::timestamp_ltz(),
        DataType::Decimal(precision, scale) => DataTypes::decimal(*precision, *scale),
        DataType::Char(length) => DataTypes::char(*length),
        DataType::Binary(length) => DataTypes::binary(*length),
    }
}

#[rustler::nif]
fn schema_builder_new() -> ResourceArc<SchemaBuilderResource> {
    ResourceArc::new(SchemaBuilderResource(Mutex::new(Some(Schema::builder()))))
}

#[rustler::nif]
fn schema_builder_column(
    builder: ResourceArc<SchemaBuilderResource>,
    name: String,
    data_type: DataType,
) -> Result<ResourceArc<SchemaBuilderResource>, rustler::Error> {
    let mut guard = builder.0.lock().unwrap();
    let b = guard
        .take()
        .ok_or_else(|| to_nif_err("schema builder already consumed"))?;
    *guard = Some(b.column(&name, to_fluss_type(&data_type)));
    drop(guard);
    Ok(builder)
}

#[rustler::nif]
fn schema_builder_primary_key(
    builder: ResourceArc<SchemaBuilderResource>,
    keys: Vec<String>,
) -> Result<ResourceArc<SchemaBuilderResource>, rustler::Error> {
    let mut guard = builder.0.lock().unwrap();
    let b = guard
        .take()
        .ok_or_else(|| to_nif_err("schema builder already consumed"))?;
    *guard = Some(b.primary_key(keys));
    drop(guard);
    Ok(builder)
}

#[rustler::nif]
fn schema_builder_build(
    builder: ResourceArc<SchemaBuilderResource>,
) -> Result<ResourceArc<SchemaResource>, rustler::Error> {
    let mut guard = builder.0.lock().unwrap();
    let b = guard
        .take()
        .ok_or_else(|| to_nif_err("schema builder already consumed"))?;
    let schema = b.build().map_err(to_nif_err)?;
    Ok(ResourceArc::new(SchemaResource(schema)))
}

#[rustler::nif]
fn table_descriptor_new(
    schema: ResourceArc<SchemaResource>,
    bucket_count: Option<i32>,
    properties: Vec<(String, String)>,
) -> Result<ResourceArc<TableDescriptorResource>, rustler::Error> {
    let mut builder = TableDescriptor::builder().schema(schema.0.clone());
    if let Some(count) = bucket_count {
        builder = builder.distributed_by(Some(count), vec![]);
    }
    for (key, value) in properties {
        builder = builder.property(&key, &value);
    }
    let descriptor = builder.build().map_err(to_nif_err)?;
    Ok(ResourceArc::new(TableDescriptorResource(descriptor)))
}
