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

use arrow::array::{
    ArrayBuilder, ArrayRef, BooleanBuilder, Float32Builder, Float64Builder, Int8Builder,
    Int16Builder, Int32Builder, Int64Builder, StringBuilder, UInt8Builder, UInt16Builder,
    UInt32Builder, UInt64Builder,
};
use arrow::{
    array::RecordBatch,
    ipc::{reader::StreamReader, writer::StreamWriter},
};
use arrow_schema::SchemaRef;
use arrow_schema::{DataType as ArrowDataType, Field};
use byteorder::WriteBytesExt;
use byteorder::{ByteOrder, LittleEndian};
use crc32c::crc32c;
use parking_lot::Mutex;
use std::{
    io::{Cursor, Write},
    sync::Arc,
};

use crate::error::Result;
use crate::metadata::DataType;
use crate::record::{ChangeType, ScanRecord};
use crate::row::{ColumnarRow, GenericRow};

/// const for record batch
pub const BASE_OFFSET_LENGTH: usize = 8;
pub const LENGTH_LENGTH: usize = 4;
pub const MAGIC_LENGTH: usize = 1;
pub const COMMIT_TIMESTAMP_LENGTH: usize = 8;
pub const CRC_LENGTH: usize = 4;
pub const SCHEMA_ID_LENGTH: usize = 2;
pub const ATTRIBUTE_LENGTH: usize = 1;
pub const LAST_OFFSET_DELTA_LENGTH: usize = 4;
pub const WRITE_CLIENT_ID_LENGTH: usize = 8;
pub const BATCH_SEQUENCE_LENGTH: usize = 4;
pub const RECORDS_COUNT_LENGTH: usize = 4;

pub const BASE_OFFSET_OFFSET: usize = 0;
pub const LENGTH_OFFSET: usize = BASE_OFFSET_OFFSET + BASE_OFFSET_LENGTH;
pub const MAGIC_OFFSET: usize = LENGTH_OFFSET + LENGTH_LENGTH;
pub const COMMIT_TIMESTAMP_OFFSET: usize = MAGIC_OFFSET + MAGIC_LENGTH;
pub const CRC_OFFSET: usize = COMMIT_TIMESTAMP_OFFSET + COMMIT_TIMESTAMP_LENGTH;
pub const SCHEMA_ID_OFFSET: usize = CRC_OFFSET + CRC_LENGTH;
pub const ATTRIBUTES_OFFSET: usize = SCHEMA_ID_OFFSET + SCHEMA_ID_LENGTH;
pub const LAST_OFFSET_DELTA_OFFSET: usize = ATTRIBUTES_OFFSET + ATTRIBUTE_LENGTH;
pub const WRITE_CLIENT_ID_OFFSET: usize = LAST_OFFSET_DELTA_OFFSET + LAST_OFFSET_DELTA_LENGTH;
pub const BATCH_SEQUENCE_OFFSET: usize = WRITE_CLIENT_ID_OFFSET + WRITE_CLIENT_ID_LENGTH;
pub const RECORDS_COUNT_OFFSET: usize = BATCH_SEQUENCE_OFFSET + BATCH_SEQUENCE_LENGTH;
pub const RECORDS_OFFSET: usize = RECORDS_COUNT_OFFSET + RECORDS_COUNT_LENGTH;

pub const RECORD_BATCH_HEADER_SIZE: usize = RECORDS_OFFSET;
pub const ARROW_CHANGETYPE_OFFSET: usize = RECORD_BATCH_HEADER_SIZE;
pub const LOG_OVERHEAD: usize = LENGTH_OFFSET + LENGTH_LENGTH;

/// const for record
/// The "magic" values.
#[derive(Debug, Clone, Copy)]
pub enum LogMagicValue {
    V0 = 0,
}

pub const CURRENT_LOG_MAGIC_VALUE: u8 = LogMagicValue::V0 as u8;

/// Value used if writer ID is not available or non-idempotent.
pub const NO_WRITER_ID: i64 = -1;

/// Value used if batch sequence is not available.
pub const NO_BATCH_SEQUENCE: i32 = -1;

pub const BUILDER_DEFAULT_OFFSET: i64 = 0;

pub const DEFAULT_MAX_RECORD: i32 = 256;

pub struct MemoryLogRecordsArrowBuilder {
    base_log_offset: i64,
    schema_id: i32,
    magic: u8,
    writer_id: i64,
    batch_sequence: i32,
    table_schema: SchemaRef,
    record_count: i32,
    arrow_column_builders: Mutex<Vec<Box<dyn ArrayBuilder>>>,
    is_closed: bool,
}

impl MemoryLogRecordsArrowBuilder {
    pub fn new(schema_id: i32, row_type: &DataType) -> Self {
        let schema_ref = to_arrow_schema(row_type);
        let builders = Mutex::new(
            schema_ref
                .fields()
                .iter()
                .map(|field| Self::create_builder(field.data_type()))
                .collect(),
        );
        MemoryLogRecordsArrowBuilder {
            base_log_offset: BUILDER_DEFAULT_OFFSET,
            schema_id,
            magic: CURRENT_LOG_MAGIC_VALUE,
            writer_id: NO_WRITER_ID,
            batch_sequence: NO_BATCH_SEQUENCE,
            record_count: 0,
            table_schema: schema_ref,
            arrow_column_builders: builders,
            is_closed: false,
        }
    }

    pub fn append(&mut self, row: &GenericRow) -> Result<()> {
        for (idx, value) in row.values.iter().enumerate() {
            let mut builder_binding = self.arrow_column_builders.lock();
            let builder = builder_binding.get_mut(idx).unwrap();
            value.append_to(builder.as_mut())?;
        }
        self.record_count += 1;
        // todo: consider write other change type
        Ok(())
    }

    pub fn is_full(&self) -> bool {
        self.record_count >= DEFAULT_MAX_RECORD
    }

    pub fn is_closed(&self) -> bool {
        self.is_closed
    }

    pub fn close(&mut self) {
        self.is_closed = true;
    }

    pub fn build(&self) -> Result<Vec<u8>> {
        // serialize arrow batch
        let mut arrow_batch_bytes = vec![];
        let mut writer = StreamWriter::try_new(&mut arrow_batch_bytes, &self.table_schema)?;

        let arrays = self
            .arrow_column_builders
            .lock()
            .iter_mut()
            .map(|b| b.finish())
            .collect::<Vec<ArrayRef>>();
        let record_batch = RecordBatch::try_new(self.table_schema.clone(), arrays)?;
        // get header len
        let header = writer.get_ref().len();
        writer.write(&record_batch)?;
        // get real arrow batch bytes
        let real_arrow_batch_bytes = &arrow_batch_bytes[header..];

        // now, write batch header and arrow batch
        let mut batch_bytes = vec![0u8; RECORD_BATCH_HEADER_SIZE + real_arrow_batch_bytes.len()];
        // write batch header
        self.write_batch_header(&mut batch_bytes[..])?;

        // write arrow batch bytes
        let mut cursor = Cursor::new(&mut batch_bytes[..]);
        cursor.set_position(RECORD_BATCH_HEADER_SIZE as u64);
        cursor.write_all(real_arrow_batch_bytes).unwrap();

        let calcute_crc_bytes = &cursor.get_ref()[SCHEMA_ID_OFFSET..];
        // then update crc
        let crc = crc32c(calcute_crc_bytes);
        cursor.set_position(CRC_OFFSET as u64);
        cursor.write_u32::<LittleEndian>(crc)?;

        Ok(batch_bytes.to_vec())
    }

    fn write_batch_header(&self, buffer: &mut [u8]) -> Result<()> {
        let total_len = buffer.len();
        let mut cursor = Cursor::new(buffer);
        cursor.write_i64::<LittleEndian>(self.base_log_offset)?;
        cursor
            .write_i32::<LittleEndian>((total_len - BASE_OFFSET_LENGTH - LENGTH_LENGTH) as i32)?;
        cursor.write_u8(self.magic)?;
        cursor.write_i64::<LittleEndian>(0)?; // timestamp placeholder
        cursor.write_u32::<LittleEndian>(0)?; // crc placeholder
        cursor.write_i16::<LittleEndian>(self.schema_id as i16)?;

        // todo: curerntly, always is append only
        let append_only = true;
        cursor.write_u8(if append_only { 1 } else { 0 })?;
        cursor.write_i32::<LittleEndian>(if self.record_count > 0 {
            self.record_count - 1
        } else {
            0
        })?;

        cursor.write_i64::<LittleEndian>(self.writer_id)?;
        cursor.write_i32::<LittleEndian>(self.batch_sequence)?;
        cursor.write_i32::<LittleEndian>(self.record_count)?;
        Ok(())
    }

    fn create_builder(data_type: &arrow_schema::DataType) -> Box<dyn ArrayBuilder> {
        match data_type {
            arrow_schema::DataType::Int8 => Box::new(Int8Builder::new()),
            arrow_schema::DataType::Int16 => Box::new(Int16Builder::new()),
            arrow_schema::DataType::Int32 => Box::new(Int32Builder::new()),
            arrow_schema::DataType::Int64 => Box::new(Int64Builder::new()),
            arrow_schema::DataType::UInt8 => Box::new(UInt8Builder::new()),
            arrow_schema::DataType::UInt16 => Box::new(UInt16Builder::new()),
            arrow_schema::DataType::UInt32 => Box::new(UInt32Builder::new()),
            arrow_schema::DataType::UInt64 => Box::new(UInt64Builder::new()),
            arrow_schema::DataType::Float32 => Box::new(Float32Builder::new()),
            arrow_schema::DataType::Float64 => Box::new(Float64Builder::new()),
            arrow_schema::DataType::Boolean => Box::new(BooleanBuilder::new()),
            arrow_schema::DataType::Utf8 => Box::new(StringBuilder::new()),
            dt => panic!("Unsupported data type: {dt:?}"),
        }
    }
}

pub trait ToArrow {
    fn append_to(&self, builder: &mut dyn ArrayBuilder) -> Result<()>;
}

pub struct LogRecordsBatchs<'a> {
    data: &'a [u8],
    current_pos: usize,
    remaining_bytes: usize,
}

impl<'a> LogRecordsBatchs<'a> {
    pub fn new(data: &'a [u8]) -> Self {
        let remaining_bytes: usize = data.len();
        Self {
            data,
            current_pos: 0,
            remaining_bytes,
        }
    }

    pub fn next_batch_size(&self) -> Option<usize> {
        if self.remaining_bytes < LOG_OVERHEAD {
            return None;
        }

        let batch_size_bytes =
            LittleEndian::read_i32(self.data.get(self.current_pos + LENGTH_OFFSET..).unwrap());
        Some(batch_size_bytes as usize + LOG_OVERHEAD)
    }
}

impl<'a> Iterator for &'a mut LogRecordsBatchs<'a> {
    type Item = LogRecordBatch<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.next_batch_size() {
            Some(batch_size) => {
                let data_slice = &self.data[self.current_pos..self.current_pos + batch_size];
                let record_batch = LogRecordBatch::new(data_slice);
                self.current_pos += batch_size;
                self.remaining_bytes -= batch_size;
                Some(record_batch)
            }
            None => None,
        }
    }
}

pub struct LogRecordBatch<'a> {
    data: &'a [u8],
}

#[allow(dead_code)]
impl<'a> LogRecordBatch<'a> {
    pub fn new(data: &'a [u8]) -> Self {
        LogRecordBatch { data }
    }

    pub fn magic(&self) -> u8 {
        self.data[MAGIC_OFFSET]
    }

    pub fn commit_timestamp(&self) -> i64 {
        let offset = COMMIT_TIMESTAMP_OFFSET;
        LittleEndian::read_i64(&self.data[offset..offset + COMMIT_TIMESTAMP_LENGTH])
    }

    pub fn writer_id(&self) -> i64 {
        let offset = WRITE_CLIENT_ID_OFFSET;
        LittleEndian::read_i64(&self.data[offset..offset + WRITE_CLIENT_ID_LENGTH])
    }

    pub fn batch_sequence(&self) -> i32 {
        let offset = BATCH_SEQUENCE_OFFSET;
        LittleEndian::read_i32(&self.data[offset..offset + BATCH_SEQUENCE_LENGTH])
    }

    pub fn ensure_valid(&self) -> Result<()> {
        // todo
        Ok(())
    }

    pub fn is_valid(&self) -> bool {
        self.size_in_bytes() >= RECORD_BATCH_HEADER_SIZE
            && self.checksum() == self.compute_checksum()
    }

    fn compute_checksum(&self) -> u32 {
        let start = SCHEMA_ID_OFFSET;
        let end = start + self.data.len();
        crc32c(&self.data[start..end])
    }

    fn attributes(&self) -> u8 {
        self.data[ATTRIBUTES_OFFSET]
    }

    pub fn next_log_offset(&self) -> i64 {
        self.last_log_offset() + 1
    }

    pub fn checksum(&self) -> u32 {
        let offset = CRC_OFFSET;
        LittleEndian::read_u32(&self.data[offset..offset + CRC_OFFSET])
    }

    pub fn schema_id(&self) -> i16 {
        let offset = SCHEMA_ID_OFFSET;
        LittleEndian::read_i16(&self.data[offset..offset + SCHEMA_ID_OFFSET])
    }

    pub fn base_log_offset(&self) -> i64 {
        let offset = BASE_OFFSET_OFFSET;
        LittleEndian::read_i64(&self.data[offset..offset + BASE_OFFSET_LENGTH])
    }

    pub fn last_log_offset(&self) -> i64 {
        self.base_log_offset() + self.last_offset_delta() as i64
    }

    fn last_offset_delta(&self) -> i32 {
        let offset = LAST_OFFSET_DELTA_OFFSET;
        LittleEndian::read_i32(&self.data[offset..offset + LAST_OFFSET_DELTA_LENGTH])
    }

    pub fn size_in_bytes(&self) -> usize {
        let offset = LENGTH_OFFSET;
        LittleEndian::read_i32(&self.data[offset..offset + LENGTH_LENGTH]) as usize + LOG_OVERHEAD
    }

    pub fn record_count(&self) -> i32 {
        let offset = RECORDS_COUNT_OFFSET;
        LittleEndian::read_i32(&self.data[offset..offset + RECORDS_COUNT_LENGTH])
    }

    pub fn records(&self, read_context: ReadContext) -> LogRecordIterator {
        let count = self.record_count();
        if count == 0 {
            return LogRecordIterator::empty();
        }

        // get arrow_metadata
        let arrow_metadata_bytes = read_context.to_arrow_metadata().unwrap();
        // arrow_batch_data
        let data = &self.data[RECORDS_OFFSET..];

        // need to combine arrow_metadata_bytes + arrow_batch_data
        let cursor = Cursor::new([&arrow_metadata_bytes, data].concat());
        let mut stream_reader = StreamReader::try_new(cursor, None).unwrap();

        let mut record_batch = None;
        if let Some(bath) = stream_reader.next() {
            record_batch = Some(bath.unwrap());
        }

        if record_batch.is_none() {
            return LogRecordIterator::empty();
        }

        let arrow_reader = ArrowReader::new(Arc::new(record_batch.unwrap()));
        LogRecordIterator::Arrow(ArrowLogRecordIterator {
            reader: arrow_reader,
            base_offset: self.base_log_offset(),
            timestamp: self.commit_timestamp(),
            row_id: 0,
            change_type: ChangeType::AppendOnly,
        })
    }
}

pub fn to_arrow_schema(fluss_schema: &DataType) -> SchemaRef {
    match &fluss_schema {
        DataType::Row(row_type) => {
            let fields: Vec<Field> = row_type
                .fields()
                .iter()
                .map(|f| {
                    Field::new(
                        f.name(),
                        to_arrow_type(f.data_type()),
                        f.data_type().is_nullable(),
                    )
                })
                .collect();

            SchemaRef::new(arrow_schema::Schema::new(fields))
        }
        _ => {
            panic!("must be row data tyoe.")
        }
    }
}

pub fn to_arrow_type(fluss_type: &DataType) -> ArrowDataType {
    match fluss_type {
        DataType::Boolean(_) => ArrowDataType::Boolean,
        DataType::TinyInt(_) => ArrowDataType::Int8,
        DataType::SmallInt(_) => ArrowDataType::Int16,
        DataType::BigInt(_) => ArrowDataType::Int64,
        DataType::Int(_) => ArrowDataType::Int32,
        DataType::Float(_) => ArrowDataType::Float32,
        DataType::Double(_) => ArrowDataType::Float64,
        DataType::Char(_) => ArrowDataType::Utf8,
        DataType::String(_) => ArrowDataType::Utf8,
        DataType::Decimal(_) => todo!(),
        DataType::Date(_) => ArrowDataType::Date32,
        DataType::Time(_) => todo!(),
        DataType::Timestamp(_) => todo!(),
        DataType::TimestampLTz(_) => todo!(),
        DataType::Bytes(_) => todo!(),
        DataType::Binary(_) => todo!(),
        DataType::Array(_data_type) => todo!(),
        DataType::Map(_data_type) => todo!(),
        DataType::Row(_data_fields) => todo!(),
    }
}

pub struct ReadContext {
    arrow_schema: SchemaRef,
}

impl ReadContext {
    pub fn new(arrow_schema: SchemaRef) -> ReadContext {
        ReadContext { arrow_schema }
    }

    pub fn to_arrow_metadata(&self) -> Result<Vec<u8>> {
        let mut arrow_schema_bytes = vec![];
        let _writer = StreamWriter::try_new(&mut arrow_schema_bytes, &self.arrow_schema)?;
        Ok(arrow_schema_bytes)
    }
}

pub enum LogRecordIterator {
    Empty,
    Arrow(ArrowLogRecordIterator),
}

impl LogRecordIterator {
    pub fn empty() -> Self {
        LogRecordIterator::Empty
    }
}

impl Iterator for LogRecordIterator {
    type Item = ScanRecord;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            LogRecordIterator::Empty => None,
            LogRecordIterator::Arrow(iter) => iter.next(),
        }
    }
}

pub struct ArrowLogRecordIterator {
    reader: ArrowReader,
    base_offset: i64,
    timestamp: i64,
    row_id: usize,
    change_type: ChangeType,
}

#[allow(dead_code)]
impl ArrowLogRecordIterator {
    fn new(reader: ArrowReader, base_offset: i64, timestamp: i64, change_type: ChangeType) -> Self {
        Self {
            reader,
            base_offset,
            timestamp,
            row_id: 0,
            change_type,
        }
    }
}

impl Iterator for ArrowLogRecordIterator {
    type Item = ScanRecord;

    fn next(&mut self) -> Option<Self::Item> {
        if self.row_id >= self.reader.row_count() {
            return None;
        }

        let columnar_row = self.reader.read(self.row_id);
        let scan_record = ScanRecord::new(
            columnar_row,
            self.base_offset + self.row_id as i64,
            self.timestamp,
            self.change_type,
        );
        self.row_id += 1;
        Some(scan_record)
    }
}

pub struct ArrowReader {
    record_batch: Arc<RecordBatch>,
}

impl ArrowReader {
    pub fn new(record_batch: Arc<RecordBatch>) -> Self {
        ArrowReader { record_batch }
    }

    pub fn row_count(&self) -> usize {
        self.record_batch.num_rows()
    }

    pub fn read(&self, row_id: usize) -> ColumnarRow {
        ColumnarRow::new_with_row_id(self.record_batch.clone(), row_id)
    }
}
pub struct MyVec<T>(pub StreamReader<T>);
