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

use crate::client::{Record, WriteRecord};
use crate::compression::ArrowCompressionInfo;
use crate::error::{Error, Result};
use crate::metadata::DataType;
use crate::record::{ChangeType, ScanRecord};
use crate::row::{ColumnarRow, GenericRow};
use arrow::array::{
    ArrayBuilder, ArrayRef, BinaryBuilder, BooleanBuilder, Float32Builder, Float64Builder,
    Int8Builder, Int16Builder, Int32Builder, Int64Builder, StringBuilder, UInt8Builder,
    UInt16Builder, UInt32Builder, UInt64Builder,
};
use arrow::{
    array::RecordBatch,
    buffer::Buffer,
    ipc::{
        reader::{StreamReader, read_record_batch},
        root_as_message,
        writer::StreamWriter,
    },
};
use arrow_schema::ArrowError::ParseError;
use arrow_schema::SchemaRef;
use arrow_schema::{DataType as ArrowDataType, Field};
use byteorder::WriteBytesExt;
use byteorder::{ByteOrder, LittleEndian};
use bytes::Bytes;
use crc32c::crc32c;
use parking_lot::Mutex;
use std::{
    io::{Cursor, Write},
    sync::Arc,
};

use arrow::ipc::writer::IpcWriteOptions;
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

// NOTE: Rust layout/offsets currently match Java only for V0.
// TODO: Add V1 layout/offsets to keep parity with Java's V1 format.
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
    arrow_record_batch_builder: Box<dyn ArrowRecordBatchInnerBuilder>,
    is_closed: bool,
    arrow_compression_info: ArrowCompressionInfo,
}

pub trait ArrowRecordBatchInnerBuilder: Send + Sync {
    fn build_arrow_record_batch(&self) -> Result<Arc<RecordBatch>>;

    fn append(&mut self, row: &GenericRow) -> Result<bool>;

    fn append_batch(&mut self, record_batch: Arc<RecordBatch>) -> Result<bool>;

    fn schema(&self) -> SchemaRef;

    fn records_count(&self) -> i32;

    fn is_full(&self) -> bool;
}

#[derive(Default)]
pub struct PrebuiltRecordBatchBuilder {
    arrow_record_batch: Option<Arc<RecordBatch>>,
    records_count: i32,
}

impl ArrowRecordBatchInnerBuilder for PrebuiltRecordBatchBuilder {
    fn build_arrow_record_batch(&self) -> Result<Arc<RecordBatch>> {
        Ok(self.arrow_record_batch.as_ref().unwrap().clone())
    }

    fn append(&mut self, _row: &GenericRow) -> Result<bool> {
        // append one single row is not supported, return false directly
        Ok(false)
    }

    fn append_batch(&mut self, record_batch: Arc<RecordBatch>) -> Result<bool> {
        if self.arrow_record_batch.is_some() {
            return Ok(false);
        }
        self.records_count = record_batch.num_rows() as i32;
        self.arrow_record_batch = Some(record_batch);
        Ok(true)
    }

    fn schema(&self) -> SchemaRef {
        self.arrow_record_batch.as_ref().unwrap().schema()
    }

    fn records_count(&self) -> i32 {
        self.records_count
    }

    fn is_full(&self) -> bool {
        // full if has one record batch
        self.arrow_record_batch.is_some()
    }
}

pub struct RowAppendRecordBatchBuilder {
    table_schema: SchemaRef,
    arrow_column_builders: Mutex<Vec<Box<dyn ArrayBuilder>>>,
    records_count: i32,
}

impl RowAppendRecordBatchBuilder {
    pub fn new(row_type: &DataType) -> Self {
        let schema_ref = to_arrow_schema(row_type);
        let builders = Mutex::new(
            schema_ref
                .fields()
                .iter()
                .map(|field| Self::create_builder(field.data_type()))
                .collect(),
        );
        Self {
            table_schema: schema_ref.clone(),
            arrow_column_builders: builders,
            records_count: 0,
        }
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
            arrow_schema::DataType::Binary => Box::new(BinaryBuilder::new()),
            dt => panic!("Unsupported data type: {dt:?}"),
        }
    }
}

impl ArrowRecordBatchInnerBuilder for RowAppendRecordBatchBuilder {
    fn build_arrow_record_batch(&self) -> Result<Arc<RecordBatch>> {
        let arrays = self
            .arrow_column_builders
            .lock()
            .iter_mut()
            .map(|b| b.finish())
            .collect::<Vec<ArrayRef>>();
        Ok(Arc::new(RecordBatch::try_new(
            self.table_schema.clone(),
            arrays,
        )?))
    }

    fn append(&mut self, row: &GenericRow) -> Result<bool> {
        for (idx, value) in row.values.iter().enumerate() {
            let mut builder_binding = self.arrow_column_builders.lock();
            let builder = builder_binding.get_mut(idx).unwrap();
            value.append_to(builder.as_mut())?;
        }
        self.records_count += 1;
        Ok(true)
    }

    fn append_batch(&mut self, _record_batch: Arc<RecordBatch>) -> Result<bool> {
        Ok(false)
    }

    fn schema(&self) -> SchemaRef {
        self.table_schema.clone()
    }

    fn records_count(&self) -> i32 {
        self.records_count
    }

    fn is_full(&self) -> bool {
        self.records_count() >= DEFAULT_MAX_RECORD
    }
}

impl MemoryLogRecordsArrowBuilder {
    pub fn new(
        schema_id: i32,
        row_type: &DataType,
        to_append_record_batch: bool,
        arrow_compression_info: ArrowCompressionInfo,
    ) -> Self {
        let arrow_batch_builder: Box<dyn ArrowRecordBatchInnerBuilder> = {
            if to_append_record_batch {
                Box::new(PrebuiltRecordBatchBuilder::default())
            } else {
                Box::new(RowAppendRecordBatchBuilder::new(row_type))
            }
        };
        MemoryLogRecordsArrowBuilder {
            base_log_offset: BUILDER_DEFAULT_OFFSET,
            schema_id,
            magic: CURRENT_LOG_MAGIC_VALUE,
            writer_id: NO_WRITER_ID,
            batch_sequence: NO_BATCH_SEQUENCE,
            is_closed: false,
            arrow_record_batch_builder: arrow_batch_builder,
            arrow_compression_info,
        }
    }

    pub fn append(&mut self, record: &WriteRecord) -> Result<bool> {
        match &record.row {
            Record::Row(row) => Ok(self.arrow_record_batch_builder.append(row)?),
            Record::RecordBatch(record_batch) => Ok(self
                .arrow_record_batch_builder
                .append_batch(record_batch.clone())?),
        }
        // todo: consider write other change type
    }

    pub fn is_full(&self) -> bool {
        self.arrow_record_batch_builder.records_count() >= DEFAULT_MAX_RECORD
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
        let table_schema = self.arrow_record_batch_builder.schema();
        let compression_type = self.arrow_compression_info.get_compression_type();
        let write_option =
            IpcWriteOptions::try_with_compression(IpcWriteOptions::default(), compression_type);
        let mut writer = StreamWriter::try_new_with_options(
            &mut arrow_batch_bytes,
            &table_schema,
            write_option?,
        )?;

        // get header len
        let header = writer.get_ref().len();
        let record_batch = self.arrow_record_batch_builder.build_arrow_record_batch()?;
        writer.write(record_batch.as_ref())?;
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

        let record_count = self.arrow_record_batch_builder.records_count();
        // todo: curerntly, always is append only
        let append_only = true;
        cursor.write_u8(if append_only { 1 } else { 0 })?;
        cursor.write_i32::<LittleEndian>(if record_count > 0 {
            record_count - 1
        } else {
            0
        })?;

        cursor.write_i64::<LittleEndian>(self.writer_id)?;
        cursor.write_i32::<LittleEndian>(self.batch_sequence)?;
        cursor.write_i32::<LittleEndian>(record_count)?;
        Ok(())
    }
}

pub trait ToArrow {
    fn append_to(&self, builder: &mut dyn ArrayBuilder) -> Result<()>;
}

pub struct LogRecordsBatches {
    data: Bytes,
    current_pos: usize,
    remaining_bytes: usize,
}

impl LogRecordsBatches {
    pub fn new(data: Vec<u8>) -> Self {
        let remaining_bytes: usize = data.len();
        Self {
            data: Bytes::from(data),
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
        let batch_size = batch_size_bytes as usize + LOG_OVERHEAD;
        if batch_size > self.remaining_bytes {
            return None;
        }
        Some(batch_size)
    }
}

impl Iterator for LogRecordsBatches {
    type Item = LogRecordBatch;

    fn next(&mut self) -> Option<Self::Item> {
        match self.next_batch_size() {
            Some(batch_size) => {
                let start = self.current_pos;
                let end = start + batch_size;
                // Since LogRecordsBatches owns the Vec<u8>, the slice is valid
                // as long as the mutable reference exists, which is 'a
                let record_batch = LogRecordBatch::new(self.data.slice(start..end));
                self.current_pos += batch_size;
                self.remaining_bytes -= batch_size;
                Some(record_batch)
            }
            None => None,
        }
    }
}

pub struct LogRecordBatch {
    data: Bytes,
}

#[allow(dead_code)]
impl LogRecordBatch {
    pub fn new(data: Bytes) -> Self {
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
        // TODO enable validation once checksum handling is corrected.
        Ok(())
    }

    pub fn is_valid(&self) -> bool {
        self.size_in_bytes() >= RECORD_BATCH_HEADER_SIZE
            && self.checksum() == self.compute_checksum()
    }

    fn compute_checksum(&self) -> u32 {
        let start = SCHEMA_ID_OFFSET;
        crc32c(&self.data[start..])
    }

    fn attributes(&self) -> u8 {
        self.data[ATTRIBUTES_OFFSET]
    }

    pub fn next_log_offset(&self) -> i64 {
        self.last_log_offset() + 1
    }

    pub fn checksum(&self) -> u32 {
        let offset = CRC_OFFSET;
        LittleEndian::read_u32(&self.data[offset..offset + CRC_LENGTH])
    }

    pub fn schema_id(&self) -> i16 {
        let offset = SCHEMA_ID_OFFSET;
        LittleEndian::read_i16(&self.data[offset..offset + SCHEMA_ID_LENGTH])
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

    pub fn records(&self, read_context: &ReadContext) -> Result<LogRecordIterator> {
        if self.record_count() == 0 {
            return Ok(LogRecordIterator::empty());
        }

        let data = &self.data[RECORDS_OFFSET..];

        let record_batch = read_context.record_batch(data)?;
        let arrow_reader = ArrowReader::new(Arc::new(record_batch));
        let log_record_iterator = LogRecordIterator::Arrow(ArrowLogRecordIterator {
            reader: arrow_reader,
            base_offset: self.base_log_offset(),
            timestamp: self.commit_timestamp(),
            row_id: 0,
            change_type: ChangeType::AppendOnly,
        });

        Ok(log_record_iterator)
    }

    pub fn records_for_remote_log(&self, read_context: &ReadContext) -> Result<LogRecordIterator> {
        if self.record_count() == 0 {
            return Ok(LogRecordIterator::empty());
        }

        let data = &self.data[RECORDS_OFFSET..];

        let record_batch = read_context.record_batch_for_remote_log(data)?;
        let log_record_iterator = match record_batch {
            None => LogRecordIterator::empty(),
            Some(record_batch) => {
                let arrow_reader = ArrowReader::new(Arc::new(record_batch));
                LogRecordIterator::Arrow(ArrowLogRecordIterator {
                    reader: arrow_reader,
                    base_offset: self.base_log_offset(),
                    timestamp: self.commit_timestamp(),
                    row_id: 0,
                    change_type: ChangeType::AppendOnly,
                })
            }
        };
        Ok(log_record_iterator)
    }

    /// Returns the record batch directly without creating an iterator.
    /// This is more efficient when you need the entire batch rather than
    /// iterating row-by-row.
    pub fn record_batch(&self, read_context: &ReadContext) -> Result<RecordBatch> {
        if self.record_count() == 0 {
            // Return empty batch with correct schema
            return Ok(RecordBatch::new_empty(read_context.target_schema.clone()));
        }

        let data = self.data.get(RECORDS_OFFSET..).ok_or_else(|| {
            crate::error::Error::UnexpectedError {
                message: format!(
                    "Corrupt log record batch: data length {} is less than RECORDS_OFFSET {}",
                    self.data.len(),
                    RECORDS_OFFSET
                ),
                source: None,
            }
        })?;
        read_context.record_batch(data)
    }
}

/// Parse an Arrow IPC message from a byte slice.
///
/// Server returns RecordBatch message (without Schema message) in the encapsulated message format.
/// Format: [continuation: 4 bytes (0xFFFFFFFF)][metadata_size: 4 bytes][RecordBatch metadata][body]
///
/// This format is documented at:
/// https://arrow.apache.org/docs/format/Columnar.html#encapsulated-message-format
///
/// # Arguments
/// * `data` - The byte slice containing the IPC message.
///
/// # Returns
/// Returns `Ok((batch_metadata, body_buffer, version))` on success:
/// - `batch_metadata`: The RecordBatch metadata from the IPC message.
/// - `body_buffer`: The buffer containing the record batch body data.
/// - `version`: The Arrow IPC metadata version.
///
/// Returns `Err(arrow_error)` on errors
/// - `arrow_error`: Error details e.g. malformed, too short or bad continuation marker.
fn parse_ipc_message(
    data: &[u8],
) -> Result<(
    arrow::ipc::RecordBatch<'_>,
    Buffer,
    arrow::ipc::MetadataVersion,
)> {
    const CONTINUATION_MARKER: u32 = 0xFFFFFFFF;

    if data.len() < 8 {
        Err(ParseError(format!("Invalid data length: {}", data.len())))?
    }

    let continuation = LittleEndian::read_u32(&data[0..4]);
    let metadata_size = LittleEndian::read_u32(&data[4..8]) as usize;

    if continuation != CONTINUATION_MARKER {
        Err(ParseError(format!(
            "Invalid continuation marker: {continuation}"
        )))?
    }

    if data.len() < 8 + metadata_size {
        Err(ParseError(format!(
            "Invalid data length. Remaining data length {} is shorter than specified size {}",
            data.len() - 8,
            metadata_size
        )))?
    }

    let metadata_bytes = &data[8..8 + metadata_size];
    let message = root_as_message(metadata_bytes).map_err(|err| ParseError(err.to_string()))?;
    let batch_metadata = message
        .header_as_record_batch()
        .ok_or(ParseError(String::from("Not a record batch")))?;

    let metadata_padded_size = (metadata_size + 7) & !7;
    let body_start = 8 + metadata_padded_size;
    let body_data = &data[body_start..];
    let body_buffer = Buffer::from(body_data);

    Ok((batch_metadata, body_buffer, message.version()))
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
            panic!("must be row data type.")
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
        DataType::Decimal(decimal_type) => ArrowDataType::Decimal128(
            decimal_type
                .precision()
                .try_into()
                .expect("precision exceeds u8::MAX"),
            decimal_type
                .scale()
                .try_into()
                .expect("scale exceeds i8::MAX"),
        ),
        DataType::Date(_) => ArrowDataType::Date32,
        DataType::Time(time_type) => match time_type.precision() {
            0 => ArrowDataType::Time32(arrow_schema::TimeUnit::Second),
            1..=3 => ArrowDataType::Time32(arrow_schema::TimeUnit::Millisecond),
            4..=6 => ArrowDataType::Time64(arrow_schema::TimeUnit::Microsecond),
            7..=9 => ArrowDataType::Time64(arrow_schema::TimeUnit::Nanosecond),
            // This arm should never be reached due to validation in TimeType.
            invalid => panic!("Invalid precision value for TimeType: {invalid}"),
        },
        DataType::Timestamp(timestamp_type) => match timestamp_type.precision() {
            0 => ArrowDataType::Timestamp(arrow_schema::TimeUnit::Second, None),
            1..=3 => ArrowDataType::Timestamp(arrow_schema::TimeUnit::Millisecond, None),
            4..=6 => ArrowDataType::Timestamp(arrow_schema::TimeUnit::Microsecond, None),
            7..=9 => ArrowDataType::Timestamp(arrow_schema::TimeUnit::Nanosecond, None),
            // This arm should never be reached due to validation in Timestamp.
            invalid => panic!("Invalid precision value for TimestampType: {invalid}"),
        },
        DataType::TimestampLTz(timestamp_ltz_type) => match timestamp_ltz_type.precision() {
            0 => ArrowDataType::Timestamp(arrow_schema::TimeUnit::Second, None),
            1..=3 => ArrowDataType::Timestamp(arrow_schema::TimeUnit::Millisecond, None),
            4..=6 => ArrowDataType::Timestamp(arrow_schema::TimeUnit::Microsecond, None),
            7..=9 => ArrowDataType::Timestamp(arrow_schema::TimeUnit::Nanosecond, None),
            // This arm should never be reached due to validation in TimestampLTz.
            invalid => panic!("Invalid precision value for TimestampLTzType: {invalid}"),
        },
        DataType::Bytes(_) => ArrowDataType::Binary,
        DataType::Binary(binary_type) => ArrowDataType::FixedSizeBinary(
            binary_type
                .length()
                .try_into()
                .expect("length exceeds i32::MAX"),
        ),
        DataType::Array(array_type) => ArrowDataType::List(
            Field::new_list_field(
                to_arrow_type(array_type.get_element_type()),
                fluss_type.is_nullable(),
            )
            .into(),
        ),
        DataType::Map(map_type) => {
            let key_type = to_arrow_type(map_type.key_type());
            let value_type = to_arrow_type(map_type.value_type());
            let entry_fields = vec![
                Field::new("key", key_type, map_type.key_type().is_nullable()),
                Field::new("value", value_type, map_type.value_type().is_nullable()),
            ];
            ArrowDataType::Map(
                Arc::new(Field::new(
                    "entries",
                    ArrowDataType::Struct(arrow_schema::Fields::from(entry_fields)),
                    fluss_type.is_nullable(),
                )),
                false,
            )
        }
        DataType::Row(row_type) => ArrowDataType::Struct(arrow_schema::Fields::from(
            row_type
                .fields()
                .iter()
                .map(|f| {
                    Field::new(
                        f.name(),
                        to_arrow_type(f.data_type()),
                        f.data_type().is_nullable(),
                    )
                })
                .collect::<Vec<Field>>(),
        )),
    }
}

#[derive(Clone)]
pub struct ReadContext {
    target_schema: SchemaRef,
    full_schema: SchemaRef,
    projection: Option<Projection>,
    is_from_remote: bool,
}

#[derive(Clone)]
struct Projection {
    ordered_schema: SchemaRef,
    projected_fields: Vec<usize>,
    ordered_fields: Vec<usize>,

    reordering_indexes: Vec<usize>,
    reordering_needed: bool,
}

impl ReadContext {
    pub fn new(arrow_schema: SchemaRef, is_from_remote: bool) -> ReadContext {
        ReadContext {
            target_schema: arrow_schema.clone(),
            full_schema: arrow_schema,
            projection: None,
            is_from_remote,
        }
    }

    pub fn with_projection_pushdown(
        arrow_schema: SchemaRef,
        projected_fields: Vec<usize>,
        is_from_remote: bool,
    ) -> Result<ReadContext> {
        Self::validate_projection(&arrow_schema, projected_fields.as_slice())?;
        let target_schema =
            Self::project_schema(arrow_schema.clone(), projected_fields.as_slice())?;
        // the logic is little bit of hard to understand, to refactor it to follow
        // java side
        let (need_do_reorder, sorted_fields) = {
            // currently, for remote read, arrow log doesn't support projection pushdown,
            // so, only need to do reordering when is not from remote
            if !is_from_remote {
                let mut sorted_fields = projected_fields.clone();
                sorted_fields.sort_unstable();
                (!sorted_fields.eq(&projected_fields), sorted_fields)
            } else {
                // sorted_fields won't be used when need_do_reorder is false,
                // let's use an empty vec directly
                (false, vec![])
            }
        };

        let project = {
            if need_do_reorder {
                // reordering is required
                // Calculate reordering indexes to transform from sorted order to user-requested order
                let mut reordering_indexes = Vec::with_capacity(projected_fields.len());
                for &original_idx in &projected_fields {
                    let pos = sorted_fields.binary_search(&original_idx).map_err(|_| {
                        Error::IllegalArgument {
                            message: format!(
                                "Projection index {original_idx} is invalid for the current schema."
                            ),
                        }
                    })?;
                    reordering_indexes.push(pos);
                }
                Projection {
                    ordered_schema: Self::project_schema(
                        arrow_schema.clone(),
                        sorted_fields.as_slice(),
                    )?,
                    projected_fields,
                    ordered_fields: sorted_fields,
                    reordering_indexes,
                    reordering_needed: true,
                }
            } else {
                Projection {
                    ordered_schema: Self::project_schema(
                        arrow_schema.clone(),
                        projected_fields.as_slice(),
                    )?,
                    ordered_fields: projected_fields.clone(),
                    projected_fields,
                    reordering_indexes: vec![],
                    reordering_needed: false,
                }
            }
        };

        Ok(ReadContext {
            target_schema,
            full_schema: arrow_schema,
            projection: Some(project),
            is_from_remote,
        })
    }

    fn validate_projection(schema: &SchemaRef, projected_fields: &[usize]) -> Result<()> {
        let field_count = schema.fields().len();
        for &index in projected_fields {
            if index >= field_count {
                return Err(Error::IllegalArgument {
                    message: format!(
                        "Projection index {index} is out of bounds for schema with {field_count} fields."
                    ),
                });
            }
        }
        Ok(())
    }

    pub fn project_schema(schema: SchemaRef, projected_fields: &[usize]) -> Result<SchemaRef> {
        Ok(SchemaRef::new(schema.project(projected_fields).map_err(
            |e| Error::IllegalArgument {
                message: format!("Invalid projection: {e}"),
            },
        )?))
    }

    pub fn project_fields(&self) -> Option<&[usize]> {
        self.projection
            .as_ref()
            .map(|p| p.projected_fields.as_slice())
    }

    pub fn project_fields_in_order(&self) -> Option<&[usize]> {
        self.projection
            .as_ref()
            .map(|p| p.ordered_fields.as_slice())
    }

    pub fn record_batch(&self, data: &[u8]) -> Result<RecordBatch> {
        let (batch_metadata, body_buffer, version) = parse_ipc_message(data)?;

        let resolve_schema = {
            // if from remote, no projection, need to use full schema
            if self.is_from_remote {
                self.full_schema.clone()
            } else {
                // the record batch from server must be ordered by field pos,
                // according to project to decide what arrow schema to use
                // to parse the record batch
                match self.projection {
                    Some(ref projection) => {
                        // projection, should use ordered schema by project field pos
                        projection.ordered_schema.clone()
                    }
                    None => {
                        // no projection, use target output schema
                        self.target_schema.clone()
                    }
                }
            }
        };

        let record_batch = read_record_batch(
            &body_buffer,
            batch_metadata,
            resolve_schema,
            &std::collections::HashMap::new(),
            None,
            &version,
        )?;

        let record_batch = match &self.projection {
            Some(projection) => {
                let reordered_columns = {
                    // need to do reorder
                    if self.is_from_remote {
                        Some(&projection.projected_fields)
                    } else if projection.reordering_needed {
                        Some(&projection.reordering_indexes)
                    } else {
                        None
                    }
                };
                match reordered_columns {
                    Some(reordered_columns) => {
                        let arrow_columns = reordered_columns
                            .iter()
                            .map(|&idx| record_batch.column(idx).clone())
                            .collect();
                        RecordBatch::try_new(self.target_schema.clone(), arrow_columns)?
                    }
                    _ => record_batch,
                }
            }
            _ => record_batch,
        };
        Ok(record_batch)
    }

    pub fn record_batch_for_remote_log(&self, data: &[u8]) -> Result<Option<RecordBatch>> {
        let (batch_metadata, body_buffer, version) = parse_ipc_message(data)?;

        let record_batch = read_record_batch(
            &body_buffer,
            batch_metadata,
            self.full_schema.clone(),
            &std::collections::HashMap::new(),
            None,
            &version,
        )?;

        let record_batch = match &self.projection {
            Some(projection) => {
                let projected_columns: Vec<_> = projection
                    .projected_fields
                    .iter()
                    .map(|&idx| record_batch.column(idx).clone())
                    .collect();
                RecordBatch::try_new(self.target_schema.clone(), projected_columns)?
            }
            None => record_batch,
        };
        Ok(Some(record_batch))
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::Error;
    use crate::metadata::DataField;
    use crate::metadata::DataTypes;

    #[test]
    fn test_to_array_type() {
        assert_eq!(to_arrow_type(&DataTypes::boolean()), ArrowDataType::Boolean);
        assert_eq!(to_arrow_type(&DataTypes::tinyint()), ArrowDataType::Int8);
        assert_eq!(to_arrow_type(&DataTypes::smallint()), ArrowDataType::Int16);
        assert_eq!(to_arrow_type(&DataTypes::bigint()), ArrowDataType::Int64);
        assert_eq!(to_arrow_type(&DataTypes::int()), ArrowDataType::Int32);
        assert_eq!(to_arrow_type(&DataTypes::float()), ArrowDataType::Float32);
        assert_eq!(to_arrow_type(&DataTypes::double()), ArrowDataType::Float64);
        assert_eq!(to_arrow_type(&DataTypes::char(16)), ArrowDataType::Utf8);
        assert_eq!(to_arrow_type(&DataTypes::string()), ArrowDataType::Utf8);
        assert_eq!(
            to_arrow_type(&DataTypes::decimal(10, 2)),
            ArrowDataType::Decimal128(10, 2)
        );
        assert_eq!(to_arrow_type(&DataTypes::date()), ArrowDataType::Date32);
        assert_eq!(
            to_arrow_type(&DataTypes::time()),
            ArrowDataType::Time32(arrow_schema::TimeUnit::Second)
        );
        assert_eq!(
            to_arrow_type(&DataTypes::time_with_precision(3)),
            ArrowDataType::Time32(arrow_schema::TimeUnit::Millisecond)
        );
        assert_eq!(
            to_arrow_type(&DataTypes::time_with_precision(6)),
            ArrowDataType::Time64(arrow_schema::TimeUnit::Microsecond)
        );
        assert_eq!(
            to_arrow_type(&DataTypes::time_with_precision(9)),
            ArrowDataType::Time64(arrow_schema::TimeUnit::Nanosecond)
        );
        assert_eq!(
            to_arrow_type(&DataTypes::timestamp_with_precision(0)),
            ArrowDataType::Timestamp(arrow_schema::TimeUnit::Second, None)
        );
        assert_eq!(
            to_arrow_type(&DataTypes::timestamp_with_precision(3)),
            ArrowDataType::Timestamp(arrow_schema::TimeUnit::Millisecond, None)
        );
        assert_eq!(
            to_arrow_type(&DataTypes::timestamp_with_precision(6)),
            ArrowDataType::Timestamp(arrow_schema::TimeUnit::Microsecond, None)
        );
        assert_eq!(
            to_arrow_type(&DataTypes::timestamp_with_precision(9)),
            ArrowDataType::Timestamp(arrow_schema::TimeUnit::Nanosecond, None)
        );
        assert_eq!(
            to_arrow_type(&DataTypes::timestamp_ltz_with_precision(0)),
            ArrowDataType::Timestamp(arrow_schema::TimeUnit::Second, None)
        );
        assert_eq!(
            to_arrow_type(&DataTypes::timestamp_ltz_with_precision(3)),
            ArrowDataType::Timestamp(arrow_schema::TimeUnit::Millisecond, None)
        );
        assert_eq!(
            to_arrow_type(&DataTypes::timestamp_ltz_with_precision(6)),
            ArrowDataType::Timestamp(arrow_schema::TimeUnit::Microsecond, None)
        );
        assert_eq!(
            to_arrow_type(&DataTypes::timestamp_ltz_with_precision(9)),
            ArrowDataType::Timestamp(arrow_schema::TimeUnit::Nanosecond, None)
        );
        assert_eq!(to_arrow_type(&DataTypes::bytes()), ArrowDataType::Binary);
        assert_eq!(
            to_arrow_type(&DataTypes::binary(16)),
            ArrowDataType::FixedSizeBinary(16)
        );

        assert_eq!(
            to_arrow_type(&DataTypes::array(DataTypes::int())),
            ArrowDataType::List(Field::new_list_field(ArrowDataType::Int32, true).into())
        );

        assert_eq!(
            to_arrow_type(&DataTypes::map(DataTypes::string(), DataTypes::int())),
            ArrowDataType::Map(
                Arc::new(Field::new(
                    "entries",
                    ArrowDataType::Struct(arrow_schema::Fields::from(vec![
                        Field::new("key", ArrowDataType::Utf8, true),
                        Field::new("value", ArrowDataType::Int32, true),
                    ])),
                    true,
                )),
                false,
            )
        );

        assert_eq!(
            to_arrow_type(&DataTypes::row(vec![
                DataTypes::field("f1".to_string(), DataTypes::int()),
                DataTypes::field("f2".to_string(), DataTypes::string()),
            ])),
            ArrowDataType::Struct(arrow_schema::Fields::from(vec![
                Field::new("f1", ArrowDataType::Int32, true),
                Field::new("f2", ArrowDataType::Utf8, true),
            ]))
        );
    }

    #[test]
    #[should_panic(expected = "Invalid precision value for TimeType: 10")]
    fn test_time_invalid_precision() {
        to_arrow_type(&DataTypes::time_with_precision(10));
    }

    #[test]
    #[should_panic(expected = "Invalid precision value for TimestampType: 10")]
    fn test_timestamp_invalid_precision() {
        to_arrow_type(&DataTypes::timestamp_with_precision(10));
    }

    #[test]
    #[should_panic(expected = "Invalid precision value for TimestampLTzType: 10")]
    fn test_timestamp_ltz_invalid_precision() {
        to_arrow_type(&DataTypes::timestamp_ltz_with_precision(10));
    }

    #[test]
    fn test_parse_ipc_message() {
        let empty_body: &[u8] = &le_bytes(&[0xFFFFFFFF, 0x00000000]);
        let result = parse_ipc_message(empty_body);
        assert_eq!(
            result.unwrap_err().to_string(),
            String::from(
                "Fluss hitting Arrow error Parser error: Range [0, 4) is out of bounds.\n\n: ParseError(\"Range [0, 4) is out of bounds.\\n\\n\")."
            )
        );

        let invalid_data = &[];
        assert_eq!(
            parse_ipc_message(invalid_data).unwrap_err().to_string(),
            String::from(
                "Fluss hitting Arrow error Parser error: Invalid data length: 0: ParseError(\"Invalid data length: 0\")."
            )
        );

        let data_with_invalid_continuation: &[u8] = &le_bytes(&[0x00000001, 0x00000000]);
        assert_eq!(
            parse_ipc_message(data_with_invalid_continuation)
                .unwrap_err()
                .to_string(),
            String::from(
                "Fluss hitting Arrow error Parser error: Invalid continuation marker: 1: ParseError(\"Invalid continuation marker: 1\")."
            )
        );

        let data_with_invalid_length: &[u8] = &le_bytes(&[0xFFFFFFFF, 0x00000001]);
        assert_eq!(
            parse_ipc_message(data_with_invalid_length)
                .unwrap_err()
                .to_string(),
            String::from(
                "Fluss hitting Arrow error Parser error: Invalid data length. Remaining data length 0 is shorter than specified size 1: ParseError(\"Invalid data length. Remaining data length 0 is shorter than specified size 1\")."
            )
        );

        let data_with_invalid_length = &le_bytes(&[0xFFFFFFFF, 0x00000004, 0x00000000]);
        assert_eq!(
            parse_ipc_message(data_with_invalid_length)
                .unwrap_err()
                .to_string(),
            String::from(
                "Fluss hitting Arrow error Parser error: Not a record batch: ParseError(\"Not a record batch\")."
            )
        );
    }

    #[test]
    fn projection_rejects_out_of_bounds_index() {
        let row_type = DataTypes::row(vec![
            DataField::new("id".to_string(), DataTypes::int(), None),
            DataField::new("name".to_string(), DataTypes::string(), None),
        ]);
        let schema = to_arrow_schema(&row_type);
        let result = ReadContext::with_projection_pushdown(schema, vec![0, 2], false);

        assert!(matches!(result, Err(Error::IllegalArgument { .. })));
    }

    #[test]
    fn checksum_and_schema_id_read_minimum_header() {
        // Header-only batches with record_count == 0 are valid; this covers the minimal bytes
        // needed for checksum/schema_id access.
        let mut data = vec![0u8; SCHEMA_ID_OFFSET + SCHEMA_ID_LENGTH];
        let crc = 0xA1B2C3D4u32;
        let schema_id = 42i16;
        LittleEndian::write_u32(&mut data[CRC_OFFSET..CRC_OFFSET + CRC_LENGTH], crc);
        LittleEndian::write_i16(
            &mut data[SCHEMA_ID_OFFSET..SCHEMA_ID_OFFSET + SCHEMA_ID_LENGTH],
            schema_id,
        );

        let batch = LogRecordBatch::new(Bytes::from(data));
        assert_eq!(batch.checksum(), crc);
        assert_eq!(batch.schema_id(), schema_id);

        let expected = crc32c(&batch.data[SCHEMA_ID_OFFSET..]);
        assert_eq!(batch.compute_checksum(), expected);
    }

    fn le_bytes(vals: &[u32]) -> Vec<u8> {
        let mut out = Vec::with_capacity(vals.len() * 4);
        for &v in vals {
            out.extend_from_slice(&v.to_le_bytes());
        }
        out
    }
}
