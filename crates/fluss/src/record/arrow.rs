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

use crate::client::{LogWriteRecord, Record, WriteRecord};
use crate::compression::ArrowCompressionInfo;
use crate::error::{Error, Result};
use crate::metadata::{DataType, RowType};
use crate::record::{ChangeType, ScanRecord};
use crate::row::{ColumnarRow, GenericRow};
use arrow::array::{
    ArrayBuilder, ArrayRef, BinaryBuilder, BooleanBuilder, Date32Builder, Decimal128Builder,
    Float32Builder, Float64Builder, Int8Builder, Int16Builder, Int32Builder, Int64Builder,
    StringBuilder, Time32MillisecondBuilder, Time32SecondBuilder, Time64MicrosecondBuilder,
    Time64NanosecondBuilder, TimestampMicrosecondBuilder, TimestampMillisecondBuilder,
    TimestampNanosecondBuilder, TimestampSecondBuilder, UInt8Builder, UInt16Builder, UInt32Builder,
    UInt64Builder,
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
use std::{
    collections::HashMap,
    fs::File,
    io::{Cursor, Read, Seek, SeekFrom, Write},
    path::PathBuf,
    sync::Arc,
};

use crate::error::Error::IllegalArgument;
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

/// Maximum batch size matches Java's Integer.MAX_VALUE limit.
/// Java uses int type for batch size, so max value is 2^31 - 1 = 2,147,483,647 bytes (~2GB).
/// This is the implicit limit in FileLogRecords.java and other Java components.
pub const MAX_BATCH_SIZE: usize = i32::MAX as usize; // 2,147,483,647 bytes (~2GB)

/// const for record
/// The "magic" values.
#[derive(Debug, Clone, Copy)]
pub enum LogMagicValue {
    V0 = 0,
}

/// Safely convert batch size from i32 to usize with validation.
///
/// Validates that:
/// - batch_size_bytes is non-negative
/// - batch_size_bytes + LOG_OVERHEAD doesn't overflow
/// - Result is within reasonable bounds
fn validate_batch_size(batch_size_bytes: i32) -> Result<usize> {
    // Check for negative size (corrupted data)
    if batch_size_bytes < 0 {
        return Err(Error::UnexpectedError {
            message: format!("Invalid negative batch size: {}", batch_size_bytes),
            source: None,
        });
    }

    let batch_size_u = batch_size_bytes as usize;

    // Check for overflow when adding LOG_OVERHEAD
    let total_size =
        batch_size_u
            .checked_add(LOG_OVERHEAD)
            .ok_or_else(|| Error::UnexpectedError {
                message: format!(
                    "Batch size {} + LOG_OVERHEAD {} would overflow",
                    batch_size_u, LOG_OVERHEAD
                ),
                source: None,
            })?;

    // Sanity check: reject unreasonably large batches
    if total_size > MAX_BATCH_SIZE {
        return Err(Error::UnexpectedError {
            message: format!(
                "Batch size {} exceeds maximum allowed size {}",
                total_size, MAX_BATCH_SIZE
            ),
            source: None,
        });
    }

    Ok(total_size)
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
    fn build_arrow_record_batch(&mut self) -> Result<Arc<RecordBatch>>;

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
    fn build_arrow_record_batch(&mut self) -> Result<Arc<RecordBatch>> {
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
    arrow_column_builders: Vec<Box<dyn ArrayBuilder>>,
    records_count: i32,
}

impl RowAppendRecordBatchBuilder {
    pub fn new(row_type: &RowType) -> Result<Self> {
        let schema_ref = to_arrow_schema(row_type)?;
        let builders: Result<Vec<_>> = schema_ref
            .fields()
            .iter()
            .map(|field| Self::create_builder(field.data_type()))
            .collect();
        Ok(Self {
            table_schema: schema_ref.clone(),
            arrow_column_builders: builders?,
            records_count: 0,
        })
    }

    fn create_builder(data_type: &arrow_schema::DataType) -> Result<Box<dyn ArrayBuilder>> {
        match data_type {
            arrow_schema::DataType::Int8 => Ok(Box::new(Int8Builder::new())),
            arrow_schema::DataType::Int16 => Ok(Box::new(Int16Builder::new())),
            arrow_schema::DataType::Int32 => Ok(Box::new(Int32Builder::new())),
            arrow_schema::DataType::Int64 => Ok(Box::new(Int64Builder::new())),
            arrow_schema::DataType::UInt8 => Ok(Box::new(UInt8Builder::new())),
            arrow_schema::DataType::UInt16 => Ok(Box::new(UInt16Builder::new())),
            arrow_schema::DataType::UInt32 => Ok(Box::new(UInt32Builder::new())),
            arrow_schema::DataType::UInt64 => Ok(Box::new(UInt64Builder::new())),
            arrow_schema::DataType::Float32 => Ok(Box::new(Float32Builder::new())),
            arrow_schema::DataType::Float64 => Ok(Box::new(Float64Builder::new())),
            arrow_schema::DataType::Boolean => Ok(Box::new(BooleanBuilder::new())),
            arrow_schema::DataType::Utf8 => Ok(Box::new(StringBuilder::new())),
            arrow_schema::DataType::Binary => Ok(Box::new(BinaryBuilder::new())),
            arrow_schema::DataType::Decimal128(precision, scale) => {
                let builder = Decimal128Builder::new()
                    .with_precision_and_scale(*precision, *scale)
                    .map_err(|e| Error::IllegalArgument {
                        message: format!(
                            "Invalid decimal precision {} or scale {}: {}",
                            precision, scale, e
                        ),
                    })?;
                Ok(Box::new(builder))
            }
            arrow_schema::DataType::Date32 => Ok(Box::new(Date32Builder::new())),
            arrow_schema::DataType::Time32(unit) => match unit {
                arrow_schema::TimeUnit::Second => Ok(Box::new(Time32SecondBuilder::new())),
                arrow_schema::TimeUnit::Millisecond => {
                    Ok(Box::new(Time32MillisecondBuilder::new()))
                }
                _ => Err(Error::IllegalArgument {
                    message: format!(
                        "Time32 only supports Second and Millisecond units, got: {:?}",
                        unit
                    ),
                }),
            },
            arrow_schema::DataType::Time64(unit) => match unit {
                arrow_schema::TimeUnit::Microsecond => {
                    Ok(Box::new(Time64MicrosecondBuilder::new()))
                }
                arrow_schema::TimeUnit::Nanosecond => Ok(Box::new(Time64NanosecondBuilder::new())),
                _ => Err(Error::IllegalArgument {
                    message: format!(
                        "Time64 only supports Microsecond and Nanosecond units, got: {:?}",
                        unit
                    ),
                }),
            },
            arrow_schema::DataType::Timestamp(arrow_schema::TimeUnit::Second, _) => {
                Ok(Box::new(TimestampSecondBuilder::new()))
            }
            arrow_schema::DataType::Timestamp(arrow_schema::TimeUnit::Millisecond, _) => {
                Ok(Box::new(TimestampMillisecondBuilder::new()))
            }
            arrow_schema::DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, _) => {
                Ok(Box::new(TimestampMicrosecondBuilder::new()))
            }
            arrow_schema::DataType::Timestamp(arrow_schema::TimeUnit::Nanosecond, _) => {
                Ok(Box::new(TimestampNanosecondBuilder::new()))
            }
            dt => Err(Error::IllegalArgument {
                message: format!("Unsupported data type: {dt:?}"),
            }),
        }
    }
}

impl ArrowRecordBatchInnerBuilder for RowAppendRecordBatchBuilder {
    fn build_arrow_record_batch(&mut self) -> Result<Arc<RecordBatch>> {
        let arrays: Result<Vec<ArrayRef>> = self
            .arrow_column_builders
            .iter_mut()
            .enumerate()
            .map(|(idx, b)| {
                let array = b.finish();
                let expected_type = self.table_schema.field(idx).data_type();

                // Validate array type matches schema
                if array.data_type() != expected_type {
                    return Err(Error::IllegalArgument {
                        message: format!(
                            "Builder type mismatch at column {}: expected {:?}, got {:?}",
                            idx,
                            expected_type,
                            array.data_type()
                        ),
                    });
                }

                Ok(array)
            })
            .collect();

        Ok(Arc::new(RecordBatch::try_new(
            self.table_schema.clone(),
            arrays?,
        )?))
    }

    fn append(&mut self, row: &GenericRow) -> Result<bool> {
        for (idx, value) in row.values.iter().enumerate() {
            let field_type = self.table_schema.field(idx).data_type();
            let builder = self.arrow_column_builders.get_mut(idx).unwrap();
            value.append_to(builder.as_mut(), field_type)?;
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
        row_type: &RowType,
        to_append_record_batch: bool,
        arrow_compression_info: ArrowCompressionInfo,
    ) -> Result<Self> {
        let arrow_batch_builder: Box<dyn ArrowRecordBatchInnerBuilder> = {
            if to_append_record_batch {
                Box::new(PrebuiltRecordBatchBuilder::default())
            } else {
                Box::new(RowAppendRecordBatchBuilder::new(row_type)?)
            }
        };
        Ok(MemoryLogRecordsArrowBuilder {
            base_log_offset: BUILDER_DEFAULT_OFFSET,
            schema_id,
            magic: CURRENT_LOG_MAGIC_VALUE,
            writer_id: NO_WRITER_ID,
            batch_sequence: NO_BATCH_SEQUENCE,
            is_closed: false,
            arrow_record_batch_builder: arrow_batch_builder,
            arrow_compression_info,
        })
    }

    pub fn append(&mut self, record: &WriteRecord) -> Result<bool> {
        match &record.record() {
            Record::Log(log_write_record) => match log_write_record {
                LogWriteRecord::Generic(row) => Ok(self.arrow_record_batch_builder.append(row)?),
                LogWriteRecord::RecordBatch(record_batch) => Ok(self
                    .arrow_record_batch_builder
                    .append_batch(record_batch.clone())?),
            },
            Record::Kv(_) => Err(Error::UnsupportedOperation {
                message: "Only LogRecord is supported to append".to_string(),
            }),
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

    pub fn build(&mut self) -> Result<Vec<u8>> {
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
        cursor.write_all(real_arrow_batch_bytes)?;

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

/// In-memory log record source.
/// Used for local tablet server fetches (existing path).
struct MemorySource {
    data: Bytes,
}

impl MemorySource {
    fn new(data: Vec<u8>) -> Self {
        Self {
            data: Bytes::from(data),
        }
    }

    fn read_batch_header(&self, pos: usize) -> Result<(i64, usize)> {
        if pos + LOG_OVERHEAD > self.data.len() {
            return Err(Error::UnexpectedError {
                message: format!(
                    "Position {} + LOG_OVERHEAD {} exceeds data size {}",
                    pos,
                    LOG_OVERHEAD,
                    self.data.len()
                ),
                source: None,
            });
        }

        let base_offset = LittleEndian::read_i64(&self.data[pos + BASE_OFFSET_OFFSET..]);
        let batch_size_bytes = LittleEndian::read_i32(&self.data[pos + LENGTH_OFFSET..]);

        // Validate batch size to prevent integer overflow and corruption
        let batch_size = validate_batch_size(batch_size_bytes)?;

        Ok((base_offset, batch_size))
    }

    fn read_batch_data(&self, pos: usize, size: usize) -> Result<Bytes> {
        if pos + size > self.data.len() {
            return Err(Error::UnexpectedError {
                message: format!(
                    "Read beyond data size: {} + {} > {}",
                    pos,
                    size,
                    self.data.len()
                ),
                source: None,
            });
        }
        // Zero-copy slice (Bytes is Arc-based)
        Ok(self.data.slice(pos..pos + size))
    }

    fn total_size(&self) -> usize {
        self.data.len()
    }
}

/// RAII guard that deletes a file when dropped.
/// Used to ensure file deletion happens AFTER the file handle is closed.
struct FileCleanupGuard {
    file_path: PathBuf,
}

impl Drop for FileCleanupGuard {
    fn drop(&mut self) {
        // File handle is already closed (this guard drops after the file field)
        if let Err(e) = std::fs::remove_file(&self.file_path) {
            log::warn!(
                "Failed to delete remote log file {}: {}",
                self.file_path.display(),
                e
            );
        } else {
            log::debug!("Deleted remote log file: {}", self.file_path.display());
        }
    }
}

/// File-backed log record source.
/// Used for remote log segments downloaded to local disk.
/// Streams data on-demand instead of loading entire file into memory.
///
/// Uses Mutex<File> with seek + read_exact for cross-platform compatibility.
/// Access pattern is sequential iteration (single consumer), so mutex overhead is negligible.
struct FileSource {
    file: Mutex<File>,
    file_size: usize,
    base_offset: usize,
    _cleanup: Option<FileCleanupGuard>, // Drops AFTER file (field order matters!)
}

impl FileSource {
    /// Create a new FileSource without automatic cleanup.
    pub fn new(file: File, base_offset: usize) -> Result<Self> {
        let file_size = file.metadata()?.len() as usize;

        // Validate base_offset to prevent underflow in total_size()
        if base_offset > file_size {
            return Err(Error::UnexpectedError {
                message: format!(
                    "base_offset ({}) exceeds file_size ({})",
                    base_offset, file_size
                ),
                source: None,
            });
        }

        Ok(Self {
            file: Mutex::new(file),
            file_size,
            base_offset,
            _cleanup: None,
        })
    }

    /// Create a new FileSource that will delete the file when dropped.
    /// This is used for remote log files that need cleanup.
    fn new_with_cleanup(file: File, base_offset: usize, file_path: PathBuf) -> Result<Self> {
        let file_size = file.metadata()?.len() as usize;

        // Validate base_offset to prevent underflow in total_size()
        if base_offset > file_size {
            return Err(Error::UnexpectedError {
                message: format!(
                    "base_offset ({}) exceeds file_size ({})",
                    base_offset, file_size
                ),
                source: None,
            });
        }

        Ok(Self {
            file: Mutex::new(file),
            file_size,
            base_offset,
            _cleanup: Some(FileCleanupGuard { file_path }),
        })
    }

    /// Read data at a specific position using seek + read_exact.
    /// This is cross-platform and adequate for sequential access patterns.
    fn read_at(&self, pos: u64, buf: &mut [u8]) -> Result<()> {
        let mut file = self.file.lock();
        file.seek(SeekFrom::Start(pos))?;
        file.read_exact(buf)?;
        Ok(())
    }

    fn read_batch_header(&self, pos: usize) -> Result<(i64, usize)> {
        let actual_pos = self.base_offset + pos;
        if actual_pos + LOG_OVERHEAD > self.file_size {
            return Err(Error::UnexpectedError {
                message: format!(
                    "Position {} exceeds file size {}",
                    actual_pos, self.file_size
                ),
                source: None,
            });
        }

        // Read only the header to extract base_offset and batch_size
        let mut header_buf = vec![0u8; LOG_OVERHEAD];
        self.read_at(actual_pos as u64, &mut header_buf)?;

        let base_offset = LittleEndian::read_i64(&header_buf[BASE_OFFSET_OFFSET..]);
        let batch_size_bytes = LittleEndian::read_i32(&header_buf[LENGTH_OFFSET..]);

        // Validate batch size to prevent integer overflow and corruption
        let batch_size = validate_batch_size(batch_size_bytes)?;

        Ok((base_offset, batch_size))
    }

    fn read_batch_data(&self, pos: usize, size: usize) -> Result<Bytes> {
        let actual_pos = self.base_offset + pos;
        if actual_pos + size > self.file_size {
            return Err(Error::UnexpectedError {
                message: format!(
                    "Read beyond file size: {} + {} > {}",
                    actual_pos, size, self.file_size
                ),
                source: None,
            });
        }

        // Read the full batch data
        let mut batch_buf = vec![0u8; size];
        self.read_at(actual_pos as u64, &mut batch_buf)?;

        Ok(Bytes::from(batch_buf))
    }

    fn total_size(&self) -> usize {
        self.file_size - self.base_offset
    }
}

/// Enum for different log record sources.
pub enum LogRecordsSource {
    Memory(MemorySource),
    File(FileSource),
}

impl LogRecordsSource {
    fn read_batch_header(&self, pos: usize) -> Result<(i64, usize)> {
        match self {
            Self::Memory(s) => s.read_batch_header(pos),
            Self::File(s) => s.read_batch_header(pos),
        }
    }

    fn read_batch_data(&self, pos: usize, size: usize) -> Result<Bytes> {
        match self {
            Self::Memory(s) => s.read_batch_data(pos, size),
            Self::File(s) => s.read_batch_data(pos, size),
        }
    }

    fn total_size(&self) -> usize {
        match self {
            Self::Memory(s) => s.total_size(),
            Self::File(s) => s.total_size(),
        }
    }
}

pub struct LogRecordsBatches {
    source: LogRecordsSource,
    current_pos: usize,
    remaining_bytes: usize,
}

impl LogRecordsBatches {
    /// Create from in-memory Vec (existing path - backward compatible).
    pub fn new(data: Vec<u8>) -> Self {
        let remaining_bytes = data.len();
        Self {
            source: LogRecordsSource::Memory(MemorySource::new(data)),
            current_pos: 0,
            remaining_bytes,
        }
    }

    /// Create from file.
    /// Enables streaming without loading entire file into memory.
    pub fn from_file(file: File, base_offset: usize) -> Result<Self> {
        let source = FileSource::new(file, base_offset)?;
        let remaining_bytes = source.total_size();
        Ok(Self {
            source: LogRecordsSource::File(source),
            current_pos: 0,
            remaining_bytes,
        })
    }

    /// Create from file with automatic cleanup.
    /// The file will be deleted when the LogRecordsBatches is dropped.
    /// This ensures file is closed before deletion.
    pub fn from_file_with_cleanup(
        file: File,
        base_offset: usize,
        file_path: PathBuf,
    ) -> Result<Self> {
        let source = FileSource::new_with_cleanup(file, base_offset, file_path)?;
        let remaining_bytes = source.total_size();
        Ok(Self {
            source: LogRecordsSource::File(source),
            current_pos: 0,
            remaining_bytes,
        })
    }

    /// Create from any source (for testing).
    pub fn from_source(source: LogRecordsSource) -> Self {
        let remaining_bytes = source.total_size();
        Self {
            source,
            current_pos: 0,
            remaining_bytes,
        }
    }

    /// Try to get the size of the next batch.
    fn next_batch_size(&self) -> Result<Option<usize>> {
        if self.remaining_bytes < LOG_OVERHEAD {
            return Ok(None);
        }

        // Read only header to get size
        match self.source.read_batch_header(self.current_pos) {
            Ok((_base_offset, batch_size)) => {
                if batch_size > self.remaining_bytes {
                    Ok(None)
                } else {
                    Ok(Some(batch_size))
                }
            }
            Err(e) => Err(e),
        }
    }
}

impl Iterator for LogRecordsBatches {
    type Item = Result<LogRecordBatch>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.next_batch_size() {
            Ok(Some(batch_size)) => {
                // Read full batch data on-demand
                match self.source.read_batch_data(self.current_pos, batch_size) {
                    Ok(data) => {
                        let record_batch = LogRecordBatch::new(data);
                        self.current_pos += batch_size;
                        self.remaining_bytes -= batch_size;
                        Some(Ok(record_batch))
                    }
                    Err(e) => Some(Err(e)),
                }
            }
            Ok(None) => None,
            Err(e) => Some(Err(e)),
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

        let data = self
            .data
            .get(RECORDS_OFFSET..)
            .ok_or_else(|| Error::UnexpectedError {
                message: format!(
                    "Corrupt log record batch: data length {} is less than RECORDS_OFFSET {}",
                    self.data.len(),
                    RECORDS_OFFSET
                ),
                source: None,
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

pub fn to_arrow_schema(fluss_schema: &RowType) -> Result<SchemaRef> {
    let fields: Result<Vec<Field>> = fluss_schema
        .fields()
        .iter()
        .map(|f| {
            Ok(Field::new(
                f.name(),
                to_arrow_type(f.data_type())?,
                f.data_type().is_nullable(),
            ))
        })
        .collect();

    Ok(SchemaRef::new(arrow_schema::Schema::new(fields?)))
}

pub fn to_arrow_type(fluss_type: &DataType) -> Result<ArrowDataType> {
    Ok(match fluss_type {
        DataType::Boolean(_) => ArrowDataType::Boolean,
        DataType::TinyInt(_) => ArrowDataType::Int8,
        DataType::SmallInt(_) => ArrowDataType::Int16,
        DataType::BigInt(_) => ArrowDataType::Int64,
        DataType::Int(_) => ArrowDataType::Int32,
        DataType::Float(_) => ArrowDataType::Float32,
        DataType::Double(_) => ArrowDataType::Float64,
        DataType::Char(_) => ArrowDataType::Utf8,
        DataType::String(_) => ArrowDataType::Utf8,
        DataType::Decimal(decimal_type) => {
            let precision =
                decimal_type
                    .precision()
                    .try_into()
                    .map_err(|_| Error::IllegalArgument {
                        message: format!(
                            "Decimal precision {} exceeds Arrow's maximum (u8::MAX)",
                            decimal_type.precision()
                        ),
                    })?;
            let scale = decimal_type
                .scale()
                .try_into()
                .map_err(|_| Error::IllegalArgument {
                    message: format!(
                        "Decimal scale {} exceeds Arrow's maximum (i8::MAX)",
                        decimal_type.scale()
                    ),
                })?;
            ArrowDataType::Decimal128(precision, scale)
        }
        DataType::Date(_) => ArrowDataType::Date32,
        DataType::Time(time_type) => match time_type.precision() {
            0 => ArrowDataType::Time32(arrow_schema::TimeUnit::Second),
            1..=3 => ArrowDataType::Time32(arrow_schema::TimeUnit::Millisecond),
            4..=6 => ArrowDataType::Time64(arrow_schema::TimeUnit::Microsecond),
            7..=9 => ArrowDataType::Time64(arrow_schema::TimeUnit::Nanosecond),
            invalid => {
                return Err(Error::IllegalArgument {
                    message: format!("Invalid precision {} for TimeType (must be 0-9)", invalid),
                });
            }
        },
        DataType::Timestamp(timestamp_type) => match timestamp_type.precision() {
            0 => ArrowDataType::Timestamp(arrow_schema::TimeUnit::Second, None),
            1..=3 => ArrowDataType::Timestamp(arrow_schema::TimeUnit::Millisecond, None),
            4..=6 => ArrowDataType::Timestamp(arrow_schema::TimeUnit::Microsecond, None),
            7..=9 => ArrowDataType::Timestamp(arrow_schema::TimeUnit::Nanosecond, None),
            invalid => {
                return Err(Error::IllegalArgument {
                    message: format!(
                        "Invalid precision {} for TimestampType (must be 0-9)",
                        invalid
                    ),
                });
            }
        },
        DataType::TimestampLTz(timestamp_ltz_type) => match timestamp_ltz_type.precision() {
            0 => ArrowDataType::Timestamp(arrow_schema::TimeUnit::Second, None),
            1..=3 => ArrowDataType::Timestamp(arrow_schema::TimeUnit::Millisecond, None),
            4..=6 => ArrowDataType::Timestamp(arrow_schema::TimeUnit::Microsecond, None),
            7..=9 => ArrowDataType::Timestamp(arrow_schema::TimeUnit::Nanosecond, None),
            invalid => {
                return Err(Error::IllegalArgument {
                    message: format!(
                        "Invalid precision {} for TimestampLTzType (must be 0-9)",
                        invalid
                    ),
                });
            }
        },
        DataType::Bytes(_) => ArrowDataType::Binary,
        DataType::Binary(binary_type) => {
            let length = binary_type
                .length()
                .try_into()
                .map_err(|_| Error::IllegalArgument {
                    message: format!(
                        "Binary length {} exceeds Arrow's maximum (i32::MAX)",
                        binary_type.length()
                    ),
                })?;
            ArrowDataType::FixedSizeBinary(length)
        }
        DataType::Array(array_type) => ArrowDataType::List(
            Field::new_list_field(
                to_arrow_type(array_type.get_element_type())?,
                fluss_type.is_nullable(),
            )
            .into(),
        ),
        DataType::Map(map_type) => {
            let key_type = to_arrow_type(map_type.key_type())?;
            let value_type = to_arrow_type(map_type.value_type())?;
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
        DataType::Row(row_type) => {
            let fields: Result<Vec<Field>> = row_type
                .fields()
                .iter()
                .map(|f| {
                    Ok(Field::new(
                        f.name(),
                        to_arrow_type(f.data_type())?,
                        f.data_type().is_nullable(),
                    ))
                })
                .collect();
            ArrowDataType::Struct(arrow_schema::Fields::from(fields?))
        }
    })
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
                        IllegalArgument {
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
                return Err(IllegalArgument {
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
            |e| IllegalArgument {
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
            &HashMap::new(),
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
            &HashMap::new(),
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
    use crate::metadata::{DataField, DataTypes, RowType};

    #[test]
    fn test_to_array_type() {
        assert_eq!(
            to_arrow_type(&DataTypes::boolean()).unwrap(),
            ArrowDataType::Boolean
        );
        assert_eq!(
            to_arrow_type(&DataTypes::tinyint()).unwrap(),
            ArrowDataType::Int8
        );
        assert_eq!(
            to_arrow_type(&DataTypes::smallint()).unwrap(),
            ArrowDataType::Int16
        );
        assert_eq!(
            to_arrow_type(&DataTypes::bigint()).unwrap(),
            ArrowDataType::Int64
        );
        assert_eq!(
            to_arrow_type(&DataTypes::int()).unwrap(),
            ArrowDataType::Int32
        );
        assert_eq!(
            to_arrow_type(&DataTypes::float()).unwrap(),
            ArrowDataType::Float32
        );
        assert_eq!(
            to_arrow_type(&DataTypes::double()).unwrap(),
            ArrowDataType::Float64
        );
        assert_eq!(
            to_arrow_type(&DataTypes::char(16)).unwrap(),
            ArrowDataType::Utf8
        );
        assert_eq!(
            to_arrow_type(&DataTypes::string()).unwrap(),
            ArrowDataType::Utf8
        );
        assert_eq!(
            to_arrow_type(&DataTypes::decimal(10, 2)).unwrap(),
            ArrowDataType::Decimal128(10, 2)
        );
        assert_eq!(
            to_arrow_type(&DataTypes::date()).unwrap(),
            ArrowDataType::Date32
        );
        assert_eq!(
            to_arrow_type(&DataTypes::time()).unwrap(),
            ArrowDataType::Time32(arrow_schema::TimeUnit::Second)
        );
        assert_eq!(
            to_arrow_type(&DataTypes::time_with_precision(3)).unwrap(),
            ArrowDataType::Time32(arrow_schema::TimeUnit::Millisecond)
        );
        assert_eq!(
            to_arrow_type(&DataTypes::time_with_precision(6)).unwrap(),
            ArrowDataType::Time64(arrow_schema::TimeUnit::Microsecond)
        );
        assert_eq!(
            to_arrow_type(&DataTypes::time_with_precision(9)).unwrap(),
            ArrowDataType::Time64(arrow_schema::TimeUnit::Nanosecond)
        );
        assert_eq!(
            to_arrow_type(&DataTypes::timestamp_with_precision(0)).unwrap(),
            ArrowDataType::Timestamp(arrow_schema::TimeUnit::Second, None)
        );
        assert_eq!(
            to_arrow_type(&DataTypes::timestamp_with_precision(3)).unwrap(),
            ArrowDataType::Timestamp(arrow_schema::TimeUnit::Millisecond, None)
        );
        assert_eq!(
            to_arrow_type(&DataTypes::timestamp_with_precision(6)).unwrap(),
            ArrowDataType::Timestamp(arrow_schema::TimeUnit::Microsecond, None)
        );
        assert_eq!(
            to_arrow_type(&DataTypes::timestamp_with_precision(9)).unwrap(),
            ArrowDataType::Timestamp(arrow_schema::TimeUnit::Nanosecond, None)
        );
        assert_eq!(
            to_arrow_type(&DataTypes::timestamp_ltz_with_precision(0)).unwrap(),
            ArrowDataType::Timestamp(arrow_schema::TimeUnit::Second, None)
        );
        assert_eq!(
            to_arrow_type(&DataTypes::timestamp_ltz_with_precision(3)).unwrap(),
            ArrowDataType::Timestamp(arrow_schema::TimeUnit::Millisecond, None)
        );
        assert_eq!(
            to_arrow_type(&DataTypes::timestamp_ltz_with_precision(6)).unwrap(),
            ArrowDataType::Timestamp(arrow_schema::TimeUnit::Microsecond, None)
        );
        assert_eq!(
            to_arrow_type(&DataTypes::timestamp_ltz_with_precision(9)).unwrap(),
            ArrowDataType::Timestamp(arrow_schema::TimeUnit::Nanosecond, None)
        );
        assert_eq!(
            to_arrow_type(&DataTypes::bytes()).unwrap(),
            ArrowDataType::Binary
        );
        assert_eq!(
            to_arrow_type(&DataTypes::binary(16)).unwrap(),
            ArrowDataType::FixedSizeBinary(16)
        );

        assert_eq!(
            to_arrow_type(&DataTypes::array(DataTypes::int())).unwrap(),
            ArrowDataType::List(Field::new_list_field(ArrowDataType::Int32, true).into())
        );

        assert_eq!(
            to_arrow_type(&DataTypes::map(DataTypes::string(), DataTypes::int())).unwrap(),
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
            ]))
            .unwrap(),
            ArrowDataType::Struct(arrow_schema::Fields::from(vec![
                Field::new("f1", ArrowDataType::Int32, true),
                Field::new("f2", ArrowDataType::Utf8, true),
            ]))
        );
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
        let row_type = RowType::new(vec![
            DataField::new("id".to_string(), DataTypes::int(), None),
            DataField::new("name".to_string(), DataTypes::string(), None),
        ]);
        let schema = to_arrow_schema(&row_type).unwrap();
        let result = ReadContext::with_projection_pushdown(schema, vec![0, 2], false);

        assert!(matches!(result, Err(IllegalArgument { .. })));
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

    #[test]
    fn test_temporal_and_decimal_builder_validation() {
        use arrow::array::Array;

        // Test valid builder creation with precision=10, scale=2
        let mut builder =
            RowAppendRecordBatchBuilder::create_builder(&ArrowDataType::Decimal128(10, 2)).unwrap();
        let decimal_builder = builder
            .as_any_mut()
            .downcast_mut::<Decimal128Builder>()
            .expect("Expected Decimal128Builder");
        // Verify precision and scale
        let array = decimal_builder.finish();
        assert_eq!(array.data_type(), &ArrowDataType::Decimal128(10, 2));

        // Test error case: invalid precision/scale
        let result =
            RowAppendRecordBatchBuilder::create_builder(&ArrowDataType::Decimal128(100, 50));
        assert!(result.is_err());
    }

    #[test]
    fn test_decimal_rescaling_and_validation() -> Result<()> {
        use crate::row::{Datum, Decimal, GenericRow};
        use arrow::array::Decimal128Array;
        use bigdecimal::BigDecimal;
        use std::str::FromStr;

        // Test 1: Rescaling from scale 3 to scale 2
        let row_type = RowType::new(vec![DataField::new(
            "amount".to_string(),
            DataTypes::decimal(10, 2),
            None,
        )]);
        let mut builder = RowAppendRecordBatchBuilder::new(&row_type)?;
        let decimal = Decimal::from_big_decimal(BigDecimal::from_str("123.456").unwrap(), 10, 3)?;
        builder.append(&GenericRow {
            values: vec![Datum::Decimal(decimal)],
        })?;
        let batch = builder.build_arrow_record_batch()?;
        let array = batch
            .column(0)
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .unwrap();
        assert_eq!(array.value(0), 12346); // 123.456 rounded to 2 decimal places
        assert_eq!(array.scale(), 2);

        // Test 2: Precision overflow (should error)
        let row_type = RowType::new(vec![DataField::new(
            "amount".to_string(),
            DataTypes::decimal(5, 2),
            None,
        )]);
        let mut builder = RowAppendRecordBatchBuilder::new(&row_type)?;
        let decimal = Decimal::from_big_decimal(BigDecimal::from_str("123456.78").unwrap(), 10, 2)?;
        let result = builder.append(&GenericRow {
            values: vec![Datum::Decimal(decimal)],
        });
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("precision overflow")
        );

        Ok(())
    }

    // Tests for file-backed streaming

    #[test]
    fn test_file_source_streaming() -> Result<()> {
        use tempfile::NamedTempFile;

        // Test 1: Basic file reads work
        let test_data = vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
        let mut tmp_file = NamedTempFile::new()?;
        tmp_file.write_all(&test_data)?;
        tmp_file.flush()?;

        let file = File::open(tmp_file.path())?;
        let source = FileSource::new(file, 0)?;

        // Read full data
        let data = source.read_batch_data(0, 10)?;
        assert_eq!(data.to_vec(), test_data);

        // Read partial data
        let partial = source.read_batch_data(2, 5)?;
        assert_eq!(partial.to_vec(), vec![3, 4, 5, 6, 7]);

        // Test 2: base_offset works (critical for remote logs with pos_in_log_segment)
        let prefix = vec![0xFF; 100];
        let actual_data = vec![1, 2, 3, 4, 5];
        let mut tmp_file2 = NamedTempFile::new()?;
        tmp_file2.write_all(&prefix)?;
        tmp_file2.write_all(&actual_data)?;
        tmp_file2.flush()?;

        let file2 = File::open(tmp_file2.path())?;
        let source2 = FileSource::new(file2, 100)?; // Skip first 100 bytes

        assert_eq!(source2.total_size(), 5); // Only counts data after offset
        let data2 = source2.read_batch_data(0, 5)?;
        assert_eq!(data2.to_vec(), actual_data);

        Ok(())
    }

    #[test]
    fn test_all_types_end_to_end() -> Result<()> {
        use crate::row::{Date, Datum, Decimal, GenericRow, Time, TimestampLtz, TimestampNtz};
        use arrow::array::{
            Date32Array, Decimal128Array, Int32Array, Time32MillisecondArray,
            Time64NanosecondArray, TimestampMicrosecondArray, TimestampNanosecondArray,
        };
        use bigdecimal::BigDecimal;
        use std::str::FromStr;

        // Schema with int, decimal, date, time (ms + ns), timestamps (μs + ns)
        let row_type = RowType::new(vec![
            DataField::new("id".to_string(), DataTypes::int(), None),
            DataField::new("amount".to_string(), DataTypes::decimal(10, 2), None),
            DataField::new("date".to_string(), DataTypes::date(), None),
            DataField::new(
                "time_ms".to_string(),
                DataTypes::time_with_precision(3),
                None,
            ),
            DataField::new(
                "time_ns".to_string(),
                DataTypes::time_with_precision(9),
                None,
            ),
            DataField::new(
                "ts_us".to_string(),
                DataTypes::timestamp_with_precision(6),
                None,
            ),
            DataField::new(
                "ts_ltz_ns".to_string(),
                DataTypes::timestamp_ltz_with_precision(9),
                None,
            ),
        ]);

        let mut builder = RowAppendRecordBatchBuilder::new(&row_type)?;

        // Append rows with various data types
        builder.append(&GenericRow {
            values: vec![
                Datum::Int32(1),
                Datum::Decimal(Decimal::from_big_decimal(
                    BigDecimal::from_str("123.456").unwrap(),
                    10,
                    3,
                )?),
                // 18000 days since epoch = 2019-04-14
                Datum::Date(Date::new(18000)),
                // 43200000 ms = 12:00:00.000 (noon)
                Datum::Time(Time::new(43200000)),
                // 12345 ms = 00:00:12.345
                Datum::Time(Time::new(12345)),
                // 1609459200000 ms = 2021-01-01 00:00:00 UTC, with 123456 additional nanoseconds
                Datum::TimestampNtz(TimestampNtz::from_millis_nanos(1609459200000, 123456)?),
                // 1609459200000 ms = 2021-01-01 00:00:00 UTC, with 987654 additional nanoseconds
                Datum::TimestampLtz(TimestampLtz::from_millis_nanos(1609459200000, 987654)?),
            ],
        })?;

        let batch = builder.build_arrow_record_batch()?;

        // Verify all conversions
        assert_eq!(
            batch
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .value(0),
            1
        );

        let dec = batch
            .column(1)
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .unwrap();
        assert_eq!(dec.value(0), 12346); // 123.456 rounded to 2 decimal places

        assert_eq!(
            batch
                .column(2)
                .as_any()
                .downcast_ref::<Date32Array>()
                .unwrap()
                .value(0),
            18000
        );

        assert_eq!(
            batch
                .column(3)
                .as_any()
                .downcast_ref::<Time32MillisecondArray>()
                .unwrap()
                .value(0),
            43200000
        );

        assert_eq!(
            batch
                .column(4)
                .as_any()
                .downcast_ref::<Time64NanosecondArray>()
                .unwrap()
                .value(0),
            12345000000
        );

        // Timestamp with sub-millisecond nanos preserved
        assert_eq!(
            batch
                .column(5)
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .unwrap()
                .value(0),
            1609459200000123
        );

        assert_eq!(
            batch
                .column(6)
                .as_any()
                .downcast_ref::<TimestampNanosecondArray>()
                .unwrap()
                .value(0),
            1609459200000987654
        );

        Ok(())
    }

    #[test]
    fn test_log_records_batches_from_file() -> Result<()> {
        use crate::client::WriteRecord;
        use crate::compression::{
            ArrowCompressionInfo, ArrowCompressionType, DEFAULT_NON_ZSTD_COMPRESSION_LEVEL,
        };
        use crate::metadata::TablePath;
        use crate::row::GenericRow;
        use tempfile::NamedTempFile;

        // Integration test: Real log record batch streamed from file
        let row_type = RowType::new(vec![
            DataField::new("id".to_string(), DataTypes::int(), None),
            DataField::new("name".to_string(), DataTypes::string(), None),
        ]);
        let table_path = Arc::new(TablePath::new("db".to_string(), "tbl".to_string()));

        let mut builder = MemoryLogRecordsArrowBuilder::new(
            1,
            &row_type,
            false,
            ArrowCompressionInfo {
                compression_type: ArrowCompressionType::None,
                compression_level: DEFAULT_NON_ZSTD_COMPRESSION_LEVEL,
            },
        );

        let mut row = GenericRow::new();
        row.set_field(0, 1_i32);
        row.set_field(1, "alice");
        let record = WriteRecord::for_append(table_path.clone(), 1, row);
        builder.append(&record)?;

        let mut row2 = GenericRow::new();
        row2.set_field(0, 2_i32);
        row2.set_field(1, "bob");
        let record2 = WriteRecord::for_append(table_path, 2, row2);
        builder.append(&record2)?;

        let data = builder.build()?;

        // Write to file
        let mut tmp_file = NamedTempFile::new()?;
        tmp_file.write_all(&data)?;
        tmp_file.flush()?;

        // Create file-backed LogRecordsBatches (should stream, not load all into memory)
        let file = File::open(tmp_file.path())?;
        let mut batches = LogRecordsBatches::from_file(file, 0)?;

        // Iterate through batches (should work just like in-memory)
        let batch = batches.next().expect("Should have at least one batch")?;
        assert!(batch.size_in_bytes() > 0);
        assert_eq!(batch.record_count(), 2);

        Ok(())
    }
}
