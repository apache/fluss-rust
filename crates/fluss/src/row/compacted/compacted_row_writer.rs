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

use crate::error::{Error, Result};
use crate::row::binary::BinaryWriter;
use crate::row::compacted::compacted_row::calculate_bit_set_width_in_bytes;
use crate::util::varint::{write_unsigned_varint_to_slice, write_unsigned_varint_u64_to_slice};
use bytes::{Bytes, BytesMut};
use std::cmp;

/// Computes the precision of a decimal's unscaled value, matching Java's BigDecimal.precision().
/// This counts significant digits by stripping trailing base-10 zeros first.
fn java_decimal_precision(unscaled: &bigdecimal::num_bigint::BigInt) -> usize {
    use bigdecimal::num_traits::Zero;

    if unscaled.is_zero() {
        return 1;
    }

    // For bounded precision (≤ 38 digits), string conversion is cheap and simple.
    let s = unscaled.magnitude().to_str_radix(10);
    let trimmed = s.trim_end_matches('0');

    trimmed.len()
}

// Writer for CompactedRow
// Reference implementation:
// https://github.com/apache/fluss/blob/d4a72fad240d4b81563aaf83fa3b09b5058674ed/fluss-common/src/main/java/org/apache/fluss/row/compacted/CompactedRowWriter.java#L71
#[allow(dead_code)]
pub struct CompactedRowWriter {
    header_size_in_bytes: usize,
    position: usize,
    buffer: BytesMut,
}

#[allow(dead_code)]
impl CompactedRowWriter {
    pub const MAX_INT_SIZE: usize = 5;
    pub const MAX_LONG_SIZE: usize = 10;

    pub fn new(field_count: usize) -> Self {
        let header_size = calculate_bit_set_width_in_bytes(field_count);
        let cap = cmp::max(64, header_size);

        let mut buffer = BytesMut::with_capacity(cap);
        buffer.resize(cap, 0);

        Self {
            header_size_in_bytes: header_size,
            position: header_size,
            buffer,
        }
    }

    pub fn position(&self) -> usize {
        self.position
    }

    pub fn buffer(&self) -> &[u8] {
        &self.buffer[..self.position]
    }

    pub fn to_bytes(&self) -> Bytes {
        Bytes::copy_from_slice(&self.buffer[..self.position])
    }

    fn ensure_capacity(&mut self, need_len: usize) {
        if (self.buffer.len() - self.position) < need_len {
            let new_len = cmp::max(self.buffer.len() * 2, self.buffer.len() + need_len);
            self.buffer.resize(new_len, 0);
        }
    }

    fn write_raw(&mut self, src: &[u8]) -> Result<()> {
        let end = self.position + src.len();
        self.ensure_capacity(src.len());
        self.buffer[self.position..end].copy_from_slice(src);
        self.position = end;
        Ok(())
    }
}

impl BinaryWriter for CompactedRowWriter {
    fn reset(&mut self) {
        self.position = self.header_size_in_bytes;
        self.buffer[..self.header_size_in_bytes].fill(0);
    }

    fn set_null_at(&mut self, pos: usize) {
        let byte_index = pos >> 3;
        let bit = pos & 7;
        debug_assert!(byte_index < self.header_size_in_bytes);
        self.buffer[byte_index] |= 1u8 << bit;
    }

    fn write_boolean(&mut self, value: bool) -> Result<()> {
        let b = if value { 1u8 } else { 0u8 };
        self.write_raw(&[b])
    }

    fn write_byte(&mut self, value: u8) -> Result<()> {
        self.write_raw(&[value])
    }

    fn write_bytes(&mut self, value: &[u8]) -> Result<()> {
        let len_i32 = i32::try_from(value.len()).map_err(|_| Error::IllegalArgument {
            message: format!(
                "Byte slice too large to encode length as i32: {} bytes exceeds i32::MAX",
                value.len()
            ),
        })?;
        self.write_int(len_i32)?;
        self.write_raw(value)
    }

    fn write_char(&mut self, value: &str, _length: usize) -> Result<()> {
        // TODO: currently, we encoding CHAR(length) as the same with STRING, the length info can be
        //  omitted and the bytes length should be enforced in the future.
        self.write_string(value)
    }

    fn write_string(&mut self, value: &str) -> Result<()> {
        self.write_bytes(value.as_ref())
    }

    fn write_short(&mut self, value: i16) -> Result<()> {
        // Use native endianness to match Java's UnsafeUtils.putShort behavior
        // Java uses sun.misc.Unsafe which writes in native byte order (typically LE on x86/ARM)
        self.write_raw(&value.to_ne_bytes())
    }

    fn write_int(&mut self, value: i32) -> Result<()> {
        self.ensure_capacity(Self::MAX_INT_SIZE);
        let bytes_written =
            write_unsigned_varint_to_slice(value as u32, &mut self.buffer[self.position..]);
        self.position += bytes_written;
        Ok(())
    }

    fn write_long(&mut self, value: i64) -> Result<()> {
        self.ensure_capacity(Self::MAX_LONG_SIZE);
        let bytes_written =
            write_unsigned_varint_u64_to_slice(value as u64, &mut self.buffer[self.position..]);
        self.position += bytes_written;
        Ok(())
    }

    fn write_float(&mut self, value: f32) -> Result<()> {
        // Use native endianness to match Java's UnsafeUtils.putFloat behavior
        self.write_raw(&value.to_ne_bytes())
    }

    fn write_double(&mut self, value: f64) -> Result<()> {
        // Use native endianness to match Java's UnsafeUtils.putDouble behavior
        self.write_raw(&value.to_ne_bytes())
    }

    fn write_binary(&mut self, bytes: &[u8], length: usize) -> Result<()> {
        // TODO: currently, we encoding BINARY(length) as the same with BYTES, the length info can
        //  be omitted and the bytes length should be enforced in the future.
        self.write_bytes(&bytes[..length.min(bytes.len())])
    }

    fn complete(&mut self) {
        // do nothing
    }

    fn write_decimal(
        &mut self,
        value: &bigdecimal::BigDecimal,
        precision: u32,
        scale: u32,
    ) -> Result<()> {
        const MAX_COMPACT_PRECISION: u32 = 18;
        use bigdecimal::rounding::RoundingMode;

        // Step 1 (Java: Decimal.fromBigDecimal): rescale + validate precision
        // bd = bd.setScale(scale, RoundingMode.HALF_UP);
        let scaled = value.with_scale_round(scale as i64, RoundingMode::HalfUp);

        // In bigdecimal, after scaling, the "unscaled value" is exactly the bigint mantissa.
        // That corresponds to Java: scaled.movePointRight(scale).toBigIntegerExact().
        let (unscaled, exp) = scaled.as_bigint_and_exponent();

        // Sanity check
        debug_assert_eq!(
            exp, scale as i64,
            "Scaled decimal exponent ({}) != expected scale ({})",
            exp, scale
        );

        // Java validates: if (bd.precision() > precision) return null;
        // Java BigDecimal.precision() == number of base-10 digits in the unscaled value (sign ignored),
        // with 0 having precision 1, and trailing zeros stripped.
        let prec = java_decimal_precision(&unscaled);
        if prec > precision as usize {
            return Err(Error::IllegalArgument {
                message: format!(
                    "Decimal precision overflow after rescale: scaled={} has {} digits but precision is {} (orig={})",
                    scaled, prec, precision, value
                ),
            });
        }

        // Step 2 (Java: CompactedRowWriter.writeDecimal): serialize precomputed unscaled representation
        if precision <= MAX_COMPACT_PRECISION {
            let unscaled_i64 = i64::try_from(&unscaled).map_err(|_| Error::IllegalArgument {
                message: format!(
                    "Decimal mantissa exceeds i64 range for compact precision {}: unscaled={} (scaled={}, orig={})",
                    precision, unscaled, scaled, value
                ),
            })?;
            self.write_long(unscaled_i64)
        } else {
            // Java: BigInteger.toByteArray() (two's complement big-endian, minimal length)
            let bytes = unscaled.to_signed_bytes_be();
            self.write_bytes(&bytes)
        }
    }

    fn write_time(&mut self, value: i32, _precision: u32) -> Result<()> {
        // TIME is always encoded as i32 (milliseconds since midnight) regardless of precision
        self.write_int(value)
    }

    fn write_timestamp_ntz(
        &mut self,
        value: &crate::row::datum::Timestamp,
        precision: u32,
    ) -> Result<()> {
        if crate::row::datum::Timestamp::is_compact(precision) {
            // Compact: write only milliseconds
            self.write_long(value.get_millisecond())?;
        } else {
            // Non-compact: write milliseconds + nanoOfMillisecond
            self.write_long(value.get_millisecond())?;
            self.write_int(value.get_nano_of_millisecond())?;
        }
        Ok(())
    }

    fn write_timestamp_ltz(
        &mut self,
        value: &crate::row::datum::TimestampLtz,
        precision: u32,
    ) -> Result<()> {
        if crate::row::datum::TimestampLtz::is_compact(precision) {
            // Compact: write only epoch milliseconds
            self.write_long(value.get_epoch_millisecond())?;
        } else {
            // Non-compact: write epoch milliseconds + nanoOfMillisecond
            self.write_long(value.get_epoch_millisecond())?;
            self.write_int(value.get_nano_of_millisecond())?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_java_decimal_precision() {
        // Tests java_decimal_precision strips trailing zeros matching Java's BigDecimal.precision()
        use bigdecimal::num_bigint::BigInt;

        let cases = vec![
            (0, 1),
            (10, 1),
            (100, 1),
            (123, 3),
            (12300, 3),
            (1_000_000_000, 1),
            (999_999_999_999_999_999i64, 18),
        ];

        for (value, expected) in cases {
            assert_eq!(java_decimal_precision(&BigInt::from(value)), expected);
        }

        // Large numbers beyond i64
        assert_eq!(java_decimal_precision(&BigInt::from(10_i128.pow(20))), 1);
        assert_eq!(
            java_decimal_precision(&BigInt::from(123_i128 * 10_i128.pow(15))),
            3
        );
    }

    #[test]
    fn test_write_decimal() {
        // Comprehensive test covering: compact/non-compact, rounding, trailing zeros, precision validation
        use bigdecimal::{BigDecimal, num_bigint::BigInt};

        // Rounding: HALF_UP (rounds half away from zero)
        let cases = vec![
            (12345, 3, 2, 10, 1235), // 12.345 → 12.35
            (-5, 1, 0, 10, -1),      // -0.5 → -1
        ];
        for (mantissa, input_scale, target_scale, precision, expected) in cases {
            let mut w = CompactedRowWriter::new(1);
            w.write_decimal(
                &BigDecimal::new(BigInt::from(mantissa), input_scale),
                precision,
                target_scale,
            )
            .unwrap();
            let (val, _) = crate::util::varint::read_unsigned_varint_u64_at(
                w.buffer(),
                w.header_size_in_bytes,
                CompactedRowWriter::MAX_LONG_SIZE,
            )
            .unwrap();
            assert_eq!(val as i64, expected);
        }

        // Trailing zeros: value=1, scale=10 → unscaled=10^10 but precision=1 (strips zeros)
        let mut w = CompactedRowWriter::new(1);
        assert!(
            w.write_decimal(&BigDecimal::new(BigInt::from(1), 0), 1, 10)
                .is_ok()
        );

        // Non-compact (precision > 18): minimal byte encoding
        let mut w = CompactedRowWriter::new(1);
        w.write_decimal(&BigDecimal::new(BigInt::from(12345), 0), 28, 0)
            .unwrap();
        // Verify something was written (at least length varint + some bytes)
        assert!(w.position() > w.header_size_in_bytes);

        // Precision overflow
        let mut w = CompactedRowWriter::new(1);
        let result = w.write_decimal(&BigDecimal::new(BigInt::from(1234), 0), 3, 2);
        assert!(
            result.is_err()
                && result
                    .unwrap_err()
                    .to_string()
                    .contains("precision overflow")
        );
    }
}
