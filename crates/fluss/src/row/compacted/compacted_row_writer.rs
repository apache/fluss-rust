use std::cmp;

use bytes::{Bytes, BytesMut};

pub struct CompactedRowWriter {
    header_size_in_bytes: usize,
    position: usize,
    buffer: BytesMut,
}

impl CompactedRowWriter {
    pub const MAX_INT_SIZE: usize = 5;
    pub const MAX_LONG_SIZE: usize = 10;

    pub fn new(field_count: usize) -> Self {
        // bitset width in bytes, it should be in CompactedRow
        let header_size = (field_count + 7) / 8;
        let cap = cmp::max(64, header_size);

        let mut buffer = BytesMut::with_capacity(cap);
        buffer.resize(cap, 0);

        Self {
            header_size_in_bytes: header_size,
            position: header_size,
            buffer,
        }
    }

    pub fn reset(&mut self) {
        self.position = self.header_size_in_bytes;
        self.buffer[..self.header_size_in_bytes].fill(0);
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

    fn ensure_len(&mut self, need_len: usize) {
        if self.buffer.len() < need_len {
            self.buffer.resize(need_len, 0);
        }
    }

    fn write_raw(&mut self, src: &[u8]) {
        let end = self.position + src.len();
        self.ensure_len(end);
        self.buffer[self.position..end].copy_from_slice(src);
        self.position = end;
    }

    pub fn set_null_at(&mut self, pos: usize) {
        let byte_index = pos >> 3;
        let bit = pos & 7;
        debug_assert!(byte_index < self.header_size_in_bytes);
        self.buffer[byte_index] |= 1u8 << bit;
    }

    pub fn write_boolean(&mut self, value: bool) {
        let b = if value { 1u8 } else { 0u8 };
        self.write_raw(&[b]);
    }

    pub fn write_byte(&mut self, value: i8) {
        self.write_raw(&[value as u8]);
    }

    pub fn write_binary(&mut self, bytes: &[u8], length: usize) {
        self.write_bytes(&bytes[..length.min(bytes.len())]);
    }

    pub fn write_bytes(&mut self, value: &[u8]) {
        self.write_int(value.len() as i32);
        self.write_raw(value);
    }

    pub fn write_char(&mut self, value: &str, length: usize) {
        self.write_string(value);
    }

    pub fn write_string(&mut self, value: &str) {
        self.write_bytes(value.as_ref());
    }

    pub fn write_short(&mut self, value: i16) {
        self.write_raw(&value.to_be_bytes());
    }

    pub fn write_int(&mut self, value: i32) {
        self.ensure_len(self.position + Self::MAX_INT_SIZE);
        let mut v = value as u32;
        while (v & !0x7F) != 0 {
            self.buffer[self.position] = ((v as u8) & 0x7F) | 0x80;
            self.position += 1;
            v >>= 7;
        }
        self.buffer[self.position] = v as u8;
        self.position += 1;
    }
    pub fn write_long(&mut self, value: i64) {
        self.ensure_len(self.position + Self::MAX_LONG_SIZE);
        let mut v = value as u64;
        while (v & !0x7F) != 0 {
            self.buffer[self.position] = ((v as u8) & 0x7F) | 0x80;
            self.position += 1;
            v >>= 7;
        }
        self.buffer[self.position] = v as u8;
        self.position += 1;
    }

    pub fn write_float(&mut self, value: f32) {
        self.write_raw(&value.to_be_bytes());
    }

    pub fn write_double(&mut self, value: f64) {
        self.write_raw(&value.to_be_bytes());
    }
}
