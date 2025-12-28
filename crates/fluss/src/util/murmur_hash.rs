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

/* This file is based on source code of Apache Flink Project (https://flink.apache.org/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. */

pub const MURMUR3_DEFAULT_SEED: i32 = 0;
pub const FLINK_MURMUR3_DEFAULT_SEED: i32 = 42;

const C1: i32 = 0xCC9E_2D51_u32 as i32;
const C2: i32 = 0x1B87_3593;
const R1: u32 = 15;
const R2: u32 = 13;
const M: i32 = 5;
const N: i32 = 0xE654_6B64_u32 as i32;
const CHUNK_SIZE: usize = 4;

/// Hashes the data using 32-bit Murmur3 hash with 0 as seed
///
/// # Arguments
/// * `data` - byte array containing data to be hashed
///
/// # Returns
/// Returns hash value
pub fn hash_bytes(data: &[u8]) -> i32 {
    hash_bytes_with_seed(data, MURMUR3_DEFAULT_SEED)
}

#[inline(always)]
fn hash_bytes_with_seed(data: &[u8], seed: i32) -> i32 {
    let length = data.len();
    let chunks = length / CHUNK_SIZE;
    let length_aligned = chunks * CHUNK_SIZE;

    let mut h1 = hash_full_chunks(data, seed, length_aligned);
    let mut k1 = 0i32;

    for (shift, &b) in data[length_aligned..].iter().enumerate() {
        k1 |= (b as i32) << (8 * shift);
    }

    h1 ^= k1.wrapping_mul(C1).rotate_left(R1).wrapping_mul(C2);

    fmix(h1, length)
}

/// Hashes the data using Fluss'/Flink's variant of 32-bit Murmur hash with 42 as seed and tail bytes mixed into hash byte-by-byte
///
/// # Arguments
/// * `data` - byte array containing data to be hashed
///
/// # Returns
/// * hash value
pub fn fluss_hash_bytes(data: &[u8]) -> i32 {
    fluss_hash_bytes_with_seed(data, FLINK_MURMUR3_DEFAULT_SEED)
}
#[inline(always)]
fn fluss_hash_bytes_with_seed(data: &[u8], seed: i32) -> i32 {
    let length = data.len();
    let chunks = length / CHUNK_SIZE;
    let length_aligned = chunks * CHUNK_SIZE;

    let mut h1 = hash_full_chunks(data, seed, length_aligned);

    #[allow(clippy::needless_range_loop)]
    for index in length_aligned..length {
        let byte = i32::from(data[index]);
        let k1 = mix_k1(byte);
        h1 = mix_h1(h1, k1);
    }

    fmix(h1, length)
}

#[inline(always)]
fn hash_full_chunks(data: &[u8], seed: i32, length_aligned: usize) -> i32 {
    let mut h1 = seed;

    for i in 0..length_aligned / CHUNK_SIZE {
        let offset = i * 4;
        let block = i32::from_le_bytes(data[offset..offset + 4].try_into().unwrap());
        let k1 = mix_k1(block);
        h1 = mix_h1(h1, k1);
    }
    h1
}

#[inline(always)]
fn mix_k1(k1: i32) -> i32 {
    k1.wrapping_mul(C1).rotate_left(R1).wrapping_mul(C2)
}

#[inline(always)]
fn mix_h1(h1: i32, k1: i32) -> i32 {
    (h1 ^ k1).rotate_left(R2).wrapping_mul(M).wrapping_add(N)
}

// Finalization mix - force all bits of a hash block to avalanche
#[inline(always)]
fn fmix(mut h1: i32, length: usize) -> i32 {
    h1 ^= length as i32;
    bit_mix(h1)
}

/// Hashes an i32 using Flink's variant of Murmur
///
/// # Arguments
/// * `code` - byte array containing data to be hashed
///
/// # Returns
/// Returns hash value
pub fn flink_hash_i32(code: i32) -> i32 {
    let mut code = code.wrapping_mul(C1);
    code = code.rotate_left(R1);
    code = code.wrapping_mul(C2);
    code = code.rotate_left(R2);

    code = code.wrapping_mul(M).wrapping_add(N);
    code ^= CHUNK_SIZE as i32;
    code = bit_mix(code);

    if code >= 0 {
        code
    } else if code != i32::MIN {
        -code
    } else {
        0
    }
}

const BIT_MIX_A: i32 = 0x85EB_CA6Bu32 as i32;
const BIT_MIX_B: i32 = 0xC2B2_AE35u32 as i32;

#[inline(always)]
fn bit_mix(mut input: i32) -> i32 {
    input = input ^ ((input as u32) >> 16) as i32;
    input = input.wrapping_mul(BIT_MIX_A);
    input = input ^ ((input as u32) >> 13) as i32;
    input = input.wrapping_mul(BIT_MIX_B);
    input = input ^ ((input as u32) >> 16) as i32;
    input
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_murmur3() {
        //
        let empty_data_hash = hash_bytes(&[]);
        assert_eq!(empty_data_hash, 0);

        let empty_data_hash = hash_bytes_with_seed(&[], 1);
        assert_eq!(0x514E_28B7, empty_data_hash);

        let empty_data_hash = hash_bytes_with_seed(&[], 0xFFFF_FFFFu32 as i32);
        assert_eq!(0x81F1_6F39u32 as i32, empty_data_hash);

        let hash = hash_bytes("The quick brown fox jumps over the lazy dog".as_bytes());
        assert_eq!(0x2E4F_F723, hash);

        let hash = hash_bytes_with_seed(
            "The quick brown fox jumps over the lazy dog".as_bytes(),
            0x9747_B28Cu32 as i32,
        );
        assert_eq!(0x2FA8_26CD, hash);
    }

    #[test]
    fn test_flink_murmur() {
        let empty_data_hash = fluss_hash_bytes_with_seed(&[], 0);
        assert_eq!(empty_data_hash, 0);

        let empty_data_hash = fluss_hash_bytes(&[]);
        assert_eq!(0x087F_CD5C, empty_data_hash);

        let empty_data_hash = fluss_hash_bytes_with_seed(&[], 0xFFFF_FFFFu32 as i32);
        assert_eq!(0x81F1_6F39u32 as i32, empty_data_hash);

        let hash =
            fluss_hash_bytes_with_seed("The quick brown fox jumps over the lazy dog".as_bytes(), 0);
        assert_eq!(0x5FD2_0A20, hash);

        let hash = fluss_hash_bytes("The quick brown fox jumps over the lazy dog".as_bytes());
        assert_eq!(0x1BC6_F880, hash);

        let hash = flink_hash_i32(0);
        assert_eq!(0x2362_F9DE, hash);

        let hash = flink_hash_i32(42);
        assert_eq!(0x43A4_6E1D, hash);

        let hash = flink_hash_i32(-77);
        assert_eq!(0x2EEB_27DE, hash);
    }
}
