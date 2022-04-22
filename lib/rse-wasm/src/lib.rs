// Copyright (c) 2022 Distributed Network Disk Group
// SPDX-License-Identifier: Apache-2.0

mod utils;

use js_sys::Uint8Array;
use wasm_bindgen::prelude::*;

#[cfg(feature = "wee_alloc")]
#[global_allocator]
static ALLOC: wee_alloc::WeeAlloc = wee_alloc::WeeAlloc::INIT;

use reed_solomon_erasure::galois_8::ReedSolomon;

#[derive(Debug)]
pub struct RSE {
  reed_solomon: ReedSolomon,
}

impl RSE {
  pub fn new(data_shard_num: usize, parity_shard_num: usize) -> Self {
    Self {
      reed_solomon: ReedSolomon::new(data_shard_num, parity_shard_num).unwrap(),
    }
  }

  pub fn encode(&self, mut shards: Vec<Vec<u8>>) -> Result<Vec<Vec<u8>>, String> {
    self
      .reed_solomon
      .encode(&mut shards)
      .map_err(|_| "RSE failed to encode".to_owned())?;
    Ok(shards)
  }

  pub fn reconstruct(&self, mut shards: Vec<Option<Vec<u8>>>) -> Result<Vec<Vec<u8>>, String> {
    self
      .reed_solomon
      .reconstruct_data(&mut shards)
      .map_err(|_| "RSE failed to reconstruct".to_owned())?;
    Ok(
      shards
        .into_iter()
        .map(|shard| shard.unwrap())
        .collect::<Vec<_>>(),
    )
  }
}

#[wasm_bindgen]
#[derive(Debug)]
pub struct RSEConfig {
  pub data_shard_num: usize,
  pub parity_shard_num: usize,
}

#[wasm_bindgen]
pub fn encode(config: RSEConfig, data: Vec<Uint8Array>) -> Result<Vec<Uint8Array>, JsValue> {
  let rse = RSE::new(config.data_shard_num, config.parity_shard_num);
  let shards = data.into_iter().map(|arr| arr.to_vec()).collect::<Vec<_>>();
  let res = rse.encode(shards).map_err(|e| e.to_string())?;
  Ok(
    res
      .into_iter()
      .map(|shard| unsafe { Uint8Array::view(shard.as_slice()) })
      .collect::<Vec<_>>(),
  )
}

#[wasm_bindgen]
pub fn reconstruct(config: RSEConfig, data: Vec<Uint8Array>) -> Result<Vec<Uint8Array>, JsValue> {
  let rse = RSE::new(config.data_shard_num, config.parity_shard_num);
  let shards = data
    .into_iter()
    .map(|arr| arr.to_vec())
    .map(|arr| if arr.len() == 0 { None } else { Some(arr) })
    .collect::<Vec<_>>();
  let res = rse.reconstruct(shards)?;
  Ok(
    res
      .into_iter()
      .map(|shard| unsafe { Uint8Array::view(shard.as_slice()) })
      .collect::<Vec<_>>(),
  )
}
