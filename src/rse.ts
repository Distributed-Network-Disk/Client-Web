// Copyright (c) 2022 Distributed Network Disk Group
// SPDX-License-Identifier: Apache-2.0

import { RSEConfig, encode, reconstruct } from 'rse-wasm'

export class RSE {
  constructor(private dataShardNum: number, private parityShardNum: number) {}

  async encode(data: Uint8Array[]) {
    const config = new RSEConfig()
    config.data_shard_num = this.dataShardNum
    config.parity_shard_num = this.parityShardNum
    return encode(config, data)
  }

  async reconstruct(data: (Uint8Array | null)[]) {
    const dataNoNull = data.map(d => d ?? new Uint8Array(0))
    const config = new RSEConfig()
    config.data_shard_num = this.dataShardNum
    config.parity_shard_num = this.parityShardNum
    return reconstruct(config, dataNoNull)
  }
}
