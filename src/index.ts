// Copyright (c) 2022 Distributed Network Disk Group
// SPDX-License-Identifier: Apache-2.0

import { MetaClient } from './meta'
import { RSE } from './rse'

export interface Config {
  dataShardNum: number
  parityShardNum: number
  shardSize: number
  redisUrl: string
}

export class Client {
  constructor(private config: Config) {}

  async upload(path: string, file: Blob) {
    const rse = new RSE(this.config.dataShardNum, this.config.parityShardNum)
    const metaClient = new MetaClient(this.config.redisUrl)
    const servers = await metaClient.servers()
    const shardsIter = this.slice(file)
    let padding: number
    const meta = {
      modified: Date.now(),
      size: file.size,
      shards: [] as string[][],
    }
    while (true) {
      const { done, value } = shardsIter.next()
      if (done) {
        padding = value as number
        break
      }
      const shards = value as Blob[]
      let bins = [] as Uint8Array[]
      for (const shard of shards) {
        const arrBuf = await shard.arrayBuffer()
        bins.push(new Uint8Array(arrBuf))
      }
      const resBins = await rse.encode(bins)
      const indexes = this.distribute(servers.length, resBins.length)
      // Do upload
      meta.shards.push(indexes.map(i => servers[i]!.id))
    }
    await metaClient.set(path, meta)
  }

  async download(path: string) {
    const rse = new RSE(this.config.dataShardNum, this.config.parityShardNum)
    const metaClient = new MetaClient(this.config.redisUrl)
    const servers = await metaClient.servers()
    const meta = await metaClient.get(path)
    let res = [] as Blob[]
    for (let shards of meta.shards) {
      // Do download
      const shardBlobs = [] as Blob[]
      let bins = [] as Uint8Array[]
      for (const shardBlob of shardBlobs) {
        const arrBuf = await shardBlob.arrayBuffer()
        bins.push(new Uint8Array(arrBuf))
      }
      const resBins = rse.reconstruct(bins)
      res.push(new Blob(resBins))
    }
    return new Blob(res)
  }

  private *slice(file: Blob): Generator<Blob[], number, undefined> {
    const n = Math.floor(file.size / (this.config.shardSize * this.config.dataShardNum))
    for (let i = 0; i < n; i++) {
      yield [...Array(this.config.dataShardNum).keys()].map(j =>
        file.slice(
          (i * this.config.dataShardNum + j) * this.config.shardSize,
          (i * this.config.dataShardNum + j + 1) * this.config.shardSize
        )
      )
    }
    const tail = file.slice(n * this.config.shardSize)
    const padding = this.config.shardSize * this.config.dataShardNum - tail.size
    const tailBlob = new Blob([tail, new ArrayBuffer(padding)])
    yield [...Array(this.config.dataShardNum).keys()].map(i =>
      tailBlob.slice(i * this.config.shardSize, (i + 1) * this.config.shardSize)
    )
    return padding
  }

  private distribute(serverNum: number, shardNum: number) {
    let res = [] as number[]
    const n = Math.ceil(shardNum / serverNum)
    for (let i = 0; i < n; i++) {
      let indexes = [...new Array(serverNum).keys()]
      const m = Math.min(serverNum, shardNum - (n - 1) * serverNum)
      for (let j = 0; j < m; j++) {
        const rand = Math.floor(Math.random() * (m - j)) + j
        const tmp = indexes[j]!
        indexes[j] = indexes[rand]!
        indexes[rand] = tmp
      }
      res.push(...indexes.slice(0, m))
    }
    return res
  }
}
