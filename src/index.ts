// Copyright (c) 2022 Distributed Network Disk Group
// SPDX-License-Identifier: Apache-2.0

import { FileMeta, MetaClient } from './meta'
import { S3Client } from './s3'
import { RSE } from './rse'
import * as utils from './utils'

export interface Config {
  dataShardNum: number
  parityShardNum: number
  redisUrl: string
  shardSize?: number
  bucket?: string
}

const bucketDefault = 'distnetdisk'
const shardSizeDefault = 16 * 1024 * 1024

export class Client {
  private config: Required<Config>
  private metaClient: MetaClient
  private rse: RSE

  constructor(config: Config) {
    config.shardSize = config.shardSize ?? shardSizeDefault
    config.bucket = config.bucket ?? bucketDefault
    this.config = config as Required<Config>
    this.metaClient = new MetaClient(this.config.redisUrl)
    this.rse = new RSE(this.config.dataShardNum, this.config.parityShardNum)
  }

  async upload(path: string, file: Blob) {
    const servers = await this.metaClient.servers()
    const shardsIter = this.slice(file)
    const meta: FileMeta = {
      modified: Date.now(),
      size: file.size,
      shards: [],
      padding: 0,
      parityNum: this.config.parityShardNum,
      shardSize: this.config.shardSize,
    }
    const indexes = this.distribute(servers.length, this.config.dataShardNum)
    meta.shards = indexes.map(i => servers[i]!.id)
    const s3Clients = indexes
      .map(i => servers[i]!)
      .map(server => new S3Client(server.url, this.config.bucket, this.config.shardSize))
    const uploadIds = [] as string[]
    for (let i = 0; i < s3Clients.length; i++) {
      uploadIds.push(await s3Clients[i]!.uploadStart(path, i))
    }
    let i = 0
    while (true) {
      const { done, value } = shardsIter.next()
      if (done) {
        meta.padding = value as number
        break
      }
      const shards = value as Blob[]
      let bins = await utils.convertBlob2View(shards)
      const resBins = await this.rse.encode(bins)
      for (let j = 0; j < resBins.length; j++) {
        await s3Clients[j]!.uploadPart(path, j, uploadIds[j]!, new Blob([resBins[j]!]), i)
      }
      i++
    }
    for (let i = 0; i < s3Clients.length; i++) {
      await s3Clients[i]!.uploadOk(path, i, uploadIds[i]!)
    }
    await this.metaClient.set(path, meta)
  }

  async download(path: string) {
    const servers = await this.metaClient.servers()
    const meta = await this.metaClient.get(path)
    const s3Clients = meta.shards
      .map(serverId => servers.find(server => server.id == serverId))
      .map(server => (server ? new S3Client(server.url, this.config.bucket, this.config.shardSize) : undefined))
    const lost = servers.length - s3Clients.filter(Boolean).length
    if (lost > meta.parityNum) {
      throw new Error(`File corrupt: Only allow ${meta.parityNum} lost but ${lost} lost actually`)
    }
    let res = [] as Blob[]
    for (let i = 0; i < Math.ceil(meta.size / meta.shardSize); i++) {
      let shards = [] as (Blob | null)[]
      for (let j = 0; j < s3Clients.length; j++) {
        const s3Client = s3Clients[j]
        if (s3Client == undefined) {
          shards.push(null)
        } else {
          shards.push(await s3Client.downloadPart(path, j, i))
        }
      }
      const bins = await utils.convertBlob2ViewNullable(shards)
      const resBins = await this.rse.reconstruct(bins)
      res.push(new Blob(resBins.slice(0, shards.length - meta.parityNum)))
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
    const tail = file.slice(n * this.config.dataShardNum * this.config.shardSize)
    const padding = this.config.shardSize * this.config.dataShardNum - tail.size
    const tailBlob = new Blob([tail, new ArrayBuffer(padding)])
    yield [...Array(this.config.dataShardNum).keys()].map(i =>
      tailBlob.slice(i * this.config.shardSize, (i + 1) * this.config.shardSize)
    )
    return padding
  }

  private distribute(serverNum: number, shardNum: number) {
    if (serverNum < shardNum) {
      throw new Error(
        `Servers not enough: dataShardNum = ${this.config.dataShardNum}, parityShardNum = ${this.config.parityShardNum}`
      )
    }
    return utils.randChoice([...new Array(serverNum).keys()], shardNum) as number[]
  }
}
