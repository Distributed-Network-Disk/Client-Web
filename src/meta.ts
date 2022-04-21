// Copyright (c) 2022 Distributed Network Disk Group
// SPDX-License-Identifier: Apache-2.0

import Redis from 'ioredis'

export class MetaClient {
  private redis: Redis

  constructor(redisUrl: string) {
    this.redis = new Redis(redisUrl)
  }

  async servers() {
    const ids = await this.redis.keys('server:*')
    const metas = await this.redis.mget(...ids)
    return ids.map((id, i) => ({ id, ...JSON.parse(metas[i]!) } as ServerMeta & { id: string }))
  }

  async set(path: string, meta: FileMeta) {
    await this.redis.set(`file:${path}`, JSON.stringify(meta))
  }

  async get(path: string) {
    const meta = await this.redis.get(`file:${path}`)
    if (meta == null) {
      throw new Error(`File not exists: path = ${path}`)
    }
    return JSON.parse(meta) as FileMeta
  }

  async close() {
    await this.redis.quit()
  }
}

export interface ServerMeta {
  name: string
  url: string
}

export interface FileMeta {
  shards: string[]
  modified: number
  size: number
  padding: number
  parityNum: number
  shardSize: number
}
