// Copyright (c) 2022 Distributed Network Disk Group
// SPDX-License-Identifier: Apache-2.0

export class MetaClient {
  constructor(private redisUrl: string) {}

  async servers() {
    const keysRes = await fetch(`${this.redisUrl}/keys/${encodeURIComponent('server:*')}`)
    if (keysRes.status != 200) {
      throw new Error(`keys request failed: status = ${keysRes.status}`)
    }
    const ids = ((await keysRes.json()) as any).keys as string[]
    const mgetRes = await fetch(`${this.redisUrl}/mget/${ids.map(id => encodeURIComponent(id)).join('/')}`)
    if (mgetRes.status != 200) {
      throw new Error(`mget request failed: status = ${mgetRes.status}`)
    }
    const metas = ((await mgetRes.json()) as any).mget as string[]
    return ids.map((id, i) => ({ id, ...JSON.parse(metas[i]!) } as ServerMeta & { id: string }))
  }

  async set(path: string, meta: FileMeta) {
    const res = await fetch(
      `${this.redisUrl}/set/${encodeURIComponent(`file:${path}`)}/${encodeURIComponent(JSON.stringify(meta))}`
    )
    if (res.status != 200) {
      throw new Error(`set request failed: status = ${res.status}`)
    }
  }

  async get(path: string) {
    const res = await fetch(`${this.redisUrl}/get/${encodeURIComponent(`file:${path}`)}`)
    if (res.status != 200) {
      throw new Error(`get request failed: status = ${res.status}`)
    }
    const meta = (await ((await res.json()) as any).get) as string | undefined
    if (!meta) {
      throw new Error(`File not exists: path = ${path}`)
    }
    return JSON.parse(meta) as FileMeta
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
