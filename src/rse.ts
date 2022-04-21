// Copyright (c) 2022 Distributed Network Disk Group
// SPDX-License-Identifier: Apache-2.0

export class RSE {
  constructor(private dataShardNum: number, private parityShardNum: number) {}

  async encode(data: Uint8Array[]): Promise<Uint8Array[]> {}

  async reconstruct(data: Uint8Array[]): Promise<Uint8Array[]> {}
}
