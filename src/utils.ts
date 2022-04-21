// Copyright (c) 2022 Distributed Network Disk Group
// SPDX-License-Identifier: Apache-2.0

export async function convertBlob2View(blobs: Blob[]) {
  let res = [] as Uint8Array[]
  for (const blob of blobs) {
    const arrBuf = await blob.arrayBuffer()
    res.push(new Uint8Array(arrBuf))
  }
  return res
}

export function randChoice<T>(array: T[], n?: number) {
  if (n == undefined) {
    return array[Math.floor(Math.random() * array.length)]!
  }
  if (n > array.length) {
    n = array.length
  }
  for (let i = 0; i < n; i++) {
    const rand = Math.floor(Math.random() * (n - i)) + i
    const tmp = array[i]!
    array[i] = array[rand]!
    array[rand] = tmp
  }
  return array.slice(0, n)
}
