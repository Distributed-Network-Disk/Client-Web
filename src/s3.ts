// Copyright (c) 2022 Distributed Network Disk Group
// SPDX-License-Identifier: Apache-2.0

import {
  S3Client as RawS3Client,
  CreateMultipartUploadCommand,
  UploadPartCommand,
  CompleteMultipartUploadCommand,
} from '@aws-sdk/client-s3'

export class S3Client {
  private client: RawS3Client

  constructor(url: string, private bucket: string) {
    this.client = new RawS3Client({
      endpoint: url,
    })
  }

  async download(path: string, seq: number): Promise<Blob> {}

  async uploadStart(path: string, seq: number) {
    const res = await this.client.send(
      new CreateMultipartUploadCommand({
        Bucket: this.bucket,
        Key: `${path}.${seq}`,
      })
    )
    return res.UploadId as string
  }

  async uploadPart(path: string, seq: number, uploadId: string, file: Blob, partNum: number) {
    await this.client.send(
      new UploadPartCommand({
        Bucket: this.bucket,
        Key: `${path}.${seq}`,
        PartNumber: partNum,
        UploadId: uploadId,
        Body: file,
      })
    )
  }

  async uploadOk(path: string, seq: number, uploadId: string) {
    await this.client.send(
      new CompleteMultipartUploadCommand({
        Bucket: this.bucket,
        Key: `${path}.${seq}`,
        UploadId: uploadId,
      })
    )
  }
}
