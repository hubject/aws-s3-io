package com.hubject.aws.s3.io

import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.model.*
import io.kotlintest.shouldBe
import io.kotlintest.shouldNotBe
import io.kotlintest.specs.FreeSpec
import io.mockk.every
import io.mockk.mockk
import io.mockk.mockkStatic
import io.mockk.verify
import sun.misc.BASE64Encoder
import java.nio.ByteBuffer

class S3MultipartUploaderTest : FreeSpec() {
    override fun isInstancePerTest(): Boolean {
        return true
    }

init {
    val s3mock = mockk<AmazonS3>()

    every { s3mock.initiateMultipartUpload(any()) } answers { call ->
        val request = call.invocation.args[0] as InitiateMultipartUploadRequest
        val result = InitiateMultipartUploadResult()
        result.bucketName = request.bucketName
        result.key = request.key
        result.uploadId = "some-random-upload-id"
        result
    }

    val partUploadRequests = mutableListOf<UploadPartRequest>()
    every { s3mock.uploadPart(capture(partUploadRequests) )} answers { call ->
        val request = call.invocation.args[0] as UploadPartRequest
        val result = UploadPartResult()
        result.eTag = "this-is-some-random-eTag-" + (Math.random() * 10000000.0).toInt()
        result.partNumber = request.partNumber

        Thread.sleep(500)

        result
    }

    every { s3mock.completeMultipartUpload(any()) } answers { call ->
        val request = call.invocation.args[0] as CompleteMultipartUploadRequest
        val result = CompleteMultipartUploadResult()
        result.bucketName = request.bucketName
        result.key = request.key
        result.location = "https://this-host-does-not-exist/nor-does-the-path"
        result.eTag = "this-is-some-random-eTag-" + (Math.random() * 10000000.0).toInt()
        result
    }

    val buffer = ByteBuffer.allocate(2048)

    "multipart upload is started" {
        // SETUP
        val uploader = S3MultipartUploader(s3mock, "bucketname", "key", true)

        // ACT
        uploader.queuePart(buffer).get()

        // ASSERT
        verify { s3mock.initiateMultipartUpload(any()) }
    }

    "parts are being uploaded" {
        // SETUP
        val uploader = S3MultipartUploader(s3mock, "bucketname", "key", true)

        // ACT
        uploader.queuePart(buffer).get()

        // ASSERT
        verify { s3mock.uploadPart(any()) }
        partUploadRequests.size shouldBe 1
    }

    "part numbers are correct" {
        // SETUP
        val uploader = S3MultipartUploader(s3mock, "bucketname", "key", true)

        // ACT
        uploader.queuePart(buffer).get()
        uploader.queuePart(buffer).get()
        uploader.queuePart(buffer).get()

        // ASSERT
        partUploadRequests.size shouldBe 3
        assert(partUploadRequests.any { it.partNumber == 1 })
        assert(partUploadRequests.any { it.partNumber == 2 })
        assert(partUploadRequests.any { it.partNumber == 3 })
    }

    "multipart upload is completed" {
        // SETUP
        val uploader = S3MultipartUploader(s3mock, "bucketname", "key", true)

        // ACT
        // actually queue data, otherwise it wont attempt to upload anything
        uploader.queuePart(buffer) // don't join them here, complete() should do that
        uploader.queuePart(buffer)

        uploader.complete().get()

        // ASSERT
        verify { s3mock.completeMultipartUpload(any()) }
    }

    "MD5 checksums are correct" {
        // SETUP
        val uploader = S3MultipartUploader(s3mock, "bucketname", "key", true)
        val data = ByteBuffer.allocate(500)
        data.put("This is some data to be uploaded and correctly hashed".toByteArray(Charsets.UTF_8))

        val md5binary = byteArrayOf(
            // intentionally incorrect to make sure the mock works
            0x72.toByte(), 0xbc.toByte(), 0x96.toByte(), 0xe1.toByte(), 0xa1.toByte(),
            0x33.toByte(), 0xa8.toByte(), 0xbe.toByte(), 0x2b.toByte(), 0x59.toByte(),
            0xba.toByte(), 0xb8.toByte(), 0xbf.toByte(), 0x09.toByte(), 0xe8.toByte(),
            0x7c.toByte()
        )
        val b64md5 = BASE64Encoder().encode(md5binary)

        mockkStatic("com.hubject.aws.s3.io.Md5Kt")
        every { data.calculateMD5() } returns md5binary

        // ACT
        uploader.queuePart(data).get()

        // VERIFY
        partUploadRequests.size shouldBe 1
        partUploadRequests[0].md5Digest shouldBe b64md5
        partUploadRequests[0].objectMetadata.contentMD5 shouldBe b64md5
    }
}}