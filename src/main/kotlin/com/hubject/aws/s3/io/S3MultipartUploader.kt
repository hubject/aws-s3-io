package com.hubject.aws.s3.io

import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.model.*
import com.fasterxml.jackson.databind.util.ByteBufferBackedInputStream
import java.nio.ByteBuffer
import java.util.concurrent.BlockingQueue
import java.util.concurrent.CompletableFuture
import java.util.concurrent.Future
import java.util.concurrent.LinkedBlockingQueue
import kotlin.concurrent.thread

/**
 * Uploads [ByteBuffer]s in a multipart upload, providing an async interface.
 * Actually there is only one background thread that talks to the S3 API for
 * uploading. This avoids multithreading issues with the S3 SDK.
 *
 * This class is thread safe.
 */
internal class S3MultipartUploader(
    /** The client to upload with */
    val awsS3: AmazonS3,

    val targetBucket: String,

    val targetS3Key: String,

    /**
     * Whether to calculate MD5 checksums of the uploaded data. Turn this
     * off if you are concerned about the CPU load.
     */
    val useChecksums: Boolean = true
) : AutoCloseable {
    /**
     * Whether a multipart upload has been initialized. Is set by the
     * getter of [multipartInitResponse]
     */
    private var multipartUploadHasBeenStarted = false

    /**
     * The response to the initialization of the multipart upload. If
     * none had yet been initialized, initializes it. Once initialized,
     * sets [multipartUploadHasBeenStarted] to `true`.
     */
    private val multipartInitResponse: InitiateMultipartUploadResult by lazy {
        val r = awsS3.initiateMultipartUpload(InitiateMultipartUploadRequest(
            targetBucket, targetS3Key
        ))
        multipartUploadHasBeenStarted = true
        r
    }

    /**
     * New uploads are placed on this queue. The [uploadingThread] picks them
     * up and completes the future
     */
    private val uploadQueue: BlockingQueue<Pair<ByteBuffer, CompletableFuture<Unit>>> = LinkedBlockingQueue(10)

    private val uploadingThread = thread(start = false, name = "S3 multipart uploader s3://$targetBucket/$targetS3Key") {
        var partNumber = 1
        val uploadedPartsETags = mutableListOf<PartETag>()

        while (true) {
            val (partData, partFuture) = try {
                uploadQueue.take()
            } catch (ex: InterruptedException) {
                // we're done polling the queue
                break
            }

            try {
                val partSize = partData.remaining().toLong()

                val metadata = ObjectMetadata()
                metadata.contentLength = partSize
                if (useChecksums) {
                    metadata.contentMD5 = partData.calculateMD5().toBase64()
                }

                ByteBufferBackedInputStream(partData).use { uploadInputStream ->
                    val uploadRequest = UploadPartRequest()
                        .withBucketName(targetBucket)
                        .withKey(targetS3Key)
                        .withUploadId(multipartInitResponse.uploadId)
                        .withFileOffset(0)
                        .withPartSize(partSize)
                        .withPartNumber(partNumber)
                        .withInputStream(uploadInputStream)
                        .withObjectMetadata(metadata)
                        .withMD5Digest(metadata.contentMD5)
                    
                    val partUploadResult = awsS3.uploadPart(uploadRequest)
                    uploadedPartsETags.add(partUploadResult.partETag)
                }

                partFuture.complete(Unit)
                partNumber++
            } catch (ex: Throwable) {
                // abort the upload if necessary
                if (partNumber > 1) {
                    try {
                        awsS3.abortMultipartUpload(AbortMultipartUploadRequest(
                            multipartInitResponse.bucketName,
                            multipartInitResponse.key,
                            multipartInitResponse.uploadId
                        ))
                    } catch (ex2: Throwable) {
                        ex.addSuppressed(ex2)
                    }
                }

                partFuture.completeExceptionally(ex)

                return@thread
            }
        }

        if (partNumber > 1) {
            try {
                val completionResult = awsS3.completeMultipartUpload(CompleteMultipartUploadRequest(
                    targetBucket,
                    targetS3Key,
                    multipartInitResponse.uploadId,
                    uploadedPartsETags.sortedBy { it.partNumber }
                ))
                
                completionFuture.complete(Unit)
            } catch (ex: Throwable) {
                completionFuture.completeExceptionally(ex)
            }
        }
    }

    /**
     * The futures given out from [queuePart]. Is used so that [complete]
     * can wait for all uploads to finish
     */
    private val uploadFutures = mutableListOf<Future<Unit>>()

    /** This is completed by the [uploadingThread] once the multipart has been competed */
    private val completionFuture = CompletableFuture<Unit>()

    /**
     * Is set to true when [complete] or [close] is called. Once set to true, must not
     * change back to false. If true, [queuePart] will no longer accept new
     * parts
     */
    var isClosed = false
        private set

    private val isClosedMutex = Any()

    /** The number of parts uploaded and currently queued for upload */
    val nPartsUploaded: Int
        get() = uploadFutures.size

    /**
     * Queues data for uploading. The future completes when the upload is done.
     * @param partData The data to upload - after passing to this method, **do not modify**
     *                 until the returned future has completed; bad things might happen otherwise.
     */
    fun queuePart(partData: ByteBuffer): Future<Unit> {
        if (isClosed) {
            throw IllegalStateException("Upload already closed / during completion")
        }

        val future = CompletableFuture<Unit>()
        uploadQueue.put(Pair(partData, future))

        synchronized(uploadFutures) {
            uploadFutures.add(future)
        }

        if (!uploadingThread.isAlive) {
            try {
                uploadingThread.start()
            } catch (ex: IllegalThreadStateException) {
                // race condition starting the thread -> all good
            }
        }

        return future
    }

    fun complete(): Future<Unit> {
        synchronized(isClosedMutex) {
            if (isClosed) {
                throw IllegalStateException("Upload already closed / during completion")
            }
            isClosed = true
        }

        if (uploadFutures.isEmpty()) {
            return CompletableFuture.completedFuture(Unit)
        } else {
            // only if any uploads were actually started

            val future = CompletableFuture<Unit>()
            thread(start = true, name = "multipart upload completion s3://$targetBucket/$targetS3Key") {
                uploadFutures.forEach { it.get() /* join */ }
                uploadingThread.interrupt()

                try {
                    future.complete(completionFuture.get())
                } catch (ex: Throwable) {
                    future.completeExceptionally(ex)
                }
            }
            return future
        }
    }

    /** Implements [AutoCloseable.close] by forwarding to [complete] */
    override fun close() {
        complete().get()
    }
}