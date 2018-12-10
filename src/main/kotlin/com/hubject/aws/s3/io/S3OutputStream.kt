package com.hubject.aws.s3.io

import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.model.ObjectMetadata
import com.fasterxml.jackson.databind.util.ByteBufferBackedInputStream
import java.io.IOException
import java.io.OutputStream
import java.nio.ByteBuffer
import java.util.concurrent.Future

/**
 * Facilitates streaming to S3 in case the actual file size is not
 * known upfront: uploading via the regular single-file upload will
 * cache the contents in memory and cause an out-of-memory error,
 * especially on AWS lambda. This may happen if you have to stream
 * several GB to S3.
 *
 * This class provides an OutputStream to write to. It will initiate
 * a multipart upload and thereby limit the amount of data currently
 * in memory.
 *
 * Upon closing this stream, the upload to S3 will be completed. Note
 * that, for this reason, the [close] method may block and throw all
 * sorts of exceptions.
 *
 * This class is **NOT thread safe!**
 *
 * ----
 *
 * Internally, there are two buffers. One is used to buffer incoming writes.
 * As soon as that is full, it will be switched over for an empty one. Then,
 * the contents of the filled buffer are uploaded in parallel. If the write
 * buffer is filled before the other buffer has been uploaded, the stream
 * blocks until the upload is completed.
 *
 * This means that, for optimal performance, you should set the
 */
class S3OutputStream(
    /** The client to upload with */
    val awsS3: AmazonS3,

    val targetBucket: String,

    val targetS3Key: String,

    /** The maximum amount of memory to use as buffer, in bytes */
    val maxLocalCache: Long = Math.max(MIN_UPLOAD_PART_SIZE * 4L, 1024L * 1024L * 50L /* 50 MiB */),

    /**
     * Whether to calculate MD5 checksums of the uploaded data. Turn this
     * off if you are concerned about the CPU load.
     */
    val useChecksums: Boolean = true,

    /**
     * The buffers for local cache are obtained from here. Pass a custom implementation
     * if you want to change the behaviour or share memory.
     */
    byteBufferPool: ByteBufferPool = ByteBufferPool.DEFAULT
) : OutputStream() {
    init {
        if (maxLocalCache < MIN_UPLOAD_PART_SIZE * 2) {
            throw IllegalArgumentException("The local cache must be at least ${MIN_UPLOAD_PART_SIZE * 2} " +
                "bytes (because single upload parts must be at least ${MIN_UPLOAD_PART_SIZE} bytes)")
        }
    }

    /**
     * The actual amount of local cache; this caps the requested [maxLocalCache] to what
     * the [ByteBuffer]s can hold (2GiB each).
     */
    val actualLocalCache: Long = Math.min(Int.MAX_VALUE.toLong() * 2, maxLocalCache)

    /**
     * Nullable so that it can be released on [close]
     */
    private var byteBufferPool: ByteBufferPool? = byteBufferPool

    /**
     * Receives the data from the write invocations. Nullable so that it can be
     * released when this stream is closed.
     */
    private var writeBuffer: ByteBuffer? = byteBufferPool.pop((actualLocalCache / 2L).toInt())

    /**
     * Serves as the backend for the current upload; by
     * convention, the thread currently uploading (see [uploadingThread])
     * has exclusive access to this variable. The switch is
     * done by [uploadCurrentBufferAndSwitch] and nowhere else.
     *
     * Nullable so that it can be released when this stream is closed.
     */
    private var uploadBuffer: ByteBuffer? = byteBufferPool.pop((maxLocalCache / 2L).toInt())

    private val uploader: S3MultipartUploader by lazy { S3MultipartUploader(awsS3, targetBucket, targetS3Key, useChecksums) }

    /**
     * Handle to the most recent part upload via [uploader]
     */
    private var mostRecentUploadHandle: Future<Unit>? = null

    /**
     * Set to true once the closing operation has been started. Remains true
     * when closed.
     */
    private var isClosed = false

    override fun write(b: Int) {
        if (isClosed) {
            throw IOException("Stream already closed.")
        }

        if (!writeBuffer!!.hasRemaining()) {
            uploadCurrentBufferAndSwitch()
        }

        writeBuffer!!.put(b.toByte())
    }

    override fun write(b: ByteArray?, off: Int, len: Int) {
        if (isClosed) {
            throw IOException("Stream already closed.")
        }

        if (!writeBuffer!!.hasRemaining()) {
            uploadCurrentBufferAndSwitch()
        }

        if (len > writeBuffer!!.remaining()) {
            val nBytes = writeBuffer!!.remaining()
            write(b, off, nBytes)
            write(b, off + nBytes, len - nBytes)
            // recursion handles the rest
            return
        }

        writeBuffer!!.put(b, off, len)
    }

    /**
     * Flushes this *ONLY* if there is enough data in the buffer,
     * see [MIN_UPLOAD_PART_SIZE]
     */
    override fun flush() {
        if (isClosed) {
            throw IllegalStateException("Stream already closed.")
        }

        if (writeBuffer!!.position() >= MIN_UPLOAD_PART_SIZE) {
            uploadCurrentBufferAndSwitch()
        }
    }

    /**
     * Fully flushes the stream and completes the upload.
     */
    override fun close() {
        if (isClosed) return

        // prevent any more data from being written
        this.isClosed = true

        waitForCurrentUploadToFinish()

        val nBytesRemaining = writeBuffer!!.position().toLong()

        if (uploader.nPartsUploaded > 0 || nBytesRemaining > MAX_SINGLE_FILE_UPLOAD_SIZE) {
            if (nBytesRemaining > 0) {
                uploadCurrentBufferAndSwitch(isLastPart = true)
                waitForCurrentUploadToFinish()
            }

            uploader.complete()
        } else {
            // this can be done in a single take
            writeBuffer!!.flip()

            val metadata = ObjectMetadata()
            metadata.contentLength = writeBuffer!!.remaining().toLong()
            if (useChecksums) {
                metadata.contentMD5 = writeBuffer!!.calculateMD5().toBase64()
            }

            ByteBufferBackedInputStream(writeBuffer).use { uploadInputStream ->
                awsS3.putObject(
                    targetBucket,
                    targetS3Key,
                    uploadInputStream,
                    metadata
                )
            }
        }

        // release resources
        byteBufferPool!!.free(writeBuffer!!)
        writeBuffer = null

        byteBufferPool!!.free(uploadBuffer!!)
        uploadBuffer = null

        byteBufferPool = null
    }

    /**
     * Starts to upload the contents of [writeBuffer] and switches
     * the buffers so that [writeBuffer] can receive more data.
     *
     * If an upload is still in progress, blocks until that is finished
     * before proceeding.
     *
     * @param isLastPart Whether this is the last part of the entire stream;
     *                   supposed to be set to `true` only by [close]. If true,
     *                   the part size for S3 (see [UploadPartRequest.withPartSize])
     *                   will be set to the minimum, regardless of whether as much
     *                   data is actually present.
     *
     * @throws IllegalStateException If [writeBuffer] contains too few data and [isLastPart] is false, see [MIN_UPLOAD_PART_SIZE]
     */
    private fun uploadCurrentBufferAndSwitch(isLastPart: Boolean = false) {
        val nBytesToUpload = writeBuffer!!.position()
        if (nBytesToUpload == 0) return

        if (!isLastPart && nBytesToUpload < MIN_UPLOAD_PART_SIZE) {
            throw IllegalStateException("Cannot upload - minimum upload size is ${MIN_UPLOAD_PART_SIZE} bytes, got only ${writeBuffer!!.position()}")
        }

        waitForCurrentUploadToFinish()

        // at this point, uploadBuffer is free for usage again
        // but the contents of writeBuffer have to be saved!

        val bufferToUpload = this.writeBuffer!!
        this.writeBuffer = this.uploadBuffer
        this.writeBuffer!!.clear() // make sure we have a fresh start
        this.uploadBuffer = bufferToUpload

        // now, things can continue to be written to writeBuffer
        // while newUploadBuffer gets transferred to S3
        bufferToUpload.flip()
        mostRecentUploadHandle = uploader.queuePart(bufferToUpload)
    }

    /** If an upload is taking place, blocks until that is finished. */
    private fun waitForCurrentUploadToFinish() {
        mostRecentUploadHandle?.get()
    }

    /** @suppres */
    companion object {
        /**
         * The minimum size for single parts of multipart uploads. This
         * value is defined by the AWS S3 people.
         *
         * See [https://docs.aws.amazon.com/AmazonS3/latest/dev/qfacts.html]
         */
        const val MIN_UPLOAD_PART_SIZE = 1024 * 1024 * 5 // 5MiB

        /**
         * The maximum size of a single part in a multipart upload. This
         * value is defined by the AWS S3 people.
         *
         * See [https://docs.aws.amazon.com/AmazonS3/latest/dev/qfacts.html]
         */
        const val MAX_UPLOAD_PART_SIZE: Long = 1024L * 1024L * 1024L * 5L // 5GiB

        /**
         * The maximum size for a single file upload. This value is defined by
         * the AWS S3 people.
         *
         * See [https://docs.aws.amazon.com/AmazonS3/latest/dev/UploadingObjects.html]
         */
        const val MAX_SINGLE_FILE_UPLOAD_SIZE: Long = 1024L * 1024L * 1024L * 5L // 5GiB
    }
}

