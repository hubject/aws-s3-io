package com.hubject.aws.s3.io

import sun.misc.BASE64Encoder
import java.nio.ByteBuffer
import java.security.MessageDigest

/**
 * Used to calculate checksums; one per thread.
 */
private val md5digests = object : ThreadLocal<Pair<MessageDigest, ByteArray>>() {
    override fun initialValue(): Pair<MessageDigest, ByteArray> {
        return Pair(
            MessageDigest.getInstance("MD5"),
            ByteArray(2048)
        )
    }
}

/**
 * Calculates the checksum of the remaining data in the buffer. The buffer
 * is returned to its initial state before this method returns.
 */
internal fun ByteBuffer.calculateMD5(): ByteArray {
    val (messageDigest, buffer) = md5digests.get()

    messageDigest.reset()

    var nBytes: Int
    do {
        nBytes = Math.min(this.remaining(), buffer.size)
        if (nBytes > 0) {
            this.get(buffer, 0, nBytes)
            messageDigest.update(buffer, 0, nBytes)
        }
    } while (nBytes > 0)

    this.flip()

    return messageDigest.digest()
}

internal fun ByteArray.toBase64(): String = BASE64Encoder().encode(this)