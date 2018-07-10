package com.hubject.aws.s3.io

import io.kotlintest.shouldBe
import io.kotlintest.specs.FreeSpec
import java.nio.ByteBuffer

class MD5Test : FreeSpec() { init {
    "empty buffer with capacity" {
        // SETUP
        val buffer = ByteBuffer.allocate(500)
        buffer.put("Some data that should affect the MD5 sum when considered".toByteArray(Charsets.UTF_8))
        // reset the buffer so the data is not officially in the buffer
        buffer.position(0)
        buffer.limit(0)

        // ACT
        val md5 = buffer.calculateMD5().toHex()

        // ASSERT
        md5 shouldBe "d41d8cd98f00b204e9800998ecf8427e"
    }

    "buffer with some data and some remaining" {
        // SETUP
        val buffer = ByteBuffer.allocate(500)
        buffer.put("Some data that should affect the MD5 sum when considered".toByteArray(Charsets.UTF_8))
        // reset the buffer so the data is not officially in the buffer
        buffer.flip()

        // ACT
        val md5 = buffer.calculateMD5().toHex()

        // ASSERT
        md5 shouldBe "ce0559c6a2e0b42c49b92a332839927f"
    }

    "buffer filled to the brink" {
        // SETUP
        val data = "Some data of which the MD5 should be calculated".toByteArray(Charsets.UTF_8)
        val buffer = ByteBuffer.allocate(data.size)
        buffer.put(data)
        buffer.flip()

        // ACT
        val md5 = buffer.calculateMD5().toHex()

        // ASSERT
        md5 shouldBe "e08ca5c09e3050c438fe651532ba8005"
    }
}}

private fun ByteArray.toHex(): String = joinToString(separator = "") {
    var str = (it.toInt() and 0xFF).toString(16)
    if (str.length == 1) {
        str = "0" + str
    }
    str
}