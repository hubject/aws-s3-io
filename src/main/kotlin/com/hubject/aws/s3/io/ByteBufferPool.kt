package com.hubject.aws.s3.io

import java.nio.ByteBuffer

interface ByteBufferPool {
    /**
     * Returns a buffer that has at least the given amount of bytes
     * of available space. If no such buffer is currently free, one
     * will be allocated.
     */
    fun pop(minSize: Int): ByteBuffer

    /**
     * Returns this buffer to the pool. User code must not hold any
     * references to the given buffer after this method returns.
     */
    fun free(buffer: ByteBuffer)

    companion object {
        /**
         * @return a suitable default implementation
         * @see SimpleByteBufferPool
         */
        val DEFAULT: ByteBufferPool by lazy { SimpleByteBufferPool() }
            @JvmName("default") get
    }
}

/**
 * A pool for byte buffers, similar to connection pools.
 * Avoids frequent re-allocation.
 *
 * Example:
 *
 *    // obtain a buffer
 *    val buffer = bufferPool.get(1024 * 50 /* 50 KiB */)
 *
 *    // use the buffer
 *    buffer.write(someData)
 *    writeData(buffer)
 *
 *    // return it
 *    bufferPool.put(buffer)
 */
class SimpleByteBufferPool
@JvmOverloads constructor(
    /**
     * Whether to allocate memory off-heap.
     * @see ByteBuffer.allocateDirect
     */
    val allocateDirect: Boolean = true,

    /**
     * Maximum number of spare buffers
     */
    val maxSpareBuffers: Int = 2
) : ByteBufferPool {
    /**
     * The spare buffers. Each buffer is [ByteBuffer.clear()]ed before being
     * added to to this list.
     */
    private val spareBuffers = ArrayList<ByteBuffer>()

    /**
     * Allocates a new byte buffer of the given size, takes [allocateDirect]
     * into account.
     */
    private val allocator: (Int) -> ByteBuffer = if (allocateDirect) ByteBuffer::allocateDirect else ByteBuffer::allocate

    override fun pop(minSize: Int): ByteBuffer {
        synchronized(spareBuffers) {
            val buffer: ByteBuffer? = spareBuffers
                .filter { it.capacity() >= minSize }
                .minBy { it.capacity() }

            if (buffer == null) {
                return createBuffer(minSize)
            } else {
                spareBuffers.remove(buffer)
                buffer.clear()
                return buffer
            }
        }
    }

    override fun free(buffer: ByteBuffer): Unit {
        synchronized(spareBuffers) {
            if (spareBuffers.size < maxSpareBuffers) {
                buffer.clear()
                spareBuffers.add(buffer)
            }
        }

        // if the user code adheres to the contract and we don't
        // need the buffer, there should now be no more references
        // to the buffers and GC will free them
    }

    private fun createBuffer(minSize: Int): ByteBuffer {
        // next largest multiple of 1024
        val targetSize = if (minSize % 1024 == 0) minSize else {
            var factor = Math.ceil(minSize.toDouble() / 1024.0).toInt()
            if (factor == 0) factor = 1
            factor * 1024
        }

        return allocator(targetSize)
    }
}