# `S3OutputStream`

## When to use

* You need to upload a file that is too large to hold in memory
  **and** you don't know the exact size when you need to start
  uploading.
* You want to upload data and the code that has the data wants
  an `java.io.OutputStream` to write data to.

---

The S3 SDK needs to know content length before uploading. If the
file size is not provided up front, the SDK will cache the data in
memory before uploading. This is a problem when you are dealing
with several gigabytes of data.  
The SDK offers multipart-uploads as a solution. But using them
efficiently is non-trivial. This class does that for you.

## How to use

**Prerequisites**

 * An instance of the S3 Client `com.amazonaws.services.s3.AmazonS3`
 * The name of the bucket you want to upload to
 * The key of the file you want to upload to
 * `S3OutputStream` is **not** thread safe. As long as only one thread
   is using it at the same time it's probably okay, but no guarantees 
   are made.
 
Just instantiate the class and use it like any other `OutputStream`.
It **must** be `close()`d. If it is not closed, your file will not
be visible in S3 and you will still pay for the data already uploaded.
`S3OutputStream` implements `AutoCloseable` to aid with that;
try-with-resources works.

### Examples

```kotlin
// Kotlin usage

openSomeLargeSource().use { inputStream ->
    S3OutputStream(awsS3, bucketName, filePath).use { outStream ->
        inputStream.copyTo(outStream)
    }
}
```

```java
// Java usage
try (InputStream inputStream = openSomeLargeSource()) {
    try (OutputStream outStream = new S3OutputStream(awsS3, bucketName, filePath)) {
        byte[] buffer = new byte[8192];
        int n;
        while ((n = inputStream.read(buffer)) != -1) {
            outStream.write(buffer, 0, n);
        }
    }
}
```

### How it works

The `S3OutputStream` creates two `ByteBuffers`. Calls to the `write()`
methods copy the data to one of the buffers. As soon as that is
filled, the buffers are switched over and the other buffer is being
uploaded to S3 in a background thread.  
At first, `write()` calls will go to the other buffer. If that is
also filled before the upload of the first buffer is done,
calls to the `write()` methods will block until the upload is
complete. The buffers then switch again, and it goes on like that
as long as data is being written to the stream.

The `close()` method will block until all parts are uploaded. Once
uploaded, it will do a final call to the S3 api to finish the upload.

### Available Options

#### ByteBuffer size

The amount of memory that is being allocated for caching can be
adjusted with the `maxLocalCache` constructor parameter to `S3OutputStream`:

```kotlin
val outStream = S3OutputStream(
    awsS3 = awsS3,
    targetBucket = "my-bucket",
    targetS3Key = "my-dir/my-file.bin",
    maxLocalCache = 1024 * 1024 * 100 /* 100 MiB */
)
```

#### ByteBuffer sharing

`ByteBuffer`s, especially off-heap ones, are expensive to allocate
and deallocate. To avoid doing this twice for every instance of
`S3OutputStream`, a `ByteBufferPool` is used for reuse of byte
buffers. It works very similar to database connection pools.

By default, `S3OutputStream` uses `ByteBufferPool.DEFAULT`, so all
instances of the class use the same pool. If you don't use them in
parallel, only two ByteBuffers will be created.

You can change this behaviour by implementing the interface
`ByteBufferPool` yourself or re-using `SimpleByteBufferPool` with
adapted parameters (see source code).

```kotlin
val myByteBufferPool = SimpleBytBufferPool(
    allocateDirect = false, // use on-heap memory
    maxSpareBuffers = 10 // keep up-to 10 buffers for on demand retrieval
)

S3OutputStream(
    awsS3 = awsS3,
    targetBucket = "my-bucket",
    targetS3Key = "my-dir/my-file.bin",
    byteBufferPool = myByteBufferPool
).use { outStream ->
    // ...
}
```