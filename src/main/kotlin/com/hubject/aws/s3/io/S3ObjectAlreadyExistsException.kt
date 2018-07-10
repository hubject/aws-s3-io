package com.hubject.aws.s3.io

import java.io.IOException

class S3ObjectAlreadyExistsException(val bucketName: String, val key: String) : IOException("The S3 object s3://$bucketName/$key already exists")