# AWS S3 IO Additions

This library contains additions to the AWS S3 SDK that help with I/O
tasks.

## Setup

Add the dependency `com.hubject:aws-s3-io` from Maven central to your
project:

    <dependecy>
        <groupId>com.hubject</groupId>
        <artifactId>aws-s3-io</groupId>
        <version>${hubject.aws-s3-io.version}</version>
    </dependency>
    
## Development

This project uses maven for dependency management and the build
process. The typical commands work:

    # compile and run unit tests
    mvn clean test 
    
    # install into local maven repository
    mvn clean install
    
## Usage

The manuals and examples for individual components are on separate
pages:

* [`S3OutputStream`](docs/outputstream.md) - Write to S3 like to regular files using streams.