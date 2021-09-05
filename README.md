# ccp
ccp copies files across different file services

## Usage
```
ccp http://example.com /tmp/file
ccp /tmp/file s3://bucket/file
ccp s3://bucket/file gs://bucket/file
ccp s3://bucket/file s3://bucket/file2
```

## Feature Matrix

SCHEME | SRC | DST | COMMENT
-- | -- | -- | --
s3 | x | x | amazon s3
gs | x | x | google cloud storage
http/https | x |   |  
file | x | x | local file
  | x | x | alias for file

## Configuration

gs/s3 are configured through the common environment variables used by the respective sdks
