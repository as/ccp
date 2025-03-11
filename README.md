# ccp
ccp transparently copies files across different file servers, such as AWS (s3), google cloud storage (gs), http, and local filesystems. It supports download acceleration and streaming output, making it ideal for workflows that require pipelining with accelerated downloads. The latter feature is unavailable with most download accelerators, as they download the entire file as partial data and stitch it together after its download. This is disk-inefficient and makes the file impossible to stream to standard output.

On an AWS s3 instance, ccp downloaded a 1.5 TiB file in less than 10 minutes on a 2 TiB volume.

## Usage

### Copy

```
ccp http://example.com /tmp/file
ccp /tmp/file s3://bucket/file
ccp s3://bucket/file gs://bucket/file
ccp s3://bucket/file s3://bucket/file2
```

### List

```
ccp -ls s3://bucket/
ccp -ls s3://bucket/dir
```


### Delete

# Note: This is currently non-recursive and only forks for os and s3
```
ccp -d test.txt
ccp -d s3://bucket/file s3://bucket/file2 ... s3://bucket/fileN
```

### Test

The `test` flag will cause `ccp` to verify that it can read and write to the locations without copying data. The first example will check read access only, whereas the second also creates an empty `file2`. This is useful when you need to verify bucket permissions in advance before copying large files, however, it will create empty files.

```
ccp -test s3://bucket/file -
ccp -test s3://bucket/file s3://bucket/file2
```

You can also use `ccp -ls` to check access controls, however some schemes will allow accounts to `list` a file and then deny read access to that file.

## Ranges (seek+skip)

Using the `-seek` and `-skip` flag allows copying byte ranges from source files. This is only supported for some protocols.

```
ccp -q -seek 41 -count 7 http://example.com -
Example
```

This works transparently with any download acceleration used by `ccp` at runtime (byte ranges are partitioned).

## Feature Matrix

SCHEME | SRC | DST | DELETE | SEEK+SKIP | COMMENT
-- | -- | -- | --| --| --
s3 | x | x |x|x| amazon s3
gs | x | x ||| google cloud storage
http/https || x || x  |  
ftp/sftp |  |||   |  
ssh |  |||   |  
file | x | x |x|| local file
  | x | x |x|| alias for file

## Configuration

gs/s3 are configured through the common environment variables used by the respective sdks


## UNIX-like Usage Example

A more in-depth example is necessary to see how ccp can integrate with your existing UNIX tools.
```
# Step 1:
# Create four test files in /tmp/ccp

ccp http://example.com /tmp/ccp/file1
ccp http://example.com /tmp/ccp/file2
ccp http://example.com /tmp/ccp/file3
echo hello | ccp - /tmp/ccp/file4

# Step 2:
# Use ccp -ls to list these files
# This will generate a tab seperated list of $size, $file
ccp -ls /tmp/ccp

	1256	/tmp/ccp/file1
	1256	/tmp/ccp/file2
	1256	/tmp/ccp/file3
	6	/tmp/ccp/file4

# Step 3:
# To see what ccp is going to do, run it -dry
# The output can be piped to the shell to execute the copy
# Or you can just remove the -dry flag.
#
ccp -l -dry /tmp/ccp /tmp/ccp.bak/

	ccp "/tmp/ccp/file1" "/tmp/ccp.bak/file1" # 1256
	ccp "/tmp/ccp/file2" "/tmp/ccp.bak/file2" # 1256
	ccp "/tmp/ccp/file3" "/tmp/ccp.bak/file3" # 1256
	ccp "/tmp/ccp/file4" "/tmp/ccp.bak/file4" # 6

# Step 4:
# Earlier you ran a command to consume stdin and write
# that to file4. CCP can, in addition to reading file contents,
# read a newline-seperated list of files from stdin with the "ccp -l" flag

ccp -ls /tmp/ccp | ccp -l -dry - /tmp/ccp.bak/

	ccp "/tmp/ccp/file1" "/tmp/ccp.bak/file1" # 1256
	ccp "/tmp/ccp/file2" "/tmp/ccp.bak/file2" # 1256
	ccp "/tmp/ccp/file3" "/tmp/ccp.bak/file3" # 1256
	ccp "/tmp/ccp/file4" "/tmp/ccp.bak/file4" # 6

# Step 5:
# The above output is identical to "ccp -l -dry /tmp/ccp /tmp/ccp.bak/"
# However, it is more powerful.


# Filter out the undesirable "file4" from the copy
ccp -ls /tmp/ccp | egrep -v file4 | ccp -l -dry - /tmp/ccp.bak/

	ccp "/tmp/ccp/file1" "/tmp/ccp.bak/file1" # 1256
	ccp "/tmp/ccp/file2" "/tmp/ccp.bak/file2" # 1256
	ccp "/tmp/ccp/file3" "/tmp/ccp.bak/file3" # 1256

# Filter files larger than 10 bytes
ccp -ls /tmp/ccp | awk '{ if ($1 <= 10) print }'  | ccp -l -dry - /tmp/ccp.bak/

	ccp "/tmp/ccp/file4" "/tmp/ccp.bak/file4" # 6

# All of these tricks can be used with gs and s3 buckets as well
# to copy files across different cloud providers.
```

## Notes

### Transfer Acceleration

- Since v0.2.4, ccp will download http/https inputs larger than (default 64MiB) by spawning multiple connection (similar to aria2c) and using temporary files. The streaming nature of ccp is preserved and the process is transparent to the user. Disable it with `ccp -slow` to revert to pre-v0.2.4 behavior.

- Additionally, `ccp` will attempt to presign urls and download them over http when possible. If `ccp -secure` is used, it prevents presigned urls from being stripped to http from https.

- Use `ccp -secure -slow` to disable these two optimizations

- The temporary folder used for disk-backed files is $TEMP, or can be overridden on the command line. 

### GS to S3 compatibility mode

- The `gs` protocol supports an `s3` compatibility mode wherein an s3 client can speak to a `gs` bucket using the `s3` protocol. This usage mode is not well-documented, and involves generating aws-compatible hmac keys (aka $AWS_ACCESS_KEY_ID, $AWS_SECRET_ACCESS_KEY). This usage mode is not supported and in my experience does not work reliably. To fix this, use `GOOGLE_APPLICATION_CREDENTIALS` or some other credentials auto-detected by the google SDK.

### Cross-Account Copying

- Currently, it is not possible to copy files across accounts using the same scheme. For example, two `s3` buckets managed by different accounts. This is not possible to resolve without introducing a config file that properly initializes credentials based on the source or destination path.

### Cross-Scheme Directory Structures

- Schemes like `s3` (and possibly `gs`) do not have a concept of directories. Hence, it is possible to create `s3://bucket/file.mp4` and `s3://bucket/file.mp4/file.txt`. The `ccp` program always operates on a directory structure, so `ccp -ls s3://bucket/file.mp4` on such a layout will produce a list of outputs including both files. The best solution is to use a sane directory-based layout to migitage potentially undefined behavior when copying these resources to a local filesystem.

 ### China

- This software is not affiliated with or sponsored by the People's Republic of China
