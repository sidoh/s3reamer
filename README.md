# s3reamer

*(pronounced "ess-three-mer")*

Automatically upload files to S3 as they're created

## Details

s3reamer watches a provided directory and starts uploading newly created files as soon as they're created. It uses [`rb-inotify`](https://github.com/rb-inotify) for filesystem event notifications. As such, it only works on Linux systems.

When it detects a new file, it immediately begins uploading it to S3 using the multipart upload API. It buffers each part (default and minimum 5MB) in memory.

## Installing

s3reamer is available on [Rubygems](https://rubygems.org). You can install it with:

```
$ gem install s3reamer
```

You can also add it to your Gemfile:

```
gem 's3reamer'
```

In order to access the executables, you might have to do something like:

```
rbenv rehash
```

depending on how your Ruby environment is configured.

## Usage

It's probably easiest to use the bundled ruby script:

```
$ s3reamer
Usage: s3reamer DIRECTORY BUCKET_NAME [options]
    -r, --region [AWS_REGION]
        --file-read-timeout [SECONDS]
                                     Number of seconds to wait for a file to grow before timing out (defaults to 10)
        --reader-sleep-interval [SECONDS]
                                     Number of seconds to sleep after an attempted read (defaults to 1)
    -n, --parallelism [N]            Maximum number of concurrent files being processed (defaults to 4)
    -v, --verbose
    -c, --aws-credientials [PATH]    Path to AWS credentials file. Defaults to ~/.aws/credentials
    -p [PROFILE],                    AWS credentials profile. Defaults to "default".
        --aws-credentials-profile
        --s3-prefix [PREFIX]         Prefix to append to all uploaded files. Defaults to empty string.
```        

It expects credentials to be in the standard AWS credentials conf format, and reads from `~/.aws/credentials` by default:

```
$ cat ~/.aws/credentials
[default]
aws_access_key_id = <access_key_id>
aws_secret_access_key = <secret_access_key>
region = us-east-1
```

It's important to note that s3reamer will **NOT** attempt to upload files that already exist (and don't change) when it's started.
