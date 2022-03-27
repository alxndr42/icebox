# icebox - Encrypting Cold Storage Client

icebox is a command-line client for secure online file storage. All metadata is
protected from unauthorized access, by using [icepack][] for archive handling.

Supported storage backends:

- Amazon S3 (`Glacier Deep Archive` storage class)
- WebDAV
- Local folder (for testing or third-party tools)

[icepack]: https://github.com/alxndr42/icepack

## Installation

Requirements:

- Python 3.8
- age 1.0
- OpenSSH 8.0

Install with `pip` or [pipx][]:

```
$ pip install icebox
```

[pipx]: https://pypa.github.io/pipx/

### AWS credentials

Configure AWS credentials as described in the [Boto][] documentation. The
credentials should have write access to the S3 buckets you plan to use. See the
example [IAM policy][] for recommended permissions.

[boto]: https://boto3.amazonaws.com/v1/documentation/api/latest/guide/quickstart.html#configuration
[iam policy]: docs/iam-policy.example.json

## Usage

Help for each command can be displayed by using the `--help` option.

### Create a new box

Create the box *mybox* for an S3 bucket called *mybucket*:

```
$ icebox init mybox s3 mybucket
```

**Please note:** encrypted metadata (several KB per `put` operation) is stored
using the `Standard` storage class.

Create the box *mybox* for a WebDAV URL:

```
$ icebox init mybox webdav https://example.com/webdav/folder
```

### Store data in a box

To store a file or directory, simply specify its location:

```
$ icebox put mybox cat-pictures/grumpy.jpg
```

### Retrieve data from a box

There are no directories in boxes, so you just specify the original name of the
source and a destination:

```
$ icebox get mybox grumpy.jpg -d ~/Desktop
```

`Bulk` retrievals from S3 can take a long time. To perform a [Standard][]
retrieval, use the `Tier` option:

```
$ icebox get mybox grumpy.jpg -d ~/Desktop -o Tier=Standard
```

Retrieval operations are tracked by icebox, so you can interrupt a pending
retrieval and request the same source again later.

[standard]: https://aws.amazon.com/s3/pricing/

### Delete data from a box

To delete a stored file or directory, use its original name:

```
$ icebox delete mybox grumpy.jpg
```

### List data in a box

To list the contents of a box:

```
$ icebox list mybox
```

### Refresh data in a box

To update local box information from the backend:

```
$ icebox refresh mybox
```

Refresh operations are tracked by icebox, so you can interrupt a refresh and
continue it later.

### Check the version and dependencies

```
$ icebox version --dependencies
icebox 1.0.0
✅ age found. (Version: v1.0.0)
✅ age-keygen found.
✅ ssh found. (Version: OpenSSH_8.2p1)
✅ ssh-keygen found.
```
