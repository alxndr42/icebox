# icebox - Encrypting Cold Storage Client

icebox is a command-line client for storing files and directories in Amazon S3.
All data is encrypted using GnuPG before being uploaded and no original
filenames will be visible remotely.

## Installation

Requirements:

- AWS credentials with write access to S3
- GnuPG public/private keypair
- Python 3.8

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

### GnuPG keypair

Create a keypair for icebox and make a note of the ID. Retrieval operations
can take a long time, so you should make sure the keypair stays accessible,
i.e. no password prompts blocking the operation.

## Usage

### Create a new box

Create the box *mybox* for an S3 bucket called *mybucket*:

```
$ icebox init mybox 0xMYKEYID s3 mybucket
```

Check out the available options:

```
$ icebox init mybox 0xMYKEYID s3 --help
```

**Please note:** encrypted metadata (1-2 KB per `put` operation) is stored
using the `Standard` storage class.

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

Standard retrievals can take a long time. To perform an [Expedited][]
retrieval, use the `Tier` option:

```
$ icebox get mybox grumpy.jpg -d ~/Desktop -o Tier=Expedited
```

Retrieval operations are tracked by icebox, so you can interrupt a pending
retrieval and request the same source again later.

[expedited]: https://aws.amazon.com/s3/pricing/

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
