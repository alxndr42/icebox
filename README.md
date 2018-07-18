icebox
======

Encrypting command-line client for [Amazon Glacier][].

This command-line client lets you store files and directories in Amazon
Glacier. All data is encrypted using GnuPG before being uploaded and no
original filenames will be visible remotely.

Requirements
------------

* AWS credentials with write access to Glacier
* GnuPG public/private keypair
* Python 3.5 or higher

Setup
-----

**AWS credentials**

Configure AWS credentials as described in the [Boto documentation][].
The credentials should have write access to the Glacier vaults you plan to use.
See the [example IAM policy](docs/iam-policy.example.json) for recommended
permissions.

**GnuPG keypair**

Create a keypair for icebox and make a note of the ID. Retrieval operations
can take a long time, so you should make sure the keypair stays accessible,
i.e. no password prompts blocking the operation.

**Install icebox**

Install icebox using pip (or [pipsi][]):

    pip3 install icebox

Usage
-----

**Create a new box**

Create the box *mybox* for a Glacier vault called *myvault* like this:

    icebox init mybox 0xMYKEYID glacier myvault

If your AWS credentials are not in the default profile, use the `--profile`
option:

    icebox init mybox 0xMYKEYID glacier myvault --profile icebox

**Store data in a box**

To store a file or directory, simply specify its location:

    icebox put mybox cat-pictures/grumpy.jpg

**Retrieve data from a box**

There are no directories in boxes, so you just specify the original name of the
source and a destination:

    icebox get mybox grumpy.jpg -d ~/Desktop

Standard retrievals can take a long time. To perform an [Expedited][pricing]
retrieval, use the `Tier` option:

    icebox get mybox grumpy.jpg -d ~/Desktop -o Tier=Expedited

Retrieval operations are tracked by icebox, so you can interrupt a pending
retrieval and request the same source again later.

**Delete data from a box**

To delete a stored file or directory, use its original name:

    icebox delete mybox grumpy.jpg

**List data in a box**

To list the contents of a box:

    icebox list mybox

**Refresh data in a box**

To update local box information with the current backend inventory:

    icebox refresh mybox

Refresh operations are tracked by icebox, so you can interrupt a refresh and
continue it later.

  [Amazon Glacier]: https://aws.amazon.com/glacier/
  [Boto documentation]: https://boto3.readthedocs.io/en/latest/guide/quickstart.html#configuration
  [pipsi]: https://github.com/mitsuhiko/pipsi
  [pricing]: https://aws.amazon.com/glacier/pricing/
