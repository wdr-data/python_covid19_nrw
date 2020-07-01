import os
from io import BytesIO
from datetime import datetime

import pytz
import pandas as pd

from botocore.exceptions import ClientError
from boto3 import client
import sentry_sdk

try:
    s3 = client('s3')
    bucket = os.environ["BUCKET_NAME"]
except:
    print('Warning: s3 client not created')


def simple_compare(old, new):
    return old == new


def make_df_compare_fn(*, ignore_columns=None):

    def is_equal(old, new):
        old = pd.read_csv(BytesIO(old))
        new = pd.read_csv(BytesIO(new))

        if ignore_columns is not None:
            old = old.drop(columns=ignore_columns, errors='ignore')
            new = new.drop(columns=ignore_columns, errors='ignore')

        return old.equals(new)

    return is_equal


def download_file(filename):
    bio = BytesIO()
    s3.download_fileobj(bucket, filename, bio)
    bio.seek(0)
    return bio


def upload_dataframe(df, filename, *, change_notification=None, compare=None):

    if compare is None:
        compare = simple_compare

    # Convert to csv and encode to get bytes
    write = df.to_csv(index=False).encode('utf-8')

    # Read old file-like object to check for differences
    bio_old = BytesIO()
    compare_failed = False

    try:
        s3.download_fileobj(bucket, filename, bio_old)
    except ClientError:
        compare_failed = True

    bio_old.seek(0)

    if compare_failed or not compare(bio_old.read(), write):
        # Notify
        if change_notification and not compare_failed:
            sentry_sdk.capture_message(change_notification)

        # Create new file-like object for upload
        bio_new = BytesIO(write)

        # Upload file with ACL and content type
        s3.upload_fileobj(
            bio_new,
            bucket,
            filename,
            ExtraArgs={
                'ACL': 'public-read',
                'ContentType': 'text/plain; charset=utf-8',
            },
        )

        # Upload file again into timestamped folder
        bio_new = BytesIO(write)
        now = datetime.now(tz=pytz.timezone('Europe/Berlin'))
        timestamp = now.date().isoformat()
        s3.upload_fileobj(
            bio_new,
            bucket,
            f'{timestamp}/{filename}',
            ExtraArgs={
                'ACL': 'public-read',
                'ContentType': 'text/plain; charset=utf-8',
            },
        )
