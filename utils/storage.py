import os
from io import BytesIO
from datetime import datetime
import pytz

from botocore.exceptions import ClientError
from boto3 import client
import sentry_sdk


def upload_dataframe(df, filename, change_notifcation=None):
    s3 = client('s3')
    bucket = os.environ["BUCKET_NAME"]

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

    if bio_old.read() != write:
        # Notify
        if change_notifcation and not compare_failed:
            sentry_sdk.capture_message(change_notifcation)

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
        bio_new.seek(0)
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
