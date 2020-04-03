import os
from io import BytesIO

from boto3 import client
import sentry_sdk


def upload_dataframe(df, filename, change_notifcation=None):
    s3 = client('s3')
    bucket = os.environ["BUCKET_NAME"]

    # Convert to csv and encode to get bytes
    write = df.to_csv(index=False).encode('utf-8')

    # Read old file-like object to check for differences
    bio_old = BytesIO()
    s3.download_fileobj(bucket, filename, bio_old)

    if bio_old.read() != write:
        # Notify
        if change_notifcation:
            sentry_sdk.capture_message(change_notifcation)

        # Create new file-like object for upload
        bio_new = BytesIO(write)

        # Upload file with ACL and content type
        return s3.upload_fileobj(
            bio_new,
            bucket,
            filename,
            ExtraArgs={
                'ACL': 'public-read',
                'ContentType': 'text/plain; charset=utf-8',
            },
        )
