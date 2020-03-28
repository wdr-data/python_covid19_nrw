import os
from io import BytesIO

from boto3 import client

def upload_dataframe(df, filename):
    s3 = client('s3')
    bucket = os.environ["BUCKET_NAME"]

    # Convert to csv and encode to get bytes
    write = df.to_csv(index=False).encode('utf-8')

    # Create file-like object for upload
    bio = BytesIO(write)

    # Upload file with ACL and content type
    return s3.upload_fileobj(
        bio,
        bucket,
        filename,
        ExtraArgs={
            'ACL': 'public-read',
            'ContentType': 'text/plain; charset=utf-8',
        },
    )
