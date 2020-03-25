def get_data():
    pass


def clear_data():
    pass


def write_data_rki():
    filename = f's3://{os.environ["BUCKET_NAME"]}/corona_rki.csv'
    df = clear_data()
    write = df.to_csv(index=False)
    fs = s3fs.S3FileSystem()
    with fs.open(filename, 'w') as f:
        f.write(write)
    fs.setxattr(filename,
                copy_kwargs={"ContentType": "text/plain; charset=utf-8"})
    fs.chmod(filename, 'public-read')
