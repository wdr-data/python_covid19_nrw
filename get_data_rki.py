# Script by Claus Hesseling, NDR https://github.com/chesselingfm/corona_rki/blob/master/rki_regional-ard.ipynb

import os
import pandas as pd
import json
import requests
from io import StringIO
# package for flattening json in pandas df
import re
import numpy as np
import datetime
import time
import s3fs

url = 'https://services7.arcgis.com/mOBPykOjAyBO2ZKk/arcgis/rest/services/RKI_Landkreisdaten/FeatureServer/0/query?f=json&where=1%3D1&returnGeometry=false&spatialRel=esriSpatialRelIntersects&outFields=*&orderByFields=OBJECTID&resultOffset=0&resultRecordCount=1000&cacheHint=true'


def get_data(url):

    r = requests.get(url)
    json_data = json.loads(r.text)

    df = pd.DataFrame(json_data['features'])
    df2 = pd.concat([df, df['attributes'].str.split(',', expand=True)], axis=1)

    attribs = pd.json_normalize(data=df2['attributes'])
    attribs['AGS'] = attribs['AGS'].str.zfill(5)
    url_ts = 'https://npgeo-de.maps.arcgis.com/sharing/rest/content/items/bca904a683844e7784141559b540dbc2?f=json'

    r_ts = requests.get(url_ts)
    json_data_ts = json.loads(r_ts.text)
    timestamp_s = int(json_data_ts['modified']) / 1000
    ts = datetime.datetime.fromtimestamp(
        timestamp_s).strftime('%Y-%m-%d %H:%M:%S')

    attribs['abruf_timestamp'] = ts
    return attribs


def write_data_rki():
    filename = f's3://{os.environ["BUCKET_NAME"]}/corona_rki.csv'
    df = get_data(url)
    print(df)
    write = df.to_csv(index=False)
    fs = s3fs.S3FileSystem()
    with fs.open(filename, 'w') as f:
        f.write(write)
    fs.setxattr(filename,
                copy_kwargs={"ContentType": "text/plain; charset=utf-8"})
    fs.chmod(filename, 'public-read')
