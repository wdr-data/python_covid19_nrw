import re
import os
from datetime import datetime, time

import requests
import json
import pandas as pd
from bs4 import BeautifulSoup as bs
import dateparser
import pytz

from utils.slackbot import send_slack_message
from utils.storage import upload_dataframe

url = 'https://api.github.com/repos/CSSEGISandData/COVID-19/contents/csse_covid_19_data/csse_covid_19_daily_reports'


def get_data():
    # get latest file from repo
    r = requests.get(url)
    json_data = json.loads(r.text)
    df_files = pd.DataFrame(json_data)
    df_files = df_files[df_files.name.str.contains('.csv')]
    df_files = df_files.sort_values('name', ascending=False)
    file = df_files.name.iloc[0]

    # Get download path
    base = 'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_daily_reports/'
    filename = base+file

    print(filename)

    # Get data
    df = pd.read_csv(filename)

    # Clean up data here
    df = df[['Province_State', 'Country_Region', 'Last_Update', 'Lat', 'Long_', 'Confirmed', 'Deaths', 'Recovered', 'Active']]
    df = df.groupby('Country_Region').sum()
    df = df.reset_index()

    return df

def write_data_jh_global():
    df = get_data()
    filename = 'covid19_confirmed_global.csv'

    upload_dataframe(df, filename)


# If the file is executed directly, print cleaned data
if __name__ == '__main__':
    pass
    df = get_data()
    # print(df)
    print(df.to_csv(index=False))
