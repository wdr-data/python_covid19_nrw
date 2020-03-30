import re
from datetime import datetime, time

import requests
import pandas as pd
from bs4 import BeautifulSoup as bs
import dateparser
import pytz
import sentry_sdk

from utils.storage import upload_dataframe

url = 'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_confirmed_global.csv'


def get_data():
    # Download website
    df = pd.read_csv(url, parse_dates=True)
    df.columns = pd.to_datetime(df.columns, errors='ignore')

    # Clean up data here
    df = df.groupby('Country/Region').sum()

    new_names = []
    for column_name in df.columns:
        r = re.compile(r'(\d*)(\/)(\d*)(\/)(\d*)')
        column_name = r.sub(r'\3.\1.\5', column_name)
        new_names.append(column_name)

    df.columns = new_names

    df = df.reset_index()

    return df


def write_data_jh_ts_global():
    df = get_data()
    filename = 'time_series_covid19_confirmed_global.csv'
    upload_dataframe(df, filename)
    return df


def write_data_jh_ts_filtered():
    df = get_data()
    countries = ['Germany', 'Italy', 'Spain', 'China', 'US']
    df = df[df['Country/Region'].isin(countries)]
    df = df.drop(columns=["Lat", "Long"])
    filename = 'time_series_covid19_confirmed_filtered_countries.csv'
    upload_dataframe(df, filename)
    return df


# If the file is executed directly, print cleaned data
if __name__ == '__main__':
    df = get_data()
    # print(df)
    print(df.to_csv(index=True))
