import re
from datetime import datetime, time, timedelta
from functools import lru_cache

import requests
import pandas as pd
from bs4 import BeautifulSoup as bs
import dateparser
import pytz
import sentry_sdk

from data.inhabitants import inhabitants
from data.studios import studios, link_for_district
from utils.storage import upload_dataframe, download_file

url = 'https://www.mags.nrw/coronavirus-fallzahlen-nrw'


class HTMLTableParser:

    def parse_response(self, response):
        soup = bs(response.text, 'lxml')
        return [(table, self.parse_html_table(table))
                for table in soup.find_all('table')]

    def parse_html_table(self, table):
        n_columns = 0
        n_rows = 0
        column_names = []

        # Find number of rows and columns
        # we also find the column titles if we can
        for row in table.find_all('tr'):

            # Determine the number of rows in the table
            td_tags = row.find_all('td')
            if len(td_tags) > 0:
                n_rows += 1
                if n_columns == 0:
                    # Set the number of columns for our table
                    n_columns = len(td_tags)

            # Handle column names if we find them
            th_tags = row.find_all('th')
            if len(th_tags) > 0 and len(column_names) == 0:
                for th in th_tags:
                    column_names.append(th.get_text())

        # Safeguard on Column Titles
        if len(column_names) > 0 and len(column_names) != n_columns:
            raise Exception("Column titles do not match the number of columns")

        columns = column_names if len(
            column_names) > 0 else range(0, n_columns)
        df = pd.DataFrame(columns=columns,
                          index=range(0, n_rows))
        row_marker = 0
        for row in table.find_all('tr'):
            column_marker = 0
            columns = row.find_all('td')
            for column in columns:
                df.iat[row_marker, column_marker] = column.get_text()
                column_marker += 1
            if len(columns) > 0:
                row_marker += 1

        return df


def parse_date(response):
    soup = bs(response.text, 'lxml')

    textBlock = soup(text=re.compile(r'Aktueller Stand: (.*)'))

    date_text = ''

    for block in textBlock:
        re_search = re.search('Aktueller Stand: (.*)(\.)', block)
        if re_search:
            date_text = re_search.group(1)
            date_text = date_text.replace('  ', ' ')
            date = dateparser.parse(date_text, languages=['de'])

    if not date_text:
        sentry_sdk.capture_message('Datumsformat hat sich geändert')
        meta_date = soup.find('meta', attrs={'name': 'dc.date.modified'})
        date_text = meta_date.get('content')
        date = dateparser.parse(date_text)
        date_text = date.strftime('%d.%m.%Y')

    assert date is not None, "Date could not be parsed"

    return date, date_text

@lru_cache
def get_data():
    response = requests.get(url)
    assert bool(response), 'Laden der Mags-Seite fehlgeschlagen'
    hp = HTMLTableParser()
    # Grabbing the table from the tuple
    table = hp.parse_response(response)[0][1]
    df = pd.DataFrame(table)
    return df, response

@lru_cache
def clear_data():
    df, response = get_data()

    site_date, date_text = parse_date(response)

    expected_columns = [
        'Landkreis/ kreisfreie Stadt',
        'Bestätigte Fälle (IfSG)',
        'Todesfälle (IfSG)',
        'Genesene*',
    ]

    for column in expected_columns:
        assert column in df.columns, 'Spaltenkopf in Mags-Daten geändert'

    if len(df.columns) > len(expected_columns):
        unknown_columns = ', '.join(set(df.columns) - set(expected_columns))
        sentry_sdk.capture_message(f'Neue Spalte(n) in Mags-Daten: {unknown_columns}')

    df["Infizierte (RKI)"] = df["Bestätigte Fälle (IfSG)"]
    df = df.rename(columns={"Bestätigte Fälle (IfSG)": "Infizierte"})

    df["Todesfälle (RKI)"] = df["Todesfälle (IfSG)"]
    df = df.rename(columns={"Todesfälle (IfSG)": "Todesfälle"})

    # wtf
    df = df.replace('Gütersloh (Kreis)**', 'Gütersloh (Kreis)')

    df = df.replace('Aachen & Städteregion Aachen', 'Städteregion Aachen')
    df = df.replace('Mülheim / Ruhr', 'Mülheim an der Ruhr')
    df = df.replace(regex=r' +\(Kreis\)', value='')
    df = df.replace(regex=r'\.', value='')
    df['Landkreis/ kreisfreie Stadt'] = df['Landkreis/ kreisfreie Stadt'].str.strip()

    df.Infizierte = df.Infizierte.replace(u'\xa0', u' ')
    df.Infizierte = df.Infizierte.replace(u' ', 0)
    df.Infizierte = df.Infizierte.astype('int')

    df.Todesfälle = df.Todesfälle.replace(u'\xa0', u' ')
    df.Todesfälle = df.Todesfälle.replace(u' ', 0)
    df.Todesfälle = df.Todesfälle.replace(u'-', 0)
    df.Todesfälle = df.Todesfälle.astype('int')

    df['Genesene*'] = df['Genesene*'].replace(u'\xa0', u' ')
    df['Genesene*'] = df['Genesene*'].replace(u' ', 0)
    df['Genesene*'] = df['Genesene*'].replace(u'-', 0)
    df['Genesene*'] = df['Genesene*'].astype('int')

    df_inhabitants = pd.DataFrame(inhabitants.items(), columns=[
                                  'Landkreis/ kreisfreie Stadt', 'Einwohner'])
    for area in df_inhabitants['Landkreis/ kreisfreie Stadt']:
        assert area in df['Landkreis/ kreisfreie Stadt'].values, f'{area} in Mags-Daten nicht gefunden'

    df = pd.merge(df, df_inhabitants)
    df.Einwohner = df.Einwohner.astype('int')
    df['Infizierte pro 100.000'] = (
        df.Infizierte * 100000 / df.Einwohner).round(1)

    df['Aktuell Infizierte'] = (
        df['Infizierte'] - df['Genesene*'] - df['Todesfälle']).round(1)

    # 7 days delta
    timestamp = (site_date - timedelta(days=7)).date().isoformat()

    bio = download_file(f'{timestamp}/corona_mags_nrw.csv')
    bio_all = download_file(f'{timestamp}/corona_mags_nrw_gesamt.csv')
    infections_7_days_ago = pd.read_csv(bio).append(pd.read_csv(bio_all), ignore_index=True)
    infections_7_days_ago = infections_7_days_ago[['Landkreis/ kreisfreie Stadt', 'Infizierte']]
    infections_7_days_ago = infections_7_days_ago.rename(columns={'Infizierte': timestamp})

    df = df.merge(infections_7_days_ago, on='Landkreis/ kreisfreie Stadt', validate='one_to_one')
    df['Neuinfektionen vergangene 7 Tage'] = df.Infizierte - df[timestamp]
    df = df.drop(columns=[timestamp])
    df['7-Tage-Inzidenz'] = (df['Neuinfektionen vergangene 7 Tage'] * 100000 / df.Einwohner).round(1)

    # 1 day delta
    timestamp = (site_date - timedelta(days=1)).date().isoformat()

    bio = download_file(f'{timestamp}/corona_mags_nrw.csv')
    bio_all = download_file(f'{timestamp}/corona_mags_nrw_gesamt.csv')
    infections_1_day_ago = pd.read_csv(bio).append(pd.read_csv(bio_all), ignore_index=True)
    infections_1_day_ago = infections_1_day_ago[['Landkreis/ kreisfreie Stadt', 'Infizierte']]
    infections_1_day_ago = infections_1_day_ago.rename(columns={'Infizierte': timestamp})

    df = df.merge(infections_1_day_ago, on='Landkreis/ kreisfreie Stadt', validate='one_to_one')
    df['Neuinfektionen zum Vortag'] = df.Infizierte - df[timestamp]
    df = df.drop(columns=[timestamp])

    df['Studio-Link'] = df['Landkreis/ kreisfreie Stadt'].map(link_for_district)

    df['Stand'] = date_text

    return df

def clear_data_nrw_gesamt():
    df1 = clear_data()
    df1 = df1[df1['Landkreis/ kreisfreie Stadt'] == 'Gesamt']
    return df1


def write_data_nrw():
    filename1 = 'corona_mags_nrw_gesamt.csv'
    df1 = clear_data_nrw_gesamt()
    upload_dataframe(df1, filename1)

    filename = 'corona_mags_nrw.csv'
    df = clear_data()
    # Remove 'Gesamt' from DF
    df = df[df['Landkreis/ kreisfreie Stadt'] != 'Gesamt']

    upload_dataframe(
        df, filename, change_notification=f'Mags-Daten aktualisiert')

    for studio, areas in studios.items():
        df_studio = df[df['Landkreis/ kreisfreie Stadt'].isin(areas)]
        filename = f'corona_mags_nrw_{studio}.csv'
        upload_dataframe(df_studio, filename)


if __name__ == '__main__':
    df = clear_data()
    # df = df[df['Landkreis/ kreisfreie Stadt'] != 'Gesamt']

    # df1 = clear_data()
    # df1 = df1[df1['Landkreis/ kreisfreie Stadt'] == 'Gesamt']

    print(df)
    # print(df.to_csv(index=False))
