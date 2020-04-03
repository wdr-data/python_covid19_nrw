import re
from datetime import datetime, time

import requests
import pandas as pd
from bs4 import BeautifulSoup as bs
import dateparser
import pytz
import sentry_sdk

from data.inhabitants import inhabitants
from data.studios import studios
from utils.storage import upload_dataframe

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
        if len(column_names) > 3:
            sentry_sdk.capture_message('Neue Spalte in Mags-Daten')

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

    for block in textBlock:
        re_search = re.search('Aktueller Stand: (.*)(\.)', block)
        if re_search:
            dateText = re_search.group(1)
            dateText = dateText.replace('  ', ' ')
        if not dateText:
            sentry_sdk.capture_message('Datumsformat hat sich geändert')
            meta_date = soup.find('meta', attrs={'name': 'dc.date.modified'})
            dateText = meta_date.get('content')
    return dateText


def get_data():
    response = requests.get(url)
    assert bool(response), 'Laden der Mags-Seite fehlgeschlagen'
    hp = HTMLTableParser()
    # Grabbing the table from the tuple
    table = hp.parse_response(response)[0][1]
    df = pd.DataFrame(table)
    return df, response


def clear_data():
    df, response = get_data()

    for column in ['Landkreis/ kreisfreie Stadt', 'Bestätigte Fälle', 'Todesfälle']:
        assert column in df.columns, 'Spaltenkopf in Mags-Daten geändert'

    df = df.rename(columns={"Bestätigte Fälle": "Infizierte"})

    df = df.replace('Aachen & Städteregion Aachen', 'Städteregion Aachen')
    df = df.replace('Mülheim / Ruhr', 'Mülheim an der Ruhr')
    df = df.replace(regex=r' +\(Kreis\)', value='')
    df = df.replace(regex=r'\.', value='')
    df['Landkreis/ kreisfreie Stadt'] = df['Landkreis/ kreisfreie Stadt'].str.strip()
    df = df[df['Landkreis/ kreisfreie Stadt'] != 'Gesamt']

    df.Infizierte = df.Infizierte.replace(u'\xa0', u' ')
    df.Infizierte = df.Infizierte.replace(u' ', 0)
    df.Infizierte = df.Infizierte.astype('int')

    df.Todesfälle = df.Todesfälle.replace(u'\xa0', u' ')
    df.Todesfälle = df.Todesfälle.replace(u' ', 0)
    df.Todesfälle = df.Todesfälle.astype('int')

    df_inhabitants = pd.DataFrame(inhabitants.items(), columns=[
                                  'Landkreis/ kreisfreie Stadt', 'Einwohner'])
    for area in df_inhabitants['Landkreis/ kreisfreie Stadt']:
        assert area in df['Landkreis/ kreisfreie Stadt'].values, f'{area} in Mags-Daten nicht gefunden'

    df = pd.merge(df, df_inhabitants)
    df.Einwohner = df.Einwohner.astype('int')
    df['Infizierte pro 100.000'] = (
        df.Infizierte * 100000 / df.Einwohner).round(1)

    df['Stand'] = parse_date(response)

    return df


def write_data_nrw():
    filename = 'corona_mags_nrw.csv'
    df = clear_data()

    upload_dataframe(
        df, filename, change_notifcation=f'Mags-Daten aktualisiert')

    for studio, areas in studios.items():
        df_studio = df[df['Landkreis/ kreisfreie Stadt'].isin(areas)]
        filename = f'corona_mags_nrw_{studio}.csv'
        upload_dataframe(df_studio, filename)


if __name__ == '__main__':
    df = clear_data()
    # print(df)
    print(df.to_csv(index=False))
