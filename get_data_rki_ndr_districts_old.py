from functools import lru_cache
from io import BytesIO
from datetime import datetime

import requests
import pandas as pd
from pytz import timezone

from utils.storage import upload_dataframe, make_df_compare_fn
from data.inhabitants_de import inhabitants_de

url = 'https://ndrdata-corona-datastore.storage.googleapis.com/rki_api/current_cases_regions.csv'


@lru_cache
def get_data():
    # Download website
    response = requests.get(url)
    assert bool(response), 'Laden der alten(!) RKI-NDR-Daten fehlgeschlagen'

    # Parse into data frame
    df = pd.read_csv(BytesIO(response.content), dtype={'IdLandkreis': 'str', 'IdBundesland': 'str'})

    return df, response


def adjust_for_2019(df):
    inhabitants_data = pd.DataFrame.from_records(inhabitants_de)
    inhabitants_data = inhabitants_data[['rs', 'ewz_2019']]
    inhabitants_data = inhabitants_data.rename(
        columns={'rs': 'IdLandkreis', 'ewz_2019': 'population_new'})

    inhabitants_data = inhabitants_data.set_index('IdLandkreis')
    print(inhabitants_data)

    df = df.join(inhabitants_data, on='IdLandkreis', how='inner')
    df['population'] = df['population_new']

    df['Inzidenz'] = df['Faelle'] / df['population'] * 100_000
    df['Todesrate'] = df['Todesfaelle'] / df['population'] * 100_000
    df['InzidenzLetzte7Tage'] = df['NeueFaelleLetzte7Tage'] / df['population'] * 100_000

    df = df.drop(columns=['population_new'])

    return df


@lru_cache
def clear_data():
    df, response = get_data()

    # Clean up data here

    df = df[df['Landkreis'] != 'LK Göttingen (alt)']

    grouped = df[df['IdLandkreis'] != '11000'].groupby('Bundesland').sum()

    def sum_of(column):
        return grouped[column]['Berlin']

    population = 3644826

    berlin = {
        'IdLandkreis': '11000',
        'Bundesland': 'Berlin',
        'IdBundesland': '11',
        'Landkreis': 'Berlin',
        'Faelle': sum_of('Faelle'),
        'FaelleDelta': sum_of('FaelleDelta'),
        'Todesfaelle': sum_of('Todesfaelle'),
        'TodesfaelleDelta': sum_of('TodesfaelleDelta'),
        'Genesen': sum_of('Genesen'),
        'GenesenDelta': sum_of('GenesenDelta'),
        'population': population,
        'Inzidenz': sum_of('Faelle') / population * 100_000,
        'Todesrate': sum_of('Todesfaelle') / population * 100_000,
        'NeueFaelleLetzte7Tage': sum_of('NeueFaelleLetzte7Tage'),
        'InzidenzLetzte7Tage': sum_of('NeueFaelleLetzte7Tage') / population * 100_000,
    }

    # Drop old Berlin rows and add new one
    df = df[df['Bundesland'] != 'Berlin']
    df = df.append(berlin, ignore_index=True).sort_values(by='IdLandkreis')

    # Adjust population statistics from 2018 to 2019
    df = adjust_for_2019(df)

    # Fix types
    df['IdLandkreis'] = df['IdLandkreis'].astype('int')
    df['IdBundesland'] = df['IdBundesland'].astype('int')
    df['Faelle'] = df['Faelle'].astype('int')
    df['FaelleDelta'] = df['FaelleDelta'].astype('int')
    df['Todesfaelle'] = df['Todesfaelle'].astype('int')
    df['TodesfaelleDelta'] = df['TodesfaelleDelta'].astype('int')
    df['Genesen'] = df['Genesen'].astype('int')
    df['GenesenDelta'] = df['GenesenDelta'].astype('int')
    df['population'] = df['population'].astype('int')
    df['NeueFaelleLetzte7Tage'] = df['NeueFaelleLetzte7Tage'].astype('int')

    df = df.rename(columns={"IdLandkreis": "ID"})

    # Add update date
    df['Stand'] = datetime.now(
        timezone('Europe/Berlin')).strftime('%d.%m.%y 0:00 Uhr')

    return df


def write_data_rki_ndr_districts_old():
    df = clear_data()
    filename = 'rki_ndr_districts_old.csv'

    compare = make_df_compare_fn(ignore_columns=['Stand'])
    upload_dataframe(df, filename, compare=compare)


# If the file is executed directly, print cleaned data
if __name__ == '__main__':
    df = clear_data()
    # print(df)
    df.to_csv('rki_de_2019.csv', index=False, encoding='utf-8', line_terminator='\n')
    # print(df.to_csv(index=False))
