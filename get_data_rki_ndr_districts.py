from functools import lru_cache
from io import BytesIO
from datetime import datetime

import requests
import pandas as pd
from pytz import timezone

from utils.storage import upload_dataframe, make_df_compare_fn
from data.inhabitants_de import inhabitants_de

url = 'https://storage.googleapis.com/ndrdata-csv-cors/csv/current_cases_regions.csv'


@lru_cache
def get_data():
    # Download website
    response = requests.get(url)
    assert bool(response), 'Laden der RKI-NDR-Daten fehlgeschlagen'

    # Parse into data frame
    df = pd.read_csv(BytesIO(response.content), dtype={'IdLandkreis': 'str', 'IdBundesland': 'str'})

    return df, response


@lru_cache
def clear_data():
    df, response = get_data()

    columns = [
        "IdLandkreis",
        "Bundesland",
        "IdBundesland",
        "Landkreis",
        "Faelle",
        "FaelleDelta",
        "Todesfaelle",
        "TodesfaelleDelta",
        "Genesen",
        "GenesenDelta",
        "population",
        "Inzidenz",
        "Todesrate",
        "NeueFaelleLetzte7Tage",
        "InzidenzLetzte7Tage",
    ]
    df = df[columns]

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


def write_data_rki_ndr_districts():
    df = clear_data()
    filename = 'rki_ndr_districts.csv'

    compare = make_df_compare_fn(ignore_columns=['Stand'])
    upload_dataframe(df, filename, compare=compare)


# If the file is executed directly, print cleaned data
if __name__ == '__main__':
    df = clear_data()
    # print(df)
    df.to_csv('rki_ndr_districts.csv', index=False, encoding='utf-8', line_terminator='\n')
    # print(df.to_csv(index=False))
