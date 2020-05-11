from functools import lru_cache
from io import BytesIO

import requests
import pandas as pd

from utils.storage import upload_dataframe

url = 'https://ndrdata-corona-datastore.storage.googleapis.com/rki_api/current_cases_regions.csv'


@lru_cache
def get_data():
    # Download website
    response = requests.get(url)
    assert bool(response), 'Laden der RKI-NDR-Daten fehlgeschlagen'

    # Parse into data frame
    df = pd.read_csv(BytesIO(response.content))

    return df, response


def clear_data():
    df, response = get_data()

    # Clean up data here

    grouped = df[df['IdLandkreis'] != 11000].groupby('Bundesland').sum()

    def sum_of(column):
        return grouped[column]['Berlin']

    population = 3644826

    berlin = {
        'IdLandkreis': 11000,
        'Bundesland': 'Berlin',
        'IdBundesland': 11,
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

    return df


def write_data_rki_ndr_districts():
    df = clear_data()
    filename = 'rki_ndr_districts.csv'

    upload_dataframe(df, filename)


# If the file is executed directly, print cleaned data
if __name__ == '__main__':
    df = clear_data()
    print(df)
    # print(df.to_csv(index=False))
