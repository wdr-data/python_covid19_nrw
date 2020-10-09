from functools import lru_cache
from io import BytesIO
from datetime import datetime

import requests
import pandas as pd
from pytz import timezone

from utils.storage import upload_dataframe, make_df_compare_fn
from data.studios import studios, link_for_district
from get_data_rki_ndr_districts import get_data, adjust_for_2019


@lru_cache
def clear_data():
    df, response = get_data()

    # Filter for the state we care about <3
    df = df[df['Bundesland'] == 'Nordrhein-Westfalen']

    # Adjust population statistics from 2018 to 2019
    df = adjust_for_2019(df)

    # Create a datapoint for all of NRW
    grouped = df.groupby('Bundesland').sum()

    def sum_of(column):
        return grouped[column]['Nordrhein-Westfalen']

    population = sum_of('population')

    gesamt = {
        'IdLandkreis': '05999',
        'Bundesland': 'Nordrhein-Westfalen',
        'IdBundesland': '05',
        'Landkreis': 'Gesamt',
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

    df = df.append(gesamt, ignore_index=True).sort_values(by='IdLandkreis')

    # Drop columns we don't need
    df = df.drop(columns=['IdLandkreis', 'Bundesland', 'IdBundesland'])
    df = df.reset_index(drop=True)

    # Rename districts
    df['Landkreis'] = df['Landkreis'].str.replace('SK ', '')
    df['Landkreis'] = df['Landkreis'].str.replace('LK ', '')
    df['Landkreis'] = df['Landkreis'].replace('StadtRegion Aachen', 'Städteregion Aachen')
    df['Landkreis'] = df['Landkreis'].replace('Mülheim a.d.Ruhr', 'Mülheim an der Ruhr')

    # Fix datatypes
    df['Faelle'] = df['Faelle'].astype('int')
    df['FaelleDelta'] = df['FaelleDelta'].astype('int')
    df['Todesfaelle'] = df['Todesfaelle'].astype('int')
    df['TodesfaelleDelta'] = df['TodesfaelleDelta'].astype('int')
    df['Genesen'] = df['Genesen'].astype('int')
    df['GenesenDelta'] = df['GenesenDelta'].astype('int')
    df['population'] = df['population'].astype('int')
    df['NeueFaelleLetzte7Tage'] = df['NeueFaelleLetzte7Tage'].astype('int')
    df['Inzidenz'] = df['Inzidenz'].round(1)
    df['Todesrate'] = df['Todesrate'].round(1)
    df['InzidenzLetzte7Tage'] = df['InzidenzLetzte7Tage'].round(1)

    # Rename columns to match MAGS data
    df = df.rename(columns={
        'Landkreis': 'Landkreis/ kreisfreie Stadt',
        'Faelle': 'Infizierte',
        'FaelleDelta': 'Neuinfektionen zum Vortag',
        'Todesfaelle': 'Todesfälle',
        'TodesfaelleDelta': 'Neue Todesfälle zum Vortag',
        'Genesen': 'Genesene*',
        'GenesenDelta': 'Neu Genesene zum Vortag*',
        'population': 'Einwohner',
        'Inzidenz': 'Infizierte pro 100.000',
        'Todesrate': 'Todesfälle pro 100.000',
        'NeueFaelleLetzte7Tage': 'Neuinfektionen vergangene 7 Tage',
        'InzidenzLetzte7Tage': '7-Tage-Inzidenz',
    })

    # Add some additional columns
    df['Aktuell Infizierte'] = (
        df['Infizierte'] - df['Genesene*'] - df['Todesfälle']).round(1)
    df['Studio-Link'] = df['Landkreis/ kreisfreie Stadt'].map(link_for_district)
    df['Stand'] = datetime.now(timezone('Europe/Berlin')).strftime('%d.%m.%y 0:00 Uhr')

    return df


def clear_data_nrw_gesamt():
    df = clear_data()
    df = df[df['Landkreis/ kreisfreie Stadt'] == 'Gesamt']
    return df


def write_data_rki_ndr_districts_nrw():
    # Make compare function
    compare = make_df_compare_fn(ignore_columns=['Stand'])

    filename_gesamt = 'rki_ndr_districts_nrw_gesamt.csv'
    df_gesamt = clear_data_nrw_gesamt()
    upload_dataframe(df_gesamt, filename_gesamt, compare=compare)

    filename = 'rki_ndr_districts_nrw.csv'
    df = clear_data()

    # Remove 'Gesamt' from DF
    df = df[df['Landkreis/ kreisfreie Stadt'] != 'Gesamt']

    upload_dataframe(
        df, filename, change_notification=f'RKI-Daten für NRW aktualisiert', compare=compare)

    for studio, areas in studios.items():
        df_studio = df[df['Landkreis/ kreisfreie Stadt'].isin(areas)]
        filename_studio = f'rki_ndr_districts_nrw_{studio}.csv'
        upload_dataframe(df_studio, filename_studio, compare=compare)


# If the file is executed directly, print cleaned data
if __name__ == '__main__':
    df = clear_data()
    #print(df)
    df.to_csv('rki_nrw_2019.csv', index=False, encoding='utf-8', line_terminator='\n')
