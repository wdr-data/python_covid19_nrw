from functools import lru_cache

import pandas as pd
import datetime as dt
import pytz

from data.inhabitants_de import inhabitants_de
from data.studios import studios
from utils.storage import upload_dataframe, download_file

url = 'http://www.divi.de/DIVI-Intensivregister-Tagesreport.csv'

@lru_cache
def get_data():
    # Download DIVI csv
    df = pd.read_csv(url, dtype={'gemeindeschluessel': str})

    return df


# Transform dict of place names into pandas df
df_inh = pd.DataFrame.from_dict(inhabitants_de)
df_inh = df_inh.rename(columns={'rs': 'gemeindeschluessel'})

# Prep studio df for joining
studio_dict = {}

for studio, districts in studios.items():
    for district in districts:
        studio_dict[district] = studio.capitalize().replace('oe', 'ö').replace('ue', 'ü')

df_studio = pd.DataFrame.from_dict(studio_dict,orient = 'index', columns=['studio'])
df_studio = df_studio.rename_axis('kreisname').reset_index()


def clear_data():
    df = get_data()

    # Calculate number of total beds
    df['betten_gesamt'] = df['betten_frei'] + df['betten_belegt']
    
    # Calculate percentage of occupied beds
    df['betten_auslastung'] = df['betten_belegt'] / df['betten_gesamt'] * 100

    # Join DIVI data with place names
    df = df.merge(df_inh, on='gemeindeschluessel', how='left')

    # Join DIVI data with place coordinates
    df_latlong = pd.read_csv('data/kreise-latlong.csv')

    # Turn gemeindeschluessel to numeric for merging
    df['gemeindeschluessel'] = pd.to_numeric(df['gemeindeschluessel'])

    df = df.merge(df_latlong, on='gemeindeschluessel', how='left')

    # Join with studio
    df = df.merge(df_studio, on='kreisname', how='left')

    # retain only necessary columns
    df = df[['lat', 'lon', 'gemeindeschluessel', 'ort', 'bundesland_y', 'studio', 
         'faelle_covid_aktuell', 'faelle_covid_aktuell_beatmet',
         'anzahl_standorte', 'betten_frei', 'betten_belegt', 'betten_gesamt', 'betten_auslastung',
         'daten_stand']]

    # remove '_y'-suffix from 'bundesland
    df = df.rename(columns={'bundesland_y': 'bundesland'})

    return df



def write_data_divi():

    # Prep: Get new data
    df = clear_data()

    # Prep: Create filenames for all dfs 
    fn_ger_map = 'intensivregister_karte_de.csv'
    fn_nrw_map = 'intensivregister_karte_nrw.csv'
    fn_ger_all = 'intensivregister_alle_heute.csv'
    fn_ger_all_archive = 'intensivregister_alle_archiv.csv'
    fn_ger_covid_archive = 'intensivregister_covid_archiv.csv'


    # Check if there is new data in divi csv
    # Use all unique values in "daten_stand"-column in case there are different dates
    dates_list = [dt.datetime.strptime(date, '%Y-%m-%d %H:%M:%S').date() for date in df.daten_stand.unique()]
    # use only latest date from all dates in divi file
    datenstand_divi = max(dates_list)

    # Get yesterdays date and compare
    today = dt.datetime.now(pytz.timezone('Europe/Berlin')).date()
    yesterday = today - dt.timedelta(days = 1) 

    # Update only if there is new data
    if(datenstand_divi > yesterday):
    
        # Germany: latlong + places + all numbers for current day
        df_ger_map = df

        # NRW: latlong + places + all numbers for current day
        df_nrw_map = df[df['bundesland'] == 'Nordrhein-Westfalen']

        # Germany: summarize values for current day
        # Aggregate data to get totals for Germany
        df_ger_all_base = df.agg({
            'betten_belegt': 'sum',
            'betten_frei': 'sum',
            'betten_gesamt': 'sum',
            'faelle_covid_aktuell': 'sum',
            'faelle_covid_aktuell_beatmet': 'sum'
            })

        # Create dict as base for df in desired format
        ger_all_data = {
            'Intensivbetten': ['Freie Betten', 'Patienten (nicht COVID-19)', 'COVID-19 Patienten (nicht beatmet)', 'COVID-19 Patienten (beatmet)'],
            'Anzahl': [
                df_ger_all_base['betten_frei'], 
                df_ger_all_base['betten_belegt'] - df_ger_all_base['faelle_covid_aktuell'],
                df_ger_all_base['faelle_covid_aktuell'] - df_ger_all_base['faelle_covid_aktuell_beatmet'], 
                df_ger_all_base['faelle_covid_aktuell_beatmet']
                ]
        }

        # Create df from dict
        df_ger_all = pd.DataFrame(data = ger_all_data)

        # Store same data plus current date as dict to append to archive
        ger_all_today = {
            'X.1': today,
            'Freie Betten': df_ger_all_base['betten_frei'],
            'Patienten (nicht COVID-19)': df_ger_all_base['betten_belegt'] - df_ger_all_base['faelle_covid_aktuell'],
            'COVID-19 Patienten (nicht beatmet)': df_ger_all_base['faelle_covid_aktuell'] - df_ger_all_base['faelle_covid_aktuell_beatmet'],
            'COVID-19 Patienten (beatmet)': df_ger_all_base['faelle_covid_aktuell_beatmet']
        }

        # Subset df with only Covid cases
        sum_ger_covid_today = {
            'X.1': today,
            'COVID-19 Patienten (nicht beatmet)': df_ger_all_base['faelle_covid_aktuell'] - df_ger_all_base['faelle_covid_aktuell_beatmet'],
            'COVID-19 Patienten (beatmet)': df_ger_all_base['faelle_covid_aktuell_beatmet']
        }

        # Fetch archived data and add current row

        # Filepath for archive with all columns
        fp_all_archive = str(yesterday) + "/" + fn_ger_all_archive
        df_ger_all_archive = pd.read_csv(download_file(fp_all_archive))

        # Local testing:
        # df_ger_all_archive = pd.read_csv('data/divi_archive_all.csv')

        # Append today's data to archive
        df_ger_all_archive = df_ger_all_archive.append(ger_all_today, ignore_index=True)

        # Filepath for COVID columns only
        fp_covid_archive = str(yesterday) + "/" + fn_ger_covid_archive
        df_ger_covid_archive = pd.read_csv(download_file(fp_covid_archive))

        # Local testing:
        # df_ger_covid_archive = pd.read_csv('data/divi_archive_covid.csv')

        # Append today's data to archive
        df_ger_covid_archive = df_ger_covid_archive.append(sum_ger_covid_today, ignore_index=True)

        # upload all dfs
        upload_dataframe(df_ger_map, fn_ger_map)
        upload_dataframe(df_nrw_map, fn_nrw_map)
        upload_dataframe(df_ger_all, fn_ger_all)
        upload_dataframe(df_ger_all_archive, fn_ger_all_archive)
        upload_dataframe(df_ger_covid_archive, fn_ger_covid_archive)


# If the file is executed directly, print cleaned data
if __name__ == '__main__':
    df = clear_data()
    # print(df.to_csv(index=False))

    # paste code from write_data() for local testing             
        # print(fn_ger_map)
        # print(df_ger_map.head().to_csv(index=False))
        # print(fn_nrw_map)
        # print(df_nrw_map.to_csv(index=False))
        # print(fn_ger_all)
        # print(df_ger_all.to_csv(index=False))
        # print(fn_ger_all_archive)
        # print(df_ger_all_archive.tail().to_csv(index=False))
        # print(fn_ger_covid_archive)
        # print(df_ger_covid_archive.tail().to_csv(index=False))

