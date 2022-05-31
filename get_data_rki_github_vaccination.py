from functools import lru_cache
from io import StringIO

import requests
import pandas as pd

from utils.storage import upload_dataframe

url = "https://raw.githubusercontent.com/robert-koch-institut/COVID-19-Impfungen_in_Deutschland/master/Aktuell_Deutschland_Impfquoten_COVID-19.csv"


@lru_cache
def get_data():
    # Download website
    response = requests.get(url)
    assert bool(response), "Laden der RKI-GitHub-Daten fehlgeschlagen ({} - {})".format(
        response.status_code, url
    )

    # Parse into data frame
    df = pd.read_csv(StringIO(response.text), sep=",", decimal=".", low_memory=False)

    return df, response

def clear_data_current():
    df, response = get_data()
    df = df.copy()

    df_bund = df[(df.Bundesland == "Deutschland")]
    df_nrw = df[(df.Bundesland == "Nordrhein-Westfalen")]

    return (df_bund, df_nrw)


def make_demo_table(df):

    demos = {
        "05bis11": "... 5 bis 11 Jahre",
        "12bis17": "... 12 bis 17 Jahre",
        "18plus": "über 18 Jahre gesamt",
        "18bis59": "... 18 bis 59 Jahre",
        "60plus": "... 60+ Jahre",
    }

    categories = {
        "min1": "Erstimpfung",
        "gi": "Grundimmunisierung",
        "boost1": "erste Auffrischung",
        "boost2": "zweite Auffrischung",
    }

    rows = []

    for demo_source, demo_target in demos.items():
        row = {"": demo_target}
        for cat_source, cat_target in categories.items():
            column = f"Impfquote_{demo_source}_{cat_source}"

            if column not in df.columns:
                row[cat_target] = pd.NA
            else:
                row[cat_target] = df[column].values[0]

        rows.append(row)

    return pd.DataFrame(rows)

def clear_data_demo():
    df, response = get_data()
    df = df.copy()

    df_bund = df[(df.Bundesland == "Deutschland")]
    df_nrw = df[(df.Bundesland == "Nordrhein-Westfalen")]

    df_bund = make_demo_table(df_bund)
    df_nrw = make_demo_table(df_nrw)

    return (df_bund, df_nrw)


def write_data_rki_github_vaccination():
    (df_current_bund, df_current_nrw) = clear_data_current()
    (df_demo_bund, df_demo_nrw) = clear_data_demo()

    upload_dataframe(df_demo_bund, "rki_github_vacc_demo_bund.csv")
    upload_dataframe(df_demo_nrw, "rki_github_vacc_demo_nrw.csv")
    upload_dataframe(df_current_bund, "rki_github_vacc_current_bund.csv")
    upload_dataframe(df_current_nrw, "rki_github_vacc_current_nrw.csv")


# If the file is executed directly, print cleaned data
if __name__ == "__main__":
    (df_current_bund, df_current_nrw) = clear_data_current()
    (df_demo_bund, df_demo_nrw) = clear_data_demo()

    print(df_demo_bund)
    print(df_demo_nrw)
    print(df_current_bund)
    print(df_current_nrw)
