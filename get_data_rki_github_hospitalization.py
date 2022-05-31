from functools import lru_cache
from io import StringIO

import requests
import pandas as pd

from utils.storage import upload_dataframe

url_adjusted = "https://raw.githubusercontent.com/robert-koch-institut/COVID-19-Hospitalisierungen_in_Deutschland/master/Aktuell_Deutschland_adjustierte-COVID-19-Hospitalisierungen.csv"


@lru_cache
def get_data_adjusted():
    # Download website
    response = requests.get(url_adjusted)
    assert bool(response), "Laden der RKI-GitHub-Daten fehlgeschlagen ({} - {})".format(
        response.status_code, url_adjusted
    )

    # Parse into data frame
    df = pd.read_csv(StringIO(response.text), sep=",", decimal=".", low_memory=False, parse_dates=["Datum"])

    return df, response


def clear_data_adjusted():
    df, response = get_data_adjusted()
    df = df.copy()

    df_gesamt = df[(df.Altersgruppe == "00+")]
    df_gesamt.drop(
        [
            "Bundesland_Id",
            "Altersgruppe",
        ],
        axis=1,
        errors="ignore",
        inplace=True,
    )

    df_bund = df_gesamt[(df_gesamt.Bundesland == "Bundesgebiet")].copy()
    df_nrw = df_gesamt[(df_gesamt.Bundesland == "Nordrhein-Westfalen")].copy()

    return (df_bund, df_nrw)


def write_data_rki_github_hospitalization():
    (df_adj_bund, df_adj_nrw) = clear_data_adjusted()

    upload_dataframe(df_adj_bund, "rki_github_hosp_adj_bund.csv")
    upload_dataframe(df_adj_nrw, "rki_github_hosp_adj_nrw.csv")

    # Save version with only last 90 days
    df_adj_bund_90 = df_adj_bund[df_adj_bund["Datum"] >= df_adj_bund["Datum"].max() - pd.Timedelta(days=90)]
    df_adj_nrw_90 = df_adj_nrw[df_adj_nrw["Datum"] >= df_adj_nrw["Datum"].max() - pd.Timedelta(days=90)]

    upload_dataframe(df_adj_bund_90, "rki_github_hosp_adj_bund_90.csv")
    upload_dataframe(df_adj_nrw_90, "rki_github_hosp_adj_nrw_90.csv")


# If the file is executed directly, print cleaned data
if __name__ == "__main__":
    dfs_adjusted = get_data_adjusted()

    # for df in dfs_adjusted:
    #     print(df.to_csv(index=False))
