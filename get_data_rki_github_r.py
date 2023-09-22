
from functools import lru_cache
from io import StringIO
import pandas as pd
import requests

from utils.storage import upload_dataframe


url = "https://raw.githubusercontent.com/robert-koch-institut/SARS-CoV-2-Nowcasting_und_-R-Schaetzung/main/Nowcast_R_aktuell.csv"

@lru_cache
def get_data():
    # Download website
    response = requests.get(url, timeout=10)
    assert bool(response), "Laden der RKI-GitHub-Daten fehlgeschlagen ({} - {})".format(
        response.status_code, url
    )

    # Parse into data frame
    df = pd.read_csv(StringIO(response.text), sep=",", decimal=".", low_memory=False)

    return df, response


def clear_data():
    df, response = get_data()
    df = df.copy()
    return df


def write_data_rki_github_r():
    df = clear_data()
    upload_dataframe(df, "rki_github_r.csv")
