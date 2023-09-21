from functools import lru_cache
import requests
import pandas as pd

from utils.storage import upload_dataframe

url = "https://www.arcgishostedserver.nrw.de/arcgis/rest/services/Hosted/COVID_19_Entwicklung/FeatureServer/0/query?f=json&where=covid_19_intensiv%20IS%20NOT%20NULL&returnGeometry=false&spatialRel=esriSpatialRelIntersects&outFields=*&orderByFields=datum%20asc&outSR=25832&resultOffset=0&resultRecordCount=16000&resultType=standard&cacheHint=false"


@lru_cache
def get_data():
    # Download website
    response = requests.get(url, timeout=10)
    assert bool(response), "Laden der ARCGIS-NRW-Daten fehlgeschlagen ({} - {})".format(
        response.status_code, url
    )

    data = response.json()

    records = [item["attributes"] for item in data["features"]]

    # Parse into data frame
    df = pd.DataFrame(records)

    return df, response


def clear_data():
    df, response = get_data()
    df = df.copy()

    keep_cols = [
        "datum_text",
        "covid_19_aktuell",
        "covid_19_intensiv",
        "covid_19_intensiv_mit_beatmung",
    ]

    df = df[keep_cols]

    return df


def write_data_arcgis_nrw_icu():
    df = clear_data()

    upload_dataframe(df, "arcgis_nrw_icu.csv")


# If the file is executed directly, print cleaned data
if __name__ == "__main__":
    df = clear_data()
    print(df)
