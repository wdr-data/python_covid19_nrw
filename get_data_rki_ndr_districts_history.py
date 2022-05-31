import datetime as dt

import pandas as pd

from utils.storage import download_file, upload_dataframe


def get_data():
    # List of the last 7 days
    dates = [
        dt.date.today() - dt.timedelta(days=i)
        for i in reversed(range(7))
    ]
    files = [
        download_file(f"{date.isoformat()}/rki_ndr_districts_nrw.csv")
        for date in dates
    ]

    dfs = [pd.read_csv(file) for file in files]
    return dfs


def clean_data():
    dfs = get_data()

    districts = dfs[0]["Landkreis/ kreisfreie Stadt"]
    incidence_columns = [
        df["7-Tage-Inzidenz"]
        for df in dfs
    ]

    for i, column in enumerate(incidence_columns):
        column.name = f"Minus_{6 - i}"

    df = pd.concat([districts] + incidence_columns, axis=1)

    return df


def write_data_rki_ndr_districts_history():
    df = clean_data()
    upload_dataframe(df, "rki_ndr_districts_nrw_history.csv")


if __name__ == "__main__":
    df = clean_data()
    print(df)
