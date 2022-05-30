import pandas as pd
from get_data_rki_ndr_districts_nrw import clear_data_nrw_gesamt
from utils.storage import download_file, upload_dataframe


def write_data_rki_ndr_history():
    archive_name = "rki_ndr_districts_nrw_gesamt_history.csv"

    df_today = clear_data_nrw_gesamt()
    df_today.drop(columns=["Studio-Link"], inplace=True)
    df_today["Stand"] = pd.to_datetime(df_today["Stand"].str.slice(0, 8), dayfirst=True)

    df_archive = pd.read_csv(download_file(archive_name), parse_dates=["Stand"])

    df = pd.concat([df_archive, df_today])

    # Remove duplicate rows
    df.drop_duplicates(subset=["Stand"], inplace=True, keep="last")
    df.reset_index(drop=True, inplace=True)

    # Update calculated columns
    df["Gemeldete Neuinfektionen 7-Tage-Mittel"] = df["Neuinfektionen zum Vortag"].rolling(7).mean().round(1)

    upload_dataframe(df, archive_name)
