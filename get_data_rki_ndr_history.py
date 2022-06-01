import pandas as pd
from get_data_rki_ndr_districts_nrw import clear_data_nrw_gesamt
from utils.storage import download_file, upload_dataframe


def _move_stand_to_front(df):
    col = df["Stand"]
    df.drop(columns=["Stand"], inplace=True)
    df.insert(0, "Stand", col)


def write_data_rki_ndr_history():
    archive_name = "rki_ndr_districts_nrw_gesamt_history"

    df_today = clear_data_nrw_gesamt()
    df_today.drop(columns=["Studio-Link"], inplace=True)
    df_today["Stand"] = pd.to_datetime(df_today["Stand"].str.slice(0, 8), dayfirst=True)
    _move_stand_to_front(df_today)

    df_archive = pd.read_csv(download_file(f"{archive_name}.csv"), parse_dates=["Stand"])

    # TODO: Remove this workaround eventually, really only needs to happen once for consistency
    _move_stand_to_front(df_archive)

    df = pd.concat([df_archive, df_today])

    # Remove duplicate rows
    df.drop_duplicates(subset=["Stand"], inplace=True, keep="last")
    df.reset_index(drop=True, inplace=True)

    # Update calculated columns
    df["Gemeldete Neuinfektionen 7-Tage-Mittel"] = df["Neuinfektionen zum Vortag"].rolling(7).mean().round(1)
    df["Gemeldete Neue Todesfälle 7-Tage-Mittel"] = df["Neue Todesfälle zum Vortag"].rolling(7).mean().round(1)

    upload_dataframe(df, f"{archive_name}.csv")

    # Only keep the last 90 days
    df_90 = df[df["Stand"] >= df["Stand"].max() - pd.Timedelta(days=90)]
    upload_dataframe(df_90, f"{archive_name}_90_days.csv")
