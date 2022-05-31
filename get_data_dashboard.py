import pandas as pd
import get_data_rki_ndr_districts_nrw
import get_data_rki_github_hospitalization
import get_data_arcgis_nrw_icu
from utils.storage import upload_dataframe


def collect_data():
    df_infections = get_data_rki_ndr_districts_nrw.clear_data_nrw_gesamt()
    (_, df_hospitalizations) = get_data_rki_github_hospitalization.clear_data_adjusted()
    df_icu = get_data_arcgis_nrw_icu.clear_data()

    incidence = df_infections["7-Tage-Inzidenz"].values[0]
    new_infections = df_infections["Neuinfektionen zum Vortag"].values[0]

    hospitalization_incidence = df_hospitalizations["fixierte_7T_Hospitalisierung_Inzidenz"].values[0]

    # icu_percentage = (
    #     df_icu["covid_19_intensiv"].values[-1] / df_icu["covid_19_aktuell"].values[-1] * 100
    # )
    # icu_percentage_diff = (
    #     df_icu["covid_19_intensiv"].values[-1] / df_icu["covid_19_aktuell"].values[-1] * 100
    #     - df_icu["covid_19_intensiv"].values[-2] / df_icu["covid_19_aktuell"].values[-2] * 100
    # )

    deaths = df_infections["Todesfälle"].values[0]
    new_deaths = df_infections["Neue Todesfälle zum Vortag"].values[0]

    record = {
        "7-Tage-Inzidenz^Neuinfektionen^": f"{incidence:.1f}^{new_infections:.0f}^",
        "Hospitalisierungs-<br>inzidenz": f"{hospitalization_incidence:.2f}",
        #"Anteil COVID-Patienten auf <br>Intensiv (%)^Veränderung zum Vortag^": f"{icu_percentage:.2f}^{icu_percentage_diff:+.1f}^",
        "Todesfälle^Veränderung zum Vortag^": f"{deaths}^{new_deaths:+.0f}^",
    }

    # Convert decimal seperators
    for key, value in record.items():
        record[key] = value.replace(".", ",")

    df = pd.DataFrame([record])
    return df


def write_data_dashboard():
    df = collect_data()
    upload_dataframe(df, "dashboard_nrw.csv")


if __name__ == "__main__":
    df = collect_data()
    print(df)
