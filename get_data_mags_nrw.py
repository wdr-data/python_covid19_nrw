import os
import requests
import pandas as pd
from bs4 import BeautifulSoup as bs
import s3fs

url = 'https://www.mags.nrw/coronavirus-fallzahlen-nrw'


class HTMLTableParser:

    def parse_url(self, url):
        response = requests.get(url)
        soup = bs(response.text, 'lxml')
        return [(table, self.parse_html_table(table))
                for table in soup.find_all('table')]

    def parse_html_table(self, table):
        n_columns = 0
        n_rows = 0
        column_names = []

        # Find number of rows and columns
        # we also find the column titles if we can
        for row in table.find_all('tr'):

            # Determine the number of rows in the table
            td_tags = row.find_all('td')
            if len(td_tags) > 0:
                n_rows += 1
                if n_columns == 0:
                    # Set the number of columns for our table
                    n_columns = len(td_tags)

            # Handle column names if we find them
            th_tags = row.find_all('th')
            if len(th_tags) > 0 and len(column_names) == 0:
                for th in th_tags:
                    column_names.append(th.get_text())

        # Safeguard on Column Titles
        if len(column_names) > 0 and len(column_names) != n_columns:
            raise Exception("Column titles do not match the number of columns")

        columns = column_names if len(
            column_names) > 0 else range(0, n_columns)
        df = pd.DataFrame(columns=columns,
                          index=range(0, n_rows))
        row_marker = 0
        for row in table.find_all('tr'):
            column_marker = 0
            columns = row.find_all('td')
            for column in columns:
                df.iat[row_marker, column_marker] = column.get_text()
                column_marker += 1
            if len(columns) > 0:
                row_marker += 1

        return df


def get_data():
    hp = HTMLTableParser()
    table = hp.parse_url(url)[0][1]  # Grabbing the table from the tuple
    df = pd.DataFrame(table)
    return df


def clear_data():
    df = get_data()
    df = df.rename(columns={"Bestätigte Fälle": "Infizierte"})
    df = df.replace('Aachen & Städteregion Aachen', 'Städteregion Aachen')
    df = df.replace('Mülheim / Ruhr', 'Mülheim an der Ruhr')
    df = df.replace(regex=r' +\(Kreis\)', value='')
    df['Landkreis/ kreisfreie Stadt'] = df['Landkreis/ kreisfreie Stadt'].str.strip()
    df = df[df['Landkreis/ kreisfreie Stadt'] != 'Gesamt']
    print(df)
    return df


# clear_data()


def write_data_nrw():
    filename = f's3://{os.environ["BUCKET_NAME"]}/corona_mags_nrw.csv'
    df = clear_data()
    write = df.to_csv(index=False)
    fs = s3fs.S3FileSystem()
    with fs.open(filename, 'w') as f:
        f.write(write)
    fs.setxattr(filename,
                copy_kwargs={"ContentType": "text/plain; charset=utf-8"})
    fs.chmod(filename, 'public-read')


if __name__ == '__main__':
    df = clear_data()
    print(df.to_csv(index=False))
