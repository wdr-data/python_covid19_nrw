import os
import requests
import pandas as pd
from bs4 import BeautifulSoup as bs
import s3fs
import re

url = 'https://www.mags.nrw/coronavirus-fallzahlen-nrw'


class HTMLTableParser:

    def parse_url(self, url):
        response = requests.get(url)
        soup = bs(response.text, 'lxml')
        date = soup.find('dc.date.modified')
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


def parse_date(url):
    response = requests.get(url)
    soup = bs(response.text, 'lxml')

    textBlock = soup(text=re.compile(r'Aktueller Stand: (.*)'))

    for block in textBlock:
        re_search = re.search('Aktueller Stand: (.*)(, .*)(\.)', block)
        if re_search:
            date = re_search.group(1)
            dateText = date + re_search.group(2)
        if not dateText:
            meta_date = soup.find('meta', attrs={'name': 'dc.date.modified'})
            dateText = meta_date.get('content')
    return dateText


print(parse_date(url))


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
    df = df.replace(regex=r'\.', value='')
    df['Landkreis/ kreisfreie Stadt'] = df['Landkreis/ kreisfreie Stadt'].str.strip()
    df = df[df['Landkreis/ kreisfreie Stadt'] != 'Gesamt']

    df.Infizierte = df.Infizierte.replace(u'\xa0', u' ')
    df.Infizierte = df.Infizierte.replace(u' ', 0)
    df.Infizierte = df.Infizierte.astype('int')

    df.Todesfälle = df.Todesfälle.replace(u'\xa0', u' ')
    df.Todesfälle = df.Todesfälle.replace(u' ', 0)
    df.Todesfälle = df.Todesfälle.astype('int')

    inhabitants = {
        'Bielefeld': 333786,
        'Bochum': 364628,
        'Bonn': 327258,
        'Borken': 370676,
        'Bottrop': 117383,
        'Coesfeld': 219929,
        'Dortmund': 587010,
        'Duisburg': 498590,
        'Düren': 263722,
        'Düsseldorf': 619294,
        'Ennepe-Ruhr-Kreis': 324296,
        'Essen': 583109,
        'Euskirchen': 192840,
        'Gelsenkirchen': 260654,
        'Gütersloh': 364083,
        'Hagen': 188814,
        'Hamm': 179111,
        'Heinsberg': 254322,
        'Herford': 250783,
        'Herne': 156374,
        'Hochsauerlandkreis': 260475,
        'Höxter': 140667,
        'Kleve': 310974,
        'Köln': 1085664,
        'Krefeld': 227020,
        'Leverkusen': 163838,
        'Lippe': 348391,
        'Märkischer Kreis': 412120,
        'Mettmann': 485684,
        'Minden-Lübbecke': 310710,
        'Mönchengladbach': 261454,
        'Mülheim an der Ruhr': 170880,
        'Münster': 314319,
        'Oberbergischer Kreis': 272471,
        'Oberhausen': 210829,
        'Olpe': 134775,
        'Paderborn': 306890,
        'Recklinghausen': 615261,
        'Remscheid': 110994,
        'Rhein-Erft-Kreis': 470089,
        'Rhein-Kreis Neuss': 451007,
        'Rhein-Sieg-Kreis': 599780,
        'Rheinisch-Bergischer Kreis': 283455,
        'Siegen-Wittgenstein': 278210,
        'Soest': 301902,
        'Solingen': 159360,
        'Städteregion Aachen': 555465,
        'Steinfurt': 447614,
        'Unna': 394782,
        'Viersen': 298935,
        'Warendorf': 277783,
        'Wesel': 459809,
        'Wuppertal': 354382,
        'Gesamt': 17932651,
    }

    df_inhabitants = pd.DataFrame(inhabitants.items(), columns=[
                                  'Landkreis/ kreisfreie Stadt', 'Einwohner'])

    df = pd.merge(df, df_inhabitants)
    df.Einwohner = df.Einwohner.astype('int')
    df['Infizierte pro 100.000'] = (
        df.Infizierte * 100000 / df.Einwohner).round(1)

    df['Stand'] = parse_date(url)

    return df


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
