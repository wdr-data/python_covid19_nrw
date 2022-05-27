import json
import datetime
import os

import sentry_sdk
from sentry_sdk.integrations.aws_lambda import AwsLambdaIntegration

sentry_sdk.init(os.environ['SENTRY_URI'],
                integrations=[AwsLambdaIntegration()])

# Import your scraper here ⬇️
from get_data_rki import write_data_rki
from get_data_rki_ndr_districts import write_data_rki_ndr_districts
from get_data_rki_ndr_districts_nrw import write_data_rki_ndr_districts_nrw
from get_data_divi import write_data_divi
from get_data_rki_github_hospitalization import write_data_rki_github_hospitalization
from get_data_rki_github_vaccination import write_data_rki_github_vaccination
from get_data_arcgis_nrw_icu import write_data_arcgis_nrw_icu

# Add your scraper here ⬇️, without () at the end
SCRAPERS = [
    write_data_rki,
    write_data_rki_ndr_districts,
    write_data_rki_ndr_districts_nrw,
    write_data_divi,
    write_data_rki_github_hospitalization,
    write_data_rki_github_vaccination,
    write_data_arcgis_nrw_icu,
]


def scrape(event, context):
    for scraper in SCRAPERS:
        scraper_name = scraper.__name__
        with sentry_sdk.configure_scope() as scope:
            scope.set_tag("scraper", scraper_name)
            try:
                scraper()
                now = datetime.datetime.now()
                print(f'Updated {scraper_name} at {now}')
            except Exception as e:
                # Catch and send error to Sentry manually so we can continue
                # running other scrapers if one fails
                print(f'Scraper {scraper_name} failed with {e}')
                print(e)
                sentry_sdk.capture_exception(e)

    body = {
        "message": f"Ran {len(SCRAPERS)} scrapers successfully.",
    }

    response = {
        "statusCode": 200,
        "body": json.dumps(body)
    }

    return response


if __name__ == "__main__":
    scrape('', '')
