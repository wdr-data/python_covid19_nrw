import json
import datetime
import os

import sentry_sdk
from sentry_sdk.integrations.aws_lambda import AwsLambdaIntegration

sentry_sdk.init(os.environ['SENTRY_URI'],
                integrations=[AwsLambdaIntegration()])

# Import your scraper here ⬇️
from get_data_rki import write_data_rki
from get_data_mags_nrw import write_data_nrw
from get_data_jh_ts_global import write_data_jh_ts_global
from get_data_jh_global import write_data_jh_global

# Add your scraper here ⬇️, without () at the end
SCRAPERS = [
    write_data_nrw,
    write_data_rki,
    write_data_jh_ts_global,
    write_data_jh_ts_filtered,
    write_data_jh_global
]


def scrape(event, context):
    for scraper in SCRAPERS:
        scraper_name = scraper.__name__

        try:
            scraper()
            now = datetime.datetime.now()
            print(f'Updated {scraper_name} at {now}')
        except Exception as e:
            # Catch and send error to Sentry manually so we can continue
            # running other scrapers if one fails
            print(f'Scraper {scraper_name} failed with {e}')
            sentry_sdk.capture_exception(e)

    body = {
        "message": f"Ran {len(SCRAPERS)} scrapers successfully.",
    }

    response = {
        "statusCode": 200,
        "body": json.dumps(body)
    }

    return response


def error(event, context):
    foo = 'bar'
    assert foo == 'foo'


if __name__ == "__main__":
    scrape('', '')
