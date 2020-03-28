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


# Add your scraper here ⬇️, without () at the end
SCRAPERS = [
    write_data_nrw,
    write_data_rki,
]


def scrape(event, context):
    for scraper in SCRAPERS:
        scraper()
        now = datetime.datetime.now()
        print(f'Updated {scraper.__name__} at {now}')

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


def notify(event, context):
    import slackbot
    slackbot.send_slack_message('Löppt')


if __name__ == "__main__":
    scrape('', '')
