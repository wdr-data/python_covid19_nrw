import json
import datetime
import os

import sentry_sdk
from sentry_sdk.integrations.aws_lambda import AwsLambdaIntegration

from get_data_rki import write_data_rki
from get_data_mags_nrw import write_data_nrw

sentry_sdk.init(os.environ['SENTRY_URI'],
                integrations=[AwsLambdaIntegration()])


def scrape(event, context):
    write_data_nrw()
    now = datetime.datetime.now()
    print(f'Updated: {now}')
    write_data_rki()

    body = {
        "message": "Go Serverless v1.0! Your function executed successfully!",
        "input": event
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
