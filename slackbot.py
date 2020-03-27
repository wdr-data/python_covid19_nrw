import json
import os

import requests

# Set the webhook_url to the one provided by Slack when you create the webhook at https://my.slack.com/services/new/incoming-webhook/

webhook_url = os.environ.get('SLACK_WEBHOOK')

def send_slack_message(message):
    if not webhook_url:
        return

    slack_data = {
        'text': message,
        'icon_emoji': ':robot_face:',
        'username': 'Mags-Slackbot'}

    response = requests.post(
        webhook_url, data=json.dumps(slack_data),
        headers={'Content-Type': 'application/json'}
    )
    if response.status_code != 200:
        raise ValueError(
            'Request to slack returned an error %s, the response is:\n%s'
            % (response.status_code, response.text)
        )
