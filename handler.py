import json
from get_data import get_data
from get_data import clear_data


def hello(event, context):
    clear_data()

    body = {
        "message": "Go Serverless v1.0! Your function executed successfully!",
        "input": event
    }

    response = {
        "statusCode": 200,
        "body": json.dumps(body)
    }

    return response


if __name__ == "__main__":
    hello('', '')
