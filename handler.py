import json
from get_data_mags_nrw import write_data_nrw
from get_data_rki import write_data_rki


def scrape(event, context):
    write_data_nrw()
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


if __name__ == "__main__":
    scrape('', '')
