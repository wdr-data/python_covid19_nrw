# Automated workflow for data scraping

## Process
- Get data from url
- clean data in pandas dataframe
- write dataframe to .csv
- store .csv in s3-bucket as text-file
- deploy scraper to aws lambda and run it there every 3 minutes

## Deploy
- Push your code to the repo, it will get deployed automatically

## Contributing
- Make pull request (or ask for repo membership)
- After review your new scraper will be deployed automatically

## How to test locally
- Run your scraper .py skript: `python get_data_scrapername.py`

## Automated tests
Lambda that checks other lambdas
- Does .csv contain expected content (table headers i.e) - if changes detected ...
- check for last update ... if tis is older than 6 minutes ... sentry-error to slack