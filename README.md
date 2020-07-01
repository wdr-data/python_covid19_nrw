# Automated workflow for data scraping

## Process
- Get data from URL
- Clean data in Pandas dataframe
- Convert dataframe to .csv
- Store .csv in S3 bucket as text file
- Deploy scraper to AWS lambda and run it there every 15 minutes

## Creating a new scraper
- Create a copy of the `scraper_template.py` file or one of the existing scrapers
- Implement your scraper
- Add your scraper to the list of scrapers in `handler.py`

## How to test locally
- Run your scraper .py skript: `python get_data_scrapername.py`

## Deploy
- Push your code to the repo, it will get deployed automatically

## Contributing
- Make pull request (or ask for repo membership)
- After review your new scraper will be deployed automatically

## Error reporting
- Any errors in a scraper are reported to Sentry
- Use asserts to confirm the data is valid
- If non-critical but interesting changes to the data are noticed, report them to Slack via Sentry message
