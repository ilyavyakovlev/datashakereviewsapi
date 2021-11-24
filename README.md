# datashakereviewsapi: python API to DATASHAKE reviews
# DATASHAKE REVIEWS API

Python API to DATASHAKE reviews (https://www.datashake.com/review-scraper-api)
This module makes it easier to schedule jobs and fetch the results
Official web API documentation: https://api.datashake.com/#reviews
You need to have datashake API key to use this module


## Installation

Through cloning this repositary only. [at the moment]

## Usage examples

Initiate API instance
```sh
from datashakereviewsapi.datashakereviewsapi import DatashakeReviewAPI

# Initiate API instance with your API key from DATASHAKE
api = DatashakeReviewAPI('your_datashake_reviews_scraper_api_key')
```

Schedule a single job with a URL to review page.
DATASHALE API takes several hours to crawl the page and collect the results.
```sh
response = api.schedule_job('https://uk.trustpilot.com/review/store.playstation.com')
# save job_id for querying the results later
first_job_id = response['job_id']
```

Get the job results - reviews
'''sh
reviews = api.get_job_reviews(first_job_id)
'''
