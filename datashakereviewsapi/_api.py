#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Unofficial python API to datashake reviews API
(https://www.datashake.com/review-scraper-api)
This module makes it easier to schedule jobs and fetch the results
Official web API documentation: https://api.datashake.com/#reviews
You need to have datashake API key to use this module
Authors:
    Ilya Yakovlev (ilya.v.yakovlev@gmail.com)
"""
import time
import math
import re
import datetime
import json
import requests
import pandas as pd


def _prepare_date(from_date):
    """
    Private function to prepare from_date by converting
    it to YYYY-MM-DD format.
    """
    # check if from_date was provided and if it was provided in the right
    # format
    from_date_str = None
    if from_date is not None:
        if not isinstance(from_date, str):
            try:
                from_date_str = from_date.strftime('%Y-%m-%d')
            except AttributeError:
                raise ValueError(
                    f"""from_date must be a string in the format YYYY-MM-DD
                        or datetime. String provided: {from_date}"
                    """
                )
        else:
            # regex template for YYYY-MM-DD
            pattern = re.compile("\\d{4}-\\d{2}-\\d{2}")
            match = pattern.match(from_date)
            if match is None:
                raise ValueError(
                    f"""from_date must be a string in the format YYYY-MM-DD \
or datetime. String provided: {from_date}"
                    """
                )
            from_date_str = from_date[0:10]
    return from_date_str


class APIConnectionError(Exception):
    """Exception to handle errors while connecting to API"""


class APIResponseError(Exception):
    """Exception to handle errors received from API"""


class DatashakeReviewAPI:
    """
    Class to manage Datashake Review API (https://api.datashake.com/#reviews)

    Pratameters
    -----------
    api_key : str, 40-symbol api key for Datashake Reviews.
        Must be obtained on their website
    max_requests_per_second : number of requests allowed to be send to
        the API service per second.
        Introduced to avoid 429 status code (Too Many Requests)
        Link to the Datashake doc: https://api.datashake.com/#rate-limiting
    language_code : str, default='en'. Language code of the reviews.
    allow_response : boolean, default=True
    min_days_since_last_crawl : int, default=3 - the number of days
        that need to pass since the last crawl to launch another one
    """

    def __init__(self, api_key, max_requests_per_second=10,
                 language_code='en', allow_response=True,
                 min_days_since_last_crawl=3):
        self.api_key = str(api_key)
        if len(self.api_key) != 40:
            raise ValueError(f"""api_key must be 40 symbols long, \
the key provided was {len(self.api_key)} symbols long"\
""")

        self.max_requests_per_second = max_requests_per_second
        self.language_code = str(language_code)
        self.allow_response = str(allow_response)
        self.min_days_since_last_crawl = min_days_since_last_crawl

        # setting up hidden attribues
        self.__time_counter = 0  # counts in seconds
        self.__requests_done = 0
        self.reviews_per_page = 500

    def __check_load_and_wait(self):
        """
        Hidden method to check workload of requests to API
        and wait to ensure the number of requests
        sent to API stays within the threshold
        Attribute max_requests_per_second regulates the behaviour
        of this method.
        More info here: https://api.datashake.com/#rate-limiting
        """

        if self.__time_counter == 0:
            self.__time_counter = time.perf_counter()
        elif (time.perf_counter() - self.__time_counter) > 1.0:
            self.__time_counter = time.perf_counter()
            self.__requests_done = 1
        elif self.__requests_done < self.max_requests_per_second:
            self.__requests_done += 1
        else:
            wait_secs = 1.0 - (time.perf_counter() - self.__time_counter) + 0.1
            print(f'API overload risk, waiting for {wait_secs} seconds')
            time.sleep(wait_secs)
            self.__requests_done = 1
            self.__time_counter = time.perf_counter()

    def get_job_status(self, job_id):
        """
        Returns the status of the scheduled review job

        Parameters
        ----------
        job_id : str, identificator of the scheduled job

        Returns
        -------
        Dictionary with the job status results. Example:
            {'success': True,
             'status': 200,
             'job_id': 278171040,
             'source_url': 'https://uk.trustpilot.com/review/uk.iqos.com',
             'source_name': 'trustpilot',
             'place_id': None,
             'external_identifier': None,
             'meta_data': None,
             'unique_id': None,
             'review_count': 3400,
             'average_rating': 4.5,
             'last_crawl': '2021-09-28',
             'crawl_status': 'complete',
             'percentage_complete': 100,
             'result_count': 3401,
             'credits_used': 3409,
             'from_date': '2017-01-01',
             'blocks': None}
        """

        url = "https://app.datashake.com/api/v2/profiles/info"
        querystring = {"job_id": str(job_id)}
        headers = {
            'spiderman-token': self.api_key,
            }
        self.__check_load_and_wait()
        response = requests.request("GET", url, headers=headers,
                                    params=querystring)

        if response.ok is False:
            error_str = 'API Connection Error. '
            error_str += f"""Error code: {response.status_code} - \
{response.reason}. URL: {url}"""
            raise APIConnectionError(error_str)

        if response.json()['success'] is False:
            error_str = 'API Response Error. '
            error_str += f"{response.text}. Job ID: {job_id}. URL: {url}"
            raise APIResponseError(error_str)

        return response.json()

    def get_job_reviews(self, job_id, from_date=None):
        """
        Return job status and reviews scraped within the sepcified job if
        job is finished.
        If gob is not finished, the reviews results will be empty

        Parameters
        ----------
        job_id : str, identificator of the job_id that was scheduled to
            scrape the reviews.
        from_date : str or datetime, optional. If not provided, all reviews
            will be queried.
            If from date was provided while scheduling the job you can't get
            any reviews before that date with this method.

        Returns
        -------
        tuple containing:
            dictionary with the job_status from the API
            pandas Dataframe with reviews

        """
        from_date_str = _prepare_date(from_date)
        df_reviews = pd.DataFrame()
        # Chekc the job status
        job_status = self.get_job_status(job_id)
        if not (job_status['success'] and
                job_status['crawl_status'] == 'complete' and
                job_status['review_count'] > 0):
            # early exit
            return (job_status, df_reviews)

        # job complete, let's fetch all the results
        review_count = job_status['review_count']
        pages_count = math.trunc((review_count - 1) /
                                 self.reviews_per_page) + 1
        for page_num in range(1, pages_count + 2):
            url = "https://app.datashake.com/api/v2/profiles/reviews"
            querystring = {"job_id": str(job_id),
                           "language_code": self.language_code,
                           "page": str(page_num),
                           "per_page": self.reviews_per_page,
                           "allow_response": str(self.allow_response)
                           }
            if from_date_str is not None:
                querystring['from_date'] = from_date_str
            headers = {
                'spiderman-token': self.api_key,
                }

            self.__check_load_and_wait()
            response = requests.request("GET", url, headers=headers,
                                        params=querystring)

            if response.ok is False:
                error_str = 'API Connection Error. '
                error_str += f"Error code: {response.status_code} - \
{response.reason}. URL: {url}"
                raise APIConnectionError(error_str)
            df = pd.DataFrame(json.loads(response.text))
            df = df[['job_id', 'source_name', 'reviews']]
            if len(df.index) == 0:
                break
            df = df.join(df['reviews'].apply(pd.Series), how='inner')
            df.drop('reviews', axis=1, inplace=True)
            df_reviews = df_reviews.append(df)
        if df_reviews.index.size > 0:
            df_reviews.set_index('unique_id', inplace=True)
        return (job_status, df_reviews)

    def schedule_job(self, review_url, from_date=None, previous_job_id=None):
        """
        Schedules a new job to get reviews from the url provided.

        Parameters
        ----------
        review_url : str, url to the page with reveiws
        from_date : str in format YYYY-MM-DD or datetime,
            the start dat of the reviews to be collected. Defaults to None.
        previous_job_id : str, id of the previous job that for this url.
            Helps to scrape only delta ans save credits.

        Returns
        -------
        Dictionary with the results of the call. Example:
        {"success":true,"job_id":278171040,"status":200,
         "message":"Added this profile to the queue..."
        }
        """
        from_date_str = _prepare_date(from_date)
        # prepare the parameteres for the POST request
        url = "https://app.datashake.com/api/v2/profiles/add"
        querystring = {"url": review_url}
        if from_date_str is not None:
            querystring['from_date'] = from_date_str
        if previous_job_id is not None:
            querystring['diff'] = str(previous_job_id)
        headers = {
            'spiderman-token': self.api_key,
            }

        # POST request
        self.__check_load_and_wait()
        response = requests.request("POST", url, headers=headers,
                                    params=querystring)

        if response.ok is False:
            error_str = 'API Connection Error. '
            error_str += f"Error code: {response.status_code} - \
{response.reason}. URL: {url}"
            raise APIConnectionError(error_str)

        print(response.json())
        return response.json()

    def schedule_job_list(self, df_jobs_input):
        """
        Schedule or refresh a list of jobs based on the csv file.
        Save the results to the same file.

        Parameters
        ----------
        df_jobs_input :pandas.DataFrame with the list of jobs
            to schedule/reschedule.

        Returns
        -------
        Dataframe with the dataframe after update
        """
        df_jobs = df_jobs_input.copy()
        df_jobs.dropna(axis=0, how='any', subset=['url'], inplace=True)
        for i in df_jobs.index:
            # skip if not enough days passed since last crawl
            if pd.isnull(df_jobs.loc[i, 'status']):
                pass
            else:
                last_crawl = datetime.datetime.strptime(
                    df_jobs.loc[i, 'last_crawl'], '%Y-%m-%d')
                days_since_last_crawl = (datetime.datetime.today() -
                                         last_crawl).days
                if days_since_last_crawl < self.min_days_since_last_crawl:
                    continue

            if pd.isnull(df_jobs.loc[i, 'latest_job_id']):
                print('latest_job_id null')
                # No previous job, schedule the new job
                schedule_job_results = self.schedule_job(df_jobs.loc[i, 'url'])
            else:
                df_jobs.loc[i, :] = self.get_job_status_and_update(
                    df_jobs.loc[i, :])
                if df_jobs.loc[i, 'status'] == 'pending':
                    continue
                # schedule the job with reference to a previous one
                schedule_job_results = self.schedule_job(
                    df_jobs.loc[i, 'url'],
                    previous_job_id=df_jobs.loc[i, 'latest_job_id'])

            # updating the df_jobs with results
            df_jobs.loc[i, 'latest_schedule_message'] = str(
                schedule_job_results)
            if schedule_job_results['success'] is True:
                df_jobs.loc[i, 'latest_job_id'] = \
                    schedule_job_results['job_id']
                df_jobs.loc[i, 'status'] = 'pending'
                df_jobs.loc[i, 'last_crawl'] = str(datetime.date.today())

        return df_jobs

    def get_job_list_reviews(self, df_jobs_input,
                             df_reviews_input=pd.DataFrame()):
        """
        Updates the jobs status and add any new reviews (if found)

        Parameters
        ----------
        df_jobs_input : pandas.DataFrame with the list of jobs
            to get fresh reviews
        df_reviews_input : pandas.DataFrame wiht the list of reviews
            already extracted

        Returns
        -------
        tuple(df_jobs, df_reviews) with updated pandas dataframes
        """
        df_reviews = df_reviews_input.copy()
        new_reviews = pd.DataFrame()
        df_jobs = df_jobs_input.copy()
        if df_jobs.index.size < 1:
            # early exit
            print('No jobs in the list')
            return df_jobs, df_reviews
        for i in df_jobs.index:
            df_jobs.loc[i, :] = self.get_job_status_and_update(
                df_jobs.loc[i, :])
            if df_jobs.loc[i, 'status'] == 'complete':
                _job_status, tmp_reviews = self.get_job_reviews(
                    df_jobs.loc[i, 'latest_job_id'])
                new_reviews = new_reviews.append(tmp_reviews)
        if new_reviews.index.size < 1:
            # early exit
            print('No new reviews found')
            return df_jobs, df_reviews

        intersection_index = df_reviews.index.join(
            new_reviews.index, how='inner')
        new_reviews.drop(intersection_index, inplace=True)
        df_reviews = df_reviews.append(new_reviews)

        return df_jobs, df_reviews

    def get_job_status_and_update(self, job_row_input):
        """
        Updates the status of a review job in a dataframe row

        Parameters
        ----------
        job_row_inout : Pandas.Series with a row from the table with the list
            of jobs and thair statuses.

        Returns
        -------
        job_row updated with the job results
        """
        job_row = job_row_input.copy()
        # ensure we work with the string representation of integer number
        job_row.loc['latest_job_id'] = str(int(job_row['latest_job_id']))
        # check if latest_job_id field is in place
        if len(job_row['latest_job_id']) < 1:
            return job_row
        # update the job status and last craw in the dataframe
        job_status = self.get_job_status(job_row['latest_job_id'])
        job_row.loc['Website'] = job_status['source_name']
        job_row.loc['url'] = job_status['source_url']
        job_row.loc['status'] = job_status['crawl_status']
        job_row.loc['last_crawl'] = job_status['last_crawl']

        return job_row
