# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""
import pytest
from unittest import mock
import datetime as dt
import pandas as pd
import pickle
import requests
from datashakereviewsapi._api import _prepare_date
from datashakereviewsapi._api import APIResponseError
from datashakereviewsapi import DatashakeReviewAPI


def test_prepare_date():

    assert _prepare_date('2021-01-01 T23:00:00') == '2021-01-01'
    assert _prepare_date(dt.date(2021, 9, 30)) == '2021-09-30'
    assert _prepare_date(dt.datetime(2021, 9, 30)) == '2021-09-30'
    with pytest.raises(ValueError):
        _prepare_date('drft2021-01-01 T23:00:00')


@pytest.fixture
def get_api():
    return DatashakeReviewAPI('aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa')


class FakeResponse(requests.Request):

    def __init__(self, text):
        self.ok = True
        self.text = text


def mocked_get_job_status_request():

    with open('testdata/mock_job_status_response', 'rb') as file:
        response = pickle.load(file)
    response.__class__ = requests.models.Response

    return None


def test_api_init():

    assert str(type(DatashakeReviewAPI('aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'))) == \
        "<class 'datashakereviewsapi._api.DatashakeReviewAPI'>"
    with pytest.raises(ValueError):
        DatashakeReviewAPI('aaa')


def test_get_job_status():

    api = DatashakeReviewAPI('aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa')
    with mock.patch('requests.request') as mocked_function:
        mocked_function.side_effect = mocked_get_job_status_request()
        api.get_job_status('fake_job_id')

    with pytest.raises(APIResponseError):
        api.get_job_status('fake_job_id')


def test_get_job_reviews(get_api):

    api = get_api

    with mock.patch('requests.request') as mocked_function:
        mocked_function.side_effect = mocked_get_job_status_request()
        api.get_job_reviews('fake_job_id')

    with pytest.raises(APIResponseError):
        api.get_job_reviews('fake_job_id')


def test_schedule_job_list(get_api):

    api = get_api

    df = pd.DataFrame(columns=['Website', 'url', 'latest_job_id', 'status',
                               'last_crawl', 'latest_schedule_message'])
    df['url'] = ['test_url']

    with mock.patch('requests.request') as mocked_function:
        mocked_function.side_effect = mocked_get_job_status_request()
        api.schedule_job_list(df)
