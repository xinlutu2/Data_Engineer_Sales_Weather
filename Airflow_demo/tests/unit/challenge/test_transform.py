import datetime
import pytest
from dags import challenge as c

import os
import shutil
import json
from pandas.io.json import json_normalize
import pandas as pd
from newsapi import NewsApiClient

# from pyfakefs.fake_filesystem_unittest import Patcher

# Sample output: ['/usr/local/airflow/English/abc-news/2018-11-01_top_headlines.csv']
key = 'c4700a5438af49c297bef16446421402'

@pytest.mark.transformtest
class TestTransform():

	def setUp(self):
		self.setUpPyfakefs()

	@pytest.fixture(scope='class')
	def airflow_context_en(self) -> dict:
		"""https://airflow.apache.org/code.html#default-variables"""
		return {
			'ds': datetime.datetime.now().isoformat().split('T')[0],
			'params': {
				'base': 'English',
			},
		}

	def test_json_to_csv(self, airflow_context_en):
		# Act
		newsapi = NewsApiClient(api_key=key)
		top_headlines = newsapi.get_top_headlines(sources='daily-mail')
		df = c.Transform.json_to_csv(top_headlines)

		test_bool = isinstance(df, pd.DataFrame)
		# Assert
		assert test_bool == True

