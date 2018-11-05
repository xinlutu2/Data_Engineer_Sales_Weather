"""Peform GET request through Python client library: newsapi-python
   Get source list and pass to next task

"""

import logging
from newsapi import NewsApiClient

# enabloe logging messages to airflow
log = logging.getLogger(__name__)
key = 'a608e481344441b58d169223c4905961'

class GET_request:
	"""Functions getting list of sources based on language or keyword

	"""
	@classmethod
	def get_en_sources(cls, **context):
		""" Return all English news source id as list
			Because source id will be used to get top_headlines, remove empty source id 

			:type context: dict
			:rtype: list
		"""
		log.info("Start extracting English news source id")

		newsapi = NewsApiClient(api_key=key)
		sources = newsapi.get_sources(language='en')
		source_list = [i['id'] for i in sources['sources']]
		source_list = [x for x in source_list if x] # remove empty source_id

		log.info("Finish extracting English news sources")

		return source_list 

	@classmethod
	def get_key_word_sources(cls, **context):
		""" Return all English news source id as list
			Because source id will be used to get top_headlines, remove empty source id 

			:type context: dict
			:rtype: list
		"""
		keyword = context['params']['keyword'] # getting keyword

		log.info("Start extracting "+keyword+ " news source id")

		newsapi = NewsApiClient(api_key='96cd169b8c1f43838eb7cf1ec4415945')
		all_news = newsapi.get_everything(q=keyword)
		sources = [i['source'] for i in all_news['articles']]
		source_list = [i['id'] for i in sources]
		source_list = [x for x in source_list if x] # remove empty source_id

		log.info("Finish extracting "+keyword+ " news source id")

		return source_list 