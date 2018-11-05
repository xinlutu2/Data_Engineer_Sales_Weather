"""For each source in source list, extract top_headlines through newsapi-python
   Flatted the json file to pandas dataframe without saving json
   Save pandas dataframe as .csv in local filesystem

"""

import logging
import os
import shutil
import json
from pandas.io.json import json_normalize
from newsapi import NewsApiClient

# enabloe logging messages to airflow
log = logging.getLogger(__name__)
key = '96cd169b8c1f43838eb7cf1ec4415945'
# store staging directory in current working directory
local_dir = os.getcwd()

class Transform:
	"""Getting top_headlines for each source
	   Transform from json format to pandas dataframe
	   Save as .csv in local file system 

	"""
	@classmethod
	def get_source_top_headline(cls, **context):
		""" ├── English/<keyword>
			│   ├── <source_name>
			│   │   ├── <pipeline_execution_date>_top_headlines.csv
			│   ├── <source_name>
			│   │   ├── <pipeline_execution_date>_top_headlines.csv
			│   ├── <source_name>
			│   │   ├── <pipeline_execution_date>_top_headlines.csv
			│   ├── <source_name>
			│   │   ├── <pipeline_execution_date>_top_headlines.csv
			.....

			Save top_headlines.csv according to degsined file structurex

			:type context: dict
			:rtype: list
		"""
		# getting execution date
		execution_date = context['ds']
		# getting base folder name: English/<keyword>
		storage_name = context['params']['base']
		# getting parent task id
		task_id = context['params']['task_id']
		base = os.path.join(local_dir, storage_name)
		# passing parent source list to current task
		source_list = context['task_instance'].xcom_pull(task_ids=task_id)		
		# store .csv file full path
		top_headlines_file_list = []

		# for each source in source list, transform and save the request top_headlines
		for source in source_list:
			top_headlines_file_list = cls.source_transform(base,source,execution_date)

		log.info("Checking all files!!")
		log.info(top_headlines_file_list)

		return top_headlines_file_list

	@classmethod
	def source_transform(cls, 
						 base,
						 source,
						 execution_date):

		top_headlines_file_list = []
		# key = 'c4700a5438af49c297bef16446421402'
		api = NewsApiClient(api_key=key)
		# getting top_headlines
		level = os.path.join(base, source)
		top_headlines = api.get_top_headlines(sources=source)

		# flatten json to pandas dataframe
		df = cls.json_to_csv(top_headlines)

		# save pandas dataframe as English/(<keyword>)/<source_name>/<pipeline_execution_date>_top_headlines.csv
		file_name = str(execution_date)+'_top_headlines.csv'
		df.to_csv(os.path.join(level ,file_name))
		top_headlines_file_list.append(str(os.path.join(level ,file_name)))

		return top_headlines_file_list

	@classmethod
	def json_to_csv(cls, top_headlines):

		# flatten json to pandas dataframe
		temp = json.dumps(top_headlines)
		loaded_top_headlines = json.loads(temp)
		df = json_normalize(loaded_top_headlines['articles'])

		return df


	




