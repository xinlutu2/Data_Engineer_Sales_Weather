"""Staging file structure creation on local file system

"""

import logging
import os
import shutil

# enabloe logging messages to airflow
log = logging.getLogger(__name__)
# store staging directory in current working directory
local_dir = os.getcwd()

class FileStructure:
	"""Functions related to folder creation based on designed file strcture

	"""

	@classmethod
	def storage_creation(cls, **context):
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

			Creates the base folder with the name English or keywords

			:type context: dict
			:rtype: list
		"""
		log.info("Start create local staging file structure")

		# getting base folder name: English/<keyword>
		storage_name = context['params']['base']
		# getting parent task id
		task_id = context['params']['task_id']
		# passing parent source list to current task
		source_list = context['task_instance'].xcom_pull(task_ids=task_id)
		base = os.path.join(local_dir, storage_name)
		
		# for each source in English(<keyword>)/<source_name> directory, create if not exist
		for source in source_list:
			level = os.path.join(local_dir, storage_name, source)
			if not os.path.exists(level):
				os.makedirs(level)

		log.info("Finish create local staging file structure")

		# pass source list to next task
		return source_list





