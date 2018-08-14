import json
import sys
from logging.config import dictConfig

import build_latest_snapshot_and_delta
from pyspark import SparkContext, HiveContext

job_path = sys.argv[1]

dictConfig(json.loads(open("config/logging.json").read()))

sc = SparkContext()
# Suppress spark console logs to error
sc.setLogLevel("ERROR")
hive_context = HiveContext(sc)
job_config = json.loads(open(job_path, "r").read())
build_latest_snapshot_and_delta.main((sc, hive_context), job_config)
