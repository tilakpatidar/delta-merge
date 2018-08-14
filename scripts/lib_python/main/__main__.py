import json
import sys
from logging.config import dictConfig

import build_latest_snapshot_and_delta
import logging
from pyspark import SparkContext, HiveContext, SparkConf

job_path = sys.argv[1]

# Load logger config
dictConfig(json.loads(open("config/logging.json").read()))
logger = logging.getLogger("root")

# Load spark config
conf_tuples = map(lambda x: x.split("="), open("config/spark-context.properties", "r").readlines())
print conf_tuples
conf = SparkConf()
for k, v in conf_tuples:
    logger.debug("Setting %s to %s in SparkContext()", k, v)
    conf.set(k, v)
sc = SparkContext(conf=conf)

# Suppress spark console logs to error
sc.setLogLevel("ERROR")
hive_context = HiveContext(sc)
job_config = json.loads(open(job_path, "r").read())
build_latest_snapshot_and_delta.main((sc, hive_context), job_config)
