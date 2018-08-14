import build_latest_snapshot_and_delta
import sys, json

from pyspark import SparkContext, HiveContext

job_path = sys.argv[1]
sc = SparkContext()
# Suppress spark console logs to error
sc.setLogLevel("ERROR")
hive_context = HiveContext(sc)
job_config = json.loads(open(job_path, "r").read())
build_latest_snapshot_and_delta.main((sc, hive_context), job_config)
