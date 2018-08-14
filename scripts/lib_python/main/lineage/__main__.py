import sys

from pyspark import HiveContext, SparkContext
from pyspark.sql import DataFrame

data_set_path = sys.argv[1]
data_set_format = sys.argv[2]
primary_cols = sys.argv[3].split(",")
primary_vals = sys.argv[4].split(",")
last_file_id = sys.argv[5]

input_data = {}

sc = SparkContext()
hive_context = HiveContext(sc)

import warnings
warnings.filterwarnings("ignore", message="numpy.dtype size changed")
warnings.filterwarnings("ignore", message="numpy.ufunc size changed")

if len(primary_cols) != len(primary_vals):
    raise Exception("Columns and values should match")

for k, v in zip(primary_cols, primary_vals):
    input_data[k] = v


def _filter_condition(d):
    # type: (dict) -> str
    return " AND ".join(["%s='%s'" % (key, val) for key, val in d.iteritems()])


def main(sc, hive_context, path=data_set_path, df_format=data_set_format, data=input_data,
         file_id=last_file_id):
    # type: (SparkContext, HiveContext, str, dict, str) -> DataFrame
    dataset = hive_context.read.format(df_format).load(path)  # type: DataFrame
    level = 1
    old_file_id = file_id
    print "=" * 50
    print "Input"
    print "Data set path: %s" % path
    print "Data set format: %s" % df_format
    print "Primary data %s" % str(data)
    print "File id %s" % str(file_id)
    print "=" * 50, "\n" * 2
    final_result = hive_context.createDataFrame(sc.emptyRDD(), dataset.schema)
    while True:
        cond = " %s AND file_id = '%s'" % (_filter_condition(data), old_file_id)
        df = dataset.filter(cond).limit(1)
        tk = df.collect()

        if len(tk) != 1:
            break

        final_result = final_result.unionAll(df)

        row = tk[0].asDict()
        if row['o_file_id'] is None:
            break

        old_file_id = row['o_file_id']
        level += 1

    return final_result


if __name__ == '__main__':
    df = main(sc, hive_context)  # type: DataFrame
    df.show(truncate=False)
