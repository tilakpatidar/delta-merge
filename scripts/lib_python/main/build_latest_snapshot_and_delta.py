# -*- coding: utf-8 -*-

import os
import sys

from pyspark import SparkContext, HiveContext, Row
from pyspark.sql import DataFrame, Window
from pyspark.sql.functions import lit, row_number, col
from typing import NoReturn, Tuple, List, Optional
from util.df_util import write_df


def main(context, options):
    # type: (Tuple[SparkContext, HiveContext], dict) -> NoReturn

    sc, hive_context = context
    print "Options ", options
    # Read the options
    [input_path,
     input_file_type,
     input_pk_columns,
     input_opts,
     output_path,
     output_path_bad,
     output_path_delta,
     output_file_type,
     partition_column,
     write_mode] = read_options_or_load_defaults(options)

    print "Input path %s" % input_path
    print "Output path %s" % output_path
    print "Output Bad path %s" % output_path_bad
    print "Output Delta path %s" % output_path_delta

    read_format = hive_context.read.format(input_file_type)
    for k in input_opts:
        read_format = read_format.option(k, input_opts[k])
    history = read_format.load(input_path)

    snapshot, bad, deltas_till_file = compute_snapshot_and_bad_records(history, input_pk_columns, sc, hive_context)
    latest_snapshot_id = _generate_latest_snapshot_id(deltas_till_file)

    snapshot = snapshot \
        .drop("o_file_id") \
        .withColumnRenamed("file_id", "o_file_id") \
        .withColumn("file_id", lit(latest_snapshot_id))

    # write the good df and bad df
    write_df(snapshot, write_mode, partition_column, output_file_type, output_path)
    write_df(bad, write_mode, partition_column, output_file_type, output_path_bad)

    # Use the snapshot and bad written on disk
    costs_snapshot_on_disk = hive_context.read.format(output_file_type).load(output_path)

    _copy_new_snapshot_to_history(costs_snapshot_on_disk, input_file_type, input_path)

    delta = _generate_deltas(costs_snapshot_on_disk, deltas_till_file, hive_context, sc)
    write_df(delta, write_mode, partition_column, output_file_type, output_path_delta)


def read_options_or_load_defaults(options):
    default_output_file_format = "parquet"
    default_write_mode = "overwrite"
    input_path = options["input_path"]  # type: str
    input_file_type = options.get("input_file_type", "parquet")  # type: str
    input_pk_columns = options.get("input_pk_columns", {})  # type: str
    input_opts = options.get("input_options", {})  # type: str
    output_path = options.get("output_path")  # type: str
    output_path_bad = options.get("output_path_bad")  # type: str
    output_path_delta = options.get("output_path_delta")  # type: str
    partition_column = options.get("partition_column", None)  # type: str
    output_file_type = options.get("output_file_type", default_output_file_format)  # type: str
    write_mode = options.get("write_mode", default_write_mode)  # type: str
    return (input_path, input_file_type, input_pk_columns, input_opts,
        output_path, output_path_bad, output_path_delta,
        output_file_type,
        partition_column,
        write_mode)


def _copy_new_snapshot_to_history(costs_snapshot_on_disk, input_file_type, input_path):
    print "Copying new snapshot to history"
    write_df(costs_snapshot_on_disk, "append", "file_id,region_code", input_file_type, input_path)


def _generate_deltas(costs_snapshot, deltas_till_file, hive_context, sc):
    # type: (DataFrame, List[str], HiveContext, SparkContext) -> DataFrame
    # Compute deltas for delta and full file, if last file was snapshot no need to calculate delta
    deltas_till_file = list(filter(lambda x: not is_snapshot_file(x), deltas_till_file))
    if len(deltas_till_file) > 0:
        # Deltas were processed
        print "Generating delta for ", deltas_till_file
        delta = costs_snapshot. \
            where(costs_snapshot['file_id'].isin(deltas_till_file))
        return delta
    return hive_context.createDataFrame(sc.emptyRDD(), costs_snapshot.schema)


def is_delta_file(x):
    return ".SNAPSHOT" not in x and ".FULL" not in x


def is_full_file(x):
    return ".FULL" in x


def is_snapshot_file(x):
    return ".SNAPSHOT" in x


def compute_snapshot_and_bad_records(history, input_pk_columns, sc, hive_context):
    # type: (DataFrame, List[str], SparkContext, HiveContext) -> Tuple[DataFrame, DataFrame, List[str]]
    file_ids_df = history.select("file_id").distinct().sort("file_id", ascending=False)
    sorted_file_ids = map(_safe_first_col, file_ids_df.collect())
    latest_file = sorted_file_ids[0]
    print "Latest file ", latest_file
    if not is_delta_file(latest_file):
        # if the latest file is full or snapshot then just use it for snapshot
        file_type = ("FULL" if is_full_file(latest_file) else "SNAPSHOT")
        print "Latest %s file %s" % (file_type, latest_file)
        snapshot = history.where(history['file_id'] == lit(latest_file))
        # if FULL file then compute delta else for SNAPSHOT leave it
        deltas_till_file = [] if is_snapshot_file(latest_file) else [latest_file]
    else:
        print "Latest file is a delta file %s" % latest_file
        latest_snapshot_file = get_first_file_id_like(sorted_file_ids, ".SNAPSHOT")
        latest_full_file = get_first_file_id_like(sorted_file_ids, '.FULL')
        recent_file_type = get_recent_type(latest_full_file, latest_snapshot_file)
        if recent_file_type == "SNAPSHOT":
            # if latest snapshot file is more recent than the recent full file
            deltas_till_file = get_deltas_till_file(file_ids_df, latest_file, latest_snapshot_file)
        else:
            # if latest full file is more recent than the recent snapshot file
            deltas_till_file = get_deltas_till_file(file_ids_df, latest_file, latest_full_file)

        print "Latest snapshot file %s" % latest_snapshot_file
        print "Latest full file %s" % latest_full_file
        print "Most recent is %s" % recent_file_type
        print "Deltas till latest full file ", deltas_till_file

        snapshot = history.where(history["file_id"].isin(deltas_till_file)) \
            .withColumn("rn", row_number().over(Window.partitionBy(input_pk_columns).orderBy(col("file_id").desc())
                                                )).where(col("rn") == 1).drop("rn")
    if len(deltas_till_file) == 0:
        print "No deltas found to merge. Exiting the job."
        sys.exit(0)

    return snapshot, hive_context.createDataFrame(sc.emptyRDD(), snapshot.schema), deltas_till_file


def _generate_latest_snapshot_id(available_deltas):
    # type: (List[str]) -> Optional[str]
    latest_snapshot_id = available_deltas[0] if len(available_deltas) > 0 else None
    if latest_snapshot_id is None:
        return None
    if is_full_file(latest_snapshot_id):
        # When latest file is FULL file
        latest_snapshot_id = latest_snapshot_id.split(".FULL")[0] + ".SNAPSHOT"
    elif is_delta_file(latest_snapshot_id):
        # When latest file is a delta
        latest_snapshot_id = latest_snapshot_id + ".SNAPSHOT"
    print "Latest snapshot id %s" % latest_snapshot_id
    return latest_snapshot_id


def get_recent_type(latest_full_file, latest_snapshot_file):
    if _extract_file_date(latest_snapshot_file) >= _extract_file_date(latest_full_file):
        return "SNAPSHOT"
    else:
        return "FULL"


def get_first_file_id_like(sorted_file_ids, like_pattern):
    # type: (DataFrame, str) -> str
    results = list(filter(lambda x: like_pattern in x, sorted_file_ids))
    return results[0] if len(results) > 0 else None


def _extract_file_date(file_id):
    # type: (str) -> str
    return None if file_id is None else file_id.split(".")[0]


def _safe_first_col(df):
    # type: (Row) -> str
    return None if df is None else df[0]


def get_deltas_till_file(sorted_file_ids, latest_file, latest_full_file):
    return map(lambda row: str(row[0]), sorted_file_ids.where(
        (sorted_file_ids['file_id'] <= lit(latest_file)) & (sorted_file_ids['file_id'] >= lit(latest_full_file))
    ).collect())
