# -*- coding: utf-8 -*-

from pyspark import HiveContext, SparkContext
from pyspark.sql import DataFrame, Window, Column
from pyspark.sql.functions import lit, row_number, col
from pyspark.sql.utils import AnalysisException
from typing import Union, List, Tuple, NoReturn


def merge(delta_df, snapshot, pk_columns):
    # type: (DataFrame, DataFrame, list) -> DataFrame
    full = snapshot.withColumn("version", lit(0)).alias("full")
    delta = delta_df.withColumn("version", lit(1)).alias("delta")

    new_df = full.union(delta).withColumn("rn", row_number().over(
        Window.partitionBy(pk_columns).orderBy(col("version").desc())
    )).where(col("rn") == 1).drop("rn").drop("version")

    return new_df


def build_join_cond(first_df, second_df, pk_columns):
    # type: (DataFrame, DataFrame, list) -> Union[Column, bool]
    join_condition = []
    for pk_col in pk_columns:
        join_condition.append(
            (first_df[pk_col] == second_df[pk_col]))
    joined_cond = reduce(lambda a, b: a & b, join_condition)
    return joined_cond


def aliased_columns(alias, columns):
    # type: (str, str) -> List[str]
    return list(map(lambda x: "%s.%s" % (alias, x), columns))


def drop_cols(df, alias, columns):
    # type: (DataFrame, str, list) -> DataFrame
    cols_to_drop = aliased_columns(alias, columns)
    for drop_col in cols_to_drop:
        df = df.drop(df[drop_col])
    return df


def remove_alias(df, alias):
    # type: (DataFrame, str) -> DataFrame
    return df.select(*list(map(lambda x: col("%s.%s" % (alias, x)).alias(x), df.columns)))


def get_delta_after_validation(delta, bad, pk_columns):
    # type: (DataFrame, DataFrame, list) -> DataFrame

    to_be_removed = delta.join(bad.alias("bad"), build_join_cond(delta, bad, pk_columns), how="inner")
    to_be_removed = drop_cols(to_be_removed, "bad", bad.columns)
    return delta.subtract(to_be_removed)


def safe_read(input_path, input_format, safe_df, context):
    # type: (str, str, DataFrame, Tuple[SparkContext, HiveContext]) -> DataFrame
    sc, hive_context = context
    try:
        return hive_context.read.format(input_format).load(input_path)
    except AnalysisException:
        return hive_context.createDataFrame(sc.emptyRDD(), safe_df.schema)


def write_df(df, write_mode, partition_column, output_file_type, output_path, coalesce=False, coalesce_num=1):
    # type: (DataFrame, str, str, str, str) -> NoReturn
    if coalesce:
        df = df.coalesce(coalesce_num)
    wdf = df.write \
        .mode(write_mode)
    if partition_column is not None:
        wdf = wdf.partitionBy(*partition_column.split(","))
    wdf.format(output_file_type).save(output_path)