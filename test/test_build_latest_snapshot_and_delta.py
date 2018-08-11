# -*- coding: utf-8 -*-
import sys

import pandas as pd
import pytest
import main.build_latest_snapshot_and_delta as build_snapshot
from pyspark import HiveContext, SparkContext
from typing import NoReturn

from conftest import date_before_today, date_after_today, date, assert_frame_equal_with_sort

sys.path.append('scripts')

pytestmark = pytest.mark.usefixtures("spark_context", "hive_context")
key_columns = ["product_id", "region_code", "start_date", "end_date", "file_id"]


def test_when_latest_file_is_delta_file(spark_context, hive_context):
    # type: (SparkContext, HiveContext) -> NoReturn

    costs = hive_context.createDataFrame(pd.DataFrame([
        {
            'product_id': '1',
            'region_code': 'AB98ABCD',
            'start_date': date_before_today(5),
            'end_date': date_after_today(5),
            'cogs': 75.67,
            'acogs': 76.67,
            'ocogs': 76.67,
            'file_id': '20180726T121212'
        },
        {
            'product_id': '1',
            'region_code': 'AB98ABCD',
            'start_date': date_before_today(5),
            'end_date': date_after_today(5),
            'cogs': 72.67,
            'acogs': 76.67,
            'ocogs': 76.67,
            'file_id': '20180727T121212'
        },
        {
            'product_id': '1',
            'region_code': 'AB98ABCD',
            'start_date': date_before_today(3),
            'end_date': date_after_today(2),
            'cogs': 72.67,
            'acogs': 76.67,
            'ocogs': 76.67,
            'file_id': '20180727T121212'
        },
        {
            'product_id': '12',
            'region_code': 'AB98ABCD',
            'start_date': date_before_today(3),
            'end_date': date_after_today(2),
            'cogs': 72.67,
            'acogs': 76.67,
            'ocogs': 76.67,
            'file_id': '20180727T121212'
        },
        {
            'product_id': '14',
            'region_code': 'AB98ABCD',
            'start_date': date_before_today(3),
            'end_date': date_after_today(2),
            'cogs': 72.67,
            'acogs': 76.67,
            'ocogs': 76.67,
            'file_id': '20180727T121212'
        },
        {
            'product_id': '2',
            'region_code': 'BC99BCDF',
            'start_date': date('2018-10-09'),
            'end_date': date('2018-10-19'),
            'cogs': 23.67,
            'acogs': 46.67,
            'ocogs': 36.67,
            'file_id': '20180726T121212'
        },
        {

            'product_id': '3',
            'region_code': 'CD00CDE',
            'start_date': date_before_today(2),
            'end_date': date_after_today(200),
            'cogs': 723.67,
            'acogs': 6.67,
            'ocogs': 96.67,
            'file_id': '20180726T121212'
        },
        {

            'product_id': '3',
            'region_code': 'CD00CDE',
            'start_date': date_before_today(2),
            'end_date': date_after_today(200),
            'cogs': 71.67,
            'acogs': 6.67,
            'ocogs': 96.67,
            'file_id': '20180725T121212.FULL'
        },
        {

            'product_id': '8',
            'region_code': 'CD00CDE',
            'start_date': date_before_today(2),
            'end_date': date_after_today(200),
            'cogs': 70.67,
            'acogs': 6.67,
            'ocogs': 96.67,
            'file_id': '20180725T121212.FULL'
        },
        {

            'product_id': '3',
            'region_code': 'CD00CDE',
            'start_date': date_before_today(2),
            'end_date': date_after_today(200),
            'cogs': 77.67,
            'acogs': 6.67,
            'ocogs': 96.67,
            'file_id': '20180720T121212.FULL'
        },
        {

            'product_id': '3',
            'region_code': 'CD00CDE',
            'start_date': date_before_today(2),
            'end_date': date_after_today(200),
            'cogs': 56.67,
            'acogs': 60.67,
            'ocogs': 96.67,
            'file_id': '20180720T121212'
        },
        {
            'product_id': '4',
            'region_code': 'DE01DEF',
            'start_date': date('2018-05-05'),
            'end_date': date('2018-05-08'),
            'cogs': 23.67,
            'acogs': 6.67,
            'ocogs': 56.67,
            'file_id': '20180726T121212'
        },
        {
            'product_id': '4',
            'region_code': 'DE01DEF',
            'start_date': date('2018-05-05'),
            'end_date': date('2018-05-08'),
            'cogs': 23.67,
            'acogs': 6.67,
            'ocogs': 56.67,
            'file_id': '20180706T121212'
        }
    ]))

    expected_costs_snapshot_pd = pd.DataFrame([
        {
            'product_id': '12',
            'region_code': 'AB98ABCD',
            'start_date': date_before_today(3),
            'end_date': date_after_today(2),
            'cogs': 72.67,
            'acogs': 76.67,
            'ocogs': 76.67,
            'file_id': '20180727T121212'
        }, {
            'product_id': '14',
            'region_code': 'AB98ABCD',
            'start_date': date_before_today(3),
            'end_date': date_after_today(2),
            'cogs': 72.67,
            'acogs': 76.67,
            'ocogs': 76.67,
            'file_id': '20180727T121212'
        },
        {
            'product_id': '2',
            'region_code': 'BC99BCDF',
            'start_date': date('2018-10-09'),
            'end_date': date('2018-10-19'),
            'cogs': 23.67,
            'acogs': 46.67,
            'ocogs': 36.67,
            'file_id': '20180726T121212'
        },
        {

            'product_id': '3',
            'region_code': 'CD00CDE',
            'start_date': date_before_today(2),
            'end_date': date_after_today(200),
            'cogs': 723.67,
            'acogs': 6.67,
            'ocogs': 96.67,
            'file_id': '20180726T121212'
        },
        {

            'product_id': '8',
            'region_code': 'CD00CDE',
            'start_date': date_before_today(2),
            'end_date': date_after_today(200),
            'cogs': 70.67,
            'acogs': 6.67,
            'ocogs': 96.67,
            'file_id': '20180725T121212.FULL'
        },
        {
            'product_id': '4',
            'region_code': 'DE01DEF',
            'start_date': date('2018-05-05'),
            'end_date': date('2018-05-08'),
            'cogs': 23.67,
            'acogs': 6.67,
            'ocogs': 56.67,
            'file_id': '20180726T121212'
        },
        {
            'product_id': '1',
            'region_code': 'AB98ABCD',
            'start_date': date_before_today(5),
            'end_date': date_after_today(5),
            'cogs': 72.67,
            'acogs': 76.67,
            'ocogs': 76.67,
            'file_id': '20180727T121212'
        },
        {
            'product_id': '1',
            'region_code': 'AB98ABCD',
            'start_date': date_before_today(3),
            'end_date': date_after_today(2),
            'cogs': 72.67,
            'acogs': 76.67,
            'ocogs': 76.67,
            'file_id': '20180727T121212'
        },
    ])

    expected_costs_delta_pd = pd.DataFrame([
        {
            'product_id': '4',
            'region_code': 'DE01DEF',
            'start_date': date('2018-05-05'),
            'end_date': date('2018-05-08'),
            'cogs': 23.67,
            'acogs': 6.67,
            'ocogs': 56.67,
            'file_id': '20180726T121212'
        },
        {

            'product_id': '3',
            'region_code': 'CD00CDE',
            'start_date': date_before_today(2),
            'end_date': date_after_today(200),
            'cogs': 723.67,
            'acogs': 6.67,
            'ocogs': 96.67,
            'file_id': '20180726T121212'
        },
        {
            'product_id': '2',
            'region_code': 'BC99BCDF',
            'start_date': date('2018-10-09'),
            'end_date': date('2018-10-19'),
            'cogs': 23.67,
            'acogs': 46.67,
            'ocogs': 36.67,
            'file_id': '20180726T121212'
        },
        {
            'product_id': '12',
            'region_code': 'AB98ABCD',
            'start_date': date_before_today(3),
            'end_date': date_after_today(2),
            'cogs': 72.67,
            'acogs': 76.67,
            'ocogs': 76.67,
            'file_id': '20180727T121212'
        }, {
            'product_id': '14',
            'region_code': 'AB98ABCD',
            'start_date': date_before_today(3),
            'end_date': date_after_today(2),
            'cogs': 72.67,
            'acogs': 76.67,
            'ocogs': 76.67,
            'file_id': '20180727T121212'
        },
        {

            'product_id': '8',
            'region_code': 'CD00CDE',
            'start_date': date_before_today(2),
            'end_date': date_after_today(200),
            'cogs': 70.67,
            'acogs': 6.67,
            'ocogs': 96.67,
            'file_id': '20180725T121212.FULL'
        },
        {
            'product_id': '1',
            'region_code': 'AB98ABCD',
            'start_date': date_before_today(5),
            'end_date': date_after_today(5),
            'cogs': 72.67,
            'acogs': 76.67,
            'ocogs': 76.67,
            'file_id': '20180727T121212'
        },
        {
            'product_id': '1',
            'region_code': 'AB98ABCD',
            'start_date': date_before_today(3),
            'end_date': date_after_today(2),
            'cogs': 72.67,
            'acogs': 76.67,
            'ocogs': 76.67,
            'file_id': '20180727T121212'
        },
    ])

    print "input costs history"
    costs.show()

    result, bad, deltas_till_file = build_snapshot.compute_snapshot_and_bad_records(costs,
                                                                                    ["product_id", "region_code",
                                                                                     "start_date",
                                                                                     "end_date"], spark_context,
                                                                                    hive_context)
    delta = build_snapshot._generate_deltas(result, deltas_till_file, hive_context, spark_context)
    latest_snapshot_id = build_snapshot._generate_latest_snapshot_id(deltas_till_file)

    print "Result costs snapshot"
    result.show()
    print "Bad records"
    bad.show()
    print "Delta"
    delta.show()
    assert_frame_equal_with_sort(result.toPandas(), expected_costs_snapshot_pd, key_columns)
    assert_frame_equal_with_sort(delta.toPandas(), expected_costs_delta_pd, key_columns)
    assert latest_snapshot_id == "20180727T121212.SNAPSHOT"


def test_when_latest_file_is_full_file(spark_context, hive_context):
    # type: (SparkContext, HiveContext) -> NoReturn

    costs = hive_context.createDataFrame(pd.DataFrame([
        {
            'product_id': '1',
            'region_code': 'AB98ABCD',
            'start_date': date_before_today(5),
            'end_date': date_after_today(5),
            'cogs': 75.67,
            'acogs': 76.67,
            'ocogs': 76.67,
            'file_id': '20180726T121212'
        },
        {
            'product_id': '1',
            'region_code': 'AB98ABCD',
            'start_date': date_before_today(5),
            'end_date': date_after_today(5),
            'cogs': 72.67,
            'acogs': 76.67,
            'ocogs': 76.67,
            'file_id': '20180727T121212.FULL'
        },
        {
            'product_id': '1',
            'region_code': 'AB98ABCD',
            'start_date': date_before_today(3),
            'end_date': date_after_today(2),
            'cogs': 72.67,
            'acogs': 76.67,
            'ocogs': 76.67,
            'file_id': '20180727T121212.FULL'
        },
        {
            'product_id': '12',
            'region_code': 'AB98ABCD',
            'start_date': date_before_today(3),
            'end_date': date_after_today(2),
            'cogs': 72.67,
            'acogs': 76.67,
            'ocogs': 76.67,
            'file_id': '20180727T121212.FULL'
        },
        {
            'product_id': '14',
            'region_code': 'AB98ABCD',
            'start_date': date_before_today(3),
            'end_date': date_after_today(2),
            'cogs': 72.67,
            'acogs': 76.67,
            'ocogs': 76.67,
            'file_id': '20180727T121212.FULL'
        },
        {
            'product_id': '2',
            'region_code': 'BC99BCDF',
            'start_date': date('2018-10-09'),
            'end_date': date('2018-10-19'),
            'cogs': 23.67,
            'acogs': 46.67,
            'ocogs': 36.67,
            'file_id': '20180726T121212'
        },
        {

            'product_id': '3',
            'region_code': 'CD00CDE',
            'start_date': date_before_today(2),
            'end_date': date_after_today(200),
            'cogs': 723.67,
            'acogs': 6.67,
            'ocogs': 96.67,
            'file_id': '20180726T121212'
        },
        {

            'product_id': '3',
            'region_code': 'CD00CDE',
            'start_date': date_before_today(2),
            'end_date': date_after_today(200),
            'cogs': 71.67,
            'acogs': 6.67,
            'ocogs': 96.67,
            'file_id': '20180725T121212.FULL'
        },
        {

            'product_id': '8',
            'region_code': 'CD00CDE',
            'start_date': date_before_today(2),
            'end_date': date_after_today(200),
            'cogs': 70.67,
            'acogs': 6.67,
            'ocogs': 96.67,
            'file_id': '20180725T121212.FULL'
        },
        {

            'product_id': '3',
            'region_code': 'CD00CDE',
            'start_date': date_before_today(2),
            'end_date': date_after_today(200),
            'cogs': 77.67,
            'acogs': 6.67,
            'ocogs': 96.67,
            'file_id': '20180720T121212.FULL'
        },
        {

            'product_id': '3',
            'region_code': 'CD00CDE',
            'start_date': date_before_today(2),
            'end_date': date_after_today(200),
            'cogs': 56.67,
            'acogs': 60.67,
            'ocogs': 96.67,
            'file_id': '20180720T121212'
        },
        {
            'product_id': '4',
            'region_code': 'DE01DEF',
            'start_date': date('2018-05-05'),
            'end_date': date('2018-05-08'),
            'cogs': 23.67,
            'acogs': 6.67,
            'ocogs': 56.67,
            'file_id': '20180726T121212'
        },
        {
            'product_id': '4',
            'region_code': 'DE01DEF',
            'start_date': date('2018-05-05'),
            'end_date': date('2018-05-08'),
            'cogs': 23.67,
            'acogs': 6.67,
            'ocogs': 56.67,
            'file_id': '20180706T121212'
        }
    ]))

    expected_costs_snapshot_pd = pd.DataFrame([
        {
            'product_id': '12',
            'region_code': 'AB98ABCD',
            'start_date': date_before_today(3),
            'end_date': date_after_today(2),
            'cogs': 72.67,
            'acogs': 76.67,
            'ocogs': 76.67,
            'file_id': '20180727T121212.FULL'
        }, {
            'product_id': '14',
            'region_code': 'AB98ABCD',
            'start_date': date_before_today(3),
            'end_date': date_after_today(2),
            'cogs': 72.67,
            'acogs': 76.67,
            'ocogs': 76.67,
            'file_id': '20180727T121212.FULL'
        },
        {
            'product_id': '1',
            'region_code': 'AB98ABCD',
            'start_date': date_before_today(5),
            'end_date': date_after_today(5),
            'cogs': 72.67,
            'acogs': 76.67,
            'ocogs': 76.67,
            'file_id': '20180727T121212.FULL'
        },
        {
            'product_id': '1',
            'region_code': 'AB98ABCD',
            'start_date': date_before_today(3),
            'end_date': date_after_today(2),
            'cogs': 72.67,
            'acogs': 76.67,
            'ocogs': 76.67,
            'file_id': '20180727T121212.FULL'
        }
    ])

    expected_costs_delta_pd = pd.DataFrame([
        {
            'product_id': '12',
            'region_code': 'AB98ABCD',
            'start_date': date_before_today(3),
            'end_date': date_after_today(2),
            'cogs': 72.67,
            'acogs': 76.67,
            'ocogs': 76.67,
            'file_id': '20180727T121212.FULL'
        }, {
            'product_id': '14',
            'region_code': 'AB98ABCD',
            'start_date': date_before_today(3),
            'end_date': date_after_today(2),
            'cogs': 72.67,
            'acogs': 76.67,
            'ocogs': 76.67,
            'file_id': '20180727T121212.FULL'
        },
        {
            'product_id': '1',
            'region_code': 'AB98ABCD',
            'start_date': date_before_today(5),
            'end_date': date_after_today(5),
            'cogs': 72.67,
            'acogs': 76.67,
            'ocogs': 76.67,
            'file_id': '20180727T121212.FULL'
        },
        {
            'product_id': '1',
            'region_code': 'AB98ABCD',
            'start_date': date_before_today(3),
            'end_date': date_after_today(2),
            'cogs': 72.67,
            'acogs': 76.67,
            'ocogs': 76.67,
            'file_id': '20180727T121212.FULL'
        }
    ])

    print "input costs history"
    costs.show()

    result, bad, deltas_till_file = build_snapshot.compute_snapshot_and_bad_records(costs,
                                                                                    ["product_id", "region_code",
                                                                                     "start_date",
                                                                                     "end_date"], spark_context,
                                                                                    hive_context)
    delta = build_snapshot._generate_deltas(result, deltas_till_file, hive_context, spark_context)
    latest_snapshot_id = build_snapshot._generate_latest_snapshot_id(deltas_till_file)

    print "Result costs snapshot"
    result.show()
    print "Bad records"
    bad.show()
    print "Delta"
    delta.show()
    assert_frame_equal_with_sort(result.toPandas(), expected_costs_snapshot_pd, key_columns)
    assert_frame_equal_with_sort(delta.toPandas(), expected_costs_delta_pd, key_columns)
    assert latest_snapshot_id == "20180727T121212.SNAPSHOT"


def test_when_latest_file_is_snapshot_file(spark_context, hive_context):
    # type: (SparkContext, HiveContext) -> NoReturn

    costs = hive_context.createDataFrame(pd.DataFrame([
        {
            'product_id': '1',
            'region_code': 'AB98ABCD',
            'start_date': date_before_today(5),
            'end_date': date_after_today(5),
            'cogs': 75.67,
            'acogs': 76.67,
            'ocogs': 76.67,
            'file_id': '20180726T121212',
            'o_file_id': ''
        },
        {
            'product_id': '1',
            'region_code': 'AB98ABCD',
            'start_date': date_before_today(5),
            'end_date': date_after_today(5),
            'cogs': 72.67,
            'acogs': 76.67,
            'ocogs': 76.67,
            'file_id': '20180729T121212.SNAPSHOT',
            'o_file_id': '20180726T121212'
        },
        {
            'product_id': '1',
            'region_code': 'AB98ABCD',
            'start_date': date_before_today(3),
            'end_date': date_after_today(2),
            'cogs': 72.67,
            'acogs': 76.67,
            'ocogs': 76.67,
            'file_id': '20180729T121212.SNAPSHOT',
            'o_file_id': '20180726T121212'
        },
        {
            'product_id': '12',
            'region_code': 'AB98ABCD',
            'start_date': date_before_today(3),
            'end_date': date_after_today(2),
            'cogs': 72.67,
            'acogs': 76.67,
            'ocogs': 76.67,
            'file_id': '20180727T121212.FULL',
            'o_file_id': None
        },
        {
            'product_id': '14',
            'region_code': 'AB98ABCD',
            'start_date': date_before_today(3),
            'end_date': date_after_today(2),
            'cogs': 72.67,
            'acogs': 76.67,
            'ocogs': 76.67,
            'file_id': '20180727T121212.SNAPSHOT',
            'o_file_id': '20180726T121212'
        },
        {
            'product_id': '2',
            'region_code': 'BC99BCDF',
            'start_date': date('2018-10-09'),
            'end_date': date('2018-10-19'),
            'cogs': 23.67,
            'acogs': 46.67,
            'ocogs': 36.67,
            'file_id': '20180726T121212',
            'o_file_id': None
        },
        {

            'product_id': '3',
            'region_code': 'CD00CDE',
            'start_date': date_before_today(2),
            'end_date': date_after_today(200),
            'cogs': 723.67,
            'acogs': 6.67,
            'ocogs': 96.67,
            'file_id': '20180726T121212',
            'o_file_id': None
        },
        {

            'product_id': '3',
            'region_code': 'CD00CDE',
            'start_date': date_before_today(2),
            'end_date': date_after_today(200),
            'cogs': 71.67,
            'acogs': 6.67,
            'ocogs': 96.67,
            'file_id': '20180725T121212.FULL',
            'o_file_id': None
        },
        {

            'product_id': '8',
            'region_code': 'CD00CDE',
            'start_date': date_before_today(2),
            'end_date': date_after_today(200),
            'cogs': 70.67,
            'acogs': 6.67,
            'ocogs': 96.67,
            'file_id': '20180725T121212.FULL',
            'o_file_id': None
        },
        {

            'product_id': '3',
            'region_code': 'CD00CDE',
            'start_date': date_before_today(2),
            'end_date': date_after_today(200),
            'cogs': 77.67,
            'acogs': 6.67,
            'ocogs': 96.67,
            'file_id': '20180720T121212.FULL',
            'o_file_id': None
        },
        {

            'product_id': '3',
            'region_code': 'CD00CDE',
            'start_date': date_before_today(2),
            'end_date': date_after_today(200),
            'cogs': 56.67,
            'acogs': 60.67,
            'ocogs': 96.67,
            'file_id': '20180720T121212',
            'o_file_id': None
        },
        {
            'product_id': '4',
            'region_code': 'DE01DEF',
            'start_date': date('2018-05-05'),
            'end_date': date('2018-05-08'),
            'cogs': 23.67,
            'acogs': 6.67,
            'ocogs': 56.67,
            'file_id': '20180726T121212',
            'o_file_id': None
        },
        {
            'product_id': '4',
            'region_code': 'DE01DEF',
            'start_date': date('2018-05-05'),
            'end_date': date('2018-05-08'),
            'cogs': 23.67,
            'acogs': 6.67,
            'ocogs': 56.67,
            'file_id': '20180706T121212',
            'o_file_id': None
        }
    ]))
    print "input costs history"
    costs.show()
    with pytest.raises(SystemExit) as pytest_wrapped_e:
        build_snapshot.compute_snapshot_and_bad_records(costs,
                                                        ["product_id", "region_code",
                                                         "start_date",
                                                         "end_date"], spark_context,
                                                        hive_context)
    assert pytest_wrapped_e.type == SystemExit
    assert pytest_wrapped_e.value.code == 0


def test_when_latest_file_is_delta_file_after_recent_snapshot(spark_context, hive_context):
    # type: (SparkContext, HiveContext) -> NoReturn

    costs = hive_context.createDataFrame(pd.DataFrame([
        {
            'product_id': '1',
            'region_code': 'AB98ABCD',
            'start_date': date_before_today(5),
            'end_date': date_after_today(5),
            'cogs': 75.67,
            'acogs': 76.67,
            'ocogs': 76.67,
            'file_id': '20180726T121212'
        },
        {
            'product_id': '1',
            'region_code': 'AB98ABCD',
            'start_date': date_before_today(5),
            'end_date': date_after_today(5),
            'cogs': 72.67,
            'acogs': 76.67,
            'ocogs': 76.67,
            'file_id': '20180727T121212'
        },
        {
            'product_id': '1',
            'region_code': 'AB98ABCD',
            'start_date': date_before_today(3),
            'end_date': date_after_today(2),
            'cogs': 72.67,
            'acogs': 76.67,
            'ocogs': 76.67,
            'file_id': '20180727T121212'
        },
        {
            'product_id': '12',
            'region_code': 'AB98ABCD',
            'start_date': date_before_today(3),
            'end_date': date_after_today(2),
            'cogs': 72.67,
            'acogs': 76.67,
            'ocogs': 76.67,
            'file_id': '20180727T121212'
        },
        {
            'product_id': '14',
            'region_code': 'AB98ABCD',
            'start_date': date_before_today(3),
            'end_date': date_after_today(2),
            'cogs': 72.67,
            'acogs': 76.67,
            'ocogs': 76.67,
            'file_id': '20180727T121212'
        },
        {
            'product_id': '2',
            'region_code': 'BC99BCDF',
            'start_date': date('2018-10-09'),
            'end_date': date('2018-10-19'),
            'cogs': 23.67,
            'acogs': 46.67,
            'ocogs': 36.67,
            'file_id': '20180726T121212'
        },
        {

            'product_id': '3',
            'region_code': 'CD00CDE',
            'start_date': date_before_today(2),
            'end_date': date_after_today(200),
            'cogs': 723.67,
            'acogs': 6.67,
            'ocogs': 96.67,
            'file_id': '20180726T121212'
        },
        {

            'product_id': '3',
            'region_code': 'CD00CDE',
            'start_date': date_before_today(2),
            'end_date': date_after_today(200),
            'cogs': 71.67,
            'acogs': 6.67,
            'ocogs': 96.67,
            'file_id': '20180725T121212.SNAPSHOT'
        },
        {

            'product_id': '8',
            'region_code': 'CD00CDE',
            'start_date': date_before_today(2),
            'end_date': date_after_today(200),
            'cogs': 70.67,
            'acogs': 6.67,
            'ocogs': 96.67,
            'file_id': '20180725T121212.SNAPSHOT'
        },
        {

            'product_id': '3',
            'region_code': 'CD00CDE',
            'start_date': date_before_today(2),
            'end_date': date_after_today(200),
            'cogs': 77.67,
            'acogs': 6.67,
            'ocogs': 96.67,
            'file_id': '20180720T121212.FULL'
        },
        {

            'product_id': '3',
            'region_code': 'CD00CDE',
            'start_date': date_before_today(2),
            'end_date': date_after_today(200),
            'cogs': 56.67,
            'acogs': 60.67,
            'ocogs': 96.67,
            'file_id': '20180720T121212'
        },
        {
            'product_id': '4',
            'region_code': 'DE01DEF',
            'start_date': date('2018-05-05'),
            'end_date': date('2018-05-08'),
            'cogs': 23.67,
            'acogs': 6.67,
            'ocogs': 56.67,
            'file_id': '20180726T121212'
        },
        {
            'product_id': '4',
            'region_code': 'DE01DEF',
            'start_date': date('2018-05-05'),
            'end_date': date('2018-05-08'),
            'cogs': 23.67,
            'acogs': 6.67,
            'ocogs': 56.67,
            'file_id': '20180706T121212'
        }
    ]))

    expected_costs_snapshot_pd = pd.DataFrame([
        {
            'product_id': '12',
            'region_code': 'AB98ABCD',
            'start_date': date_before_today(3),
            'end_date': date_after_today(2),
            'cogs': 72.67,
            'acogs': 76.67,
            'ocogs': 76.67,
            'file_id': '20180727T121212'
        },
        {
            'product_id': '14',
            'region_code': 'AB98ABCD',
            'start_date': date_before_today(3),
            'end_date': date_after_today(2),
            'cogs': 72.67,
            'acogs': 76.67,
            'ocogs': 76.67,
            'file_id': '20180727T121212'
        },
        {
            'product_id': '2',
            'region_code': 'BC99BCDF',
            'start_date': date('2018-10-09'),
            'end_date': date('2018-10-19'),
            'cogs': 23.67,
            'acogs': 46.67,
            'ocogs': 36.67,
            'file_id': '20180726T121212'
        },
        {

            'product_id': '3',
            'region_code': 'CD00CDE',
            'start_date': date_before_today(2),
            'end_date': date_after_today(200),
            'cogs': 723.67,
            'acogs': 6.67,
            'ocogs': 96.67,
            'file_id': '20180726T121212'
        },
        {

            'product_id': '8',
            'region_code': 'CD00CDE',
            'start_date': date_before_today(2),
            'end_date': date_after_today(200),
            'cogs': 70.67,
            'acogs': 6.67,
            'ocogs': 96.67,
            'file_id': '20180725T121212.SNAPSHOT'
        },
        {
            'product_id': '4',
            'region_code': 'DE01DEF',
            'start_date': date('2018-05-05'),
            'end_date': date('2018-05-08'),
            'cogs': 23.67,
            'acogs': 6.67,
            'ocogs': 56.67,
            'file_id': '20180726T121212'
        },
        {
            'product_id': '1',
            'region_code': 'AB98ABCD',
            'start_date': date_before_today(5),
            'end_date': date_after_today(5),
            'cogs': 72.67,
            'acogs': 76.67,
            'ocogs': 76.67,
            'file_id': '20180727T121212'
        },
        {
            'product_id': '1',
            'region_code': 'AB98ABCD',
            'start_date': date_before_today(3),
            'end_date': date_after_today(2),
            'cogs': 72.67,
            'acogs': 76.67,
            'ocogs': 76.67,
            'file_id': '20180727T121212'
        },
    ])

    expected_costs_delta_pd = pd.DataFrame([
        {
            'product_id': '12',
            'region_code': 'AB98ABCD',
            'start_date': date_before_today(3),
            'end_date': date_after_today(2),
            'cogs': 72.67,
            'acogs': 76.67,
            'ocogs': 76.67,
            'file_id': '20180727T121212'
        }, {
            'product_id': '14',
            'region_code': 'AB98ABCD',
            'start_date': date_before_today(3),
            'end_date': date_after_today(2),
            'cogs': 72.67,
            'acogs': 76.67,
            'ocogs': 76.67,
            'file_id': '20180727T121212'
        },
        {
            'product_id': '2',
            'region_code': 'BC99BCDF',
            'start_date': date('2018-10-09'),
            'end_date': date('2018-10-19'),
            'cogs': 23.67,
            'acogs': 46.67,
            'ocogs': 36.67,
            'file_id': '20180726T121212'
        },
        {

            'product_id': '3',
            'region_code': 'CD00CDE',
            'start_date': date_before_today(2),
            'end_date': date_after_today(200),
            'cogs': 723.67,
            'acogs': 6.67,
            'ocogs': 96.67,
            'file_id': '20180726T121212'
        },
        {
            'product_id': '4',
            'region_code': 'DE01DEF',
            'start_date': date('2018-05-05'),
            'end_date': date('2018-05-08'),
            'cogs': 23.67,
            'acogs': 6.67,
            'ocogs': 56.67,
            'file_id': '20180726T121212'
        },
        {
            'product_id': '1',
            'region_code': 'AB98ABCD',
            'start_date': date_before_today(5),
            'end_date': date_after_today(5),
            'cogs': 72.67,
            'acogs': 76.67,
            'ocogs': 76.67,
            'file_id': '20180727T121212'
        },
        {
            'product_id': '1',
            'region_code': 'AB98ABCD',
            'start_date': date_before_today(3),
            'end_date': date_after_today(2),
            'cogs': 72.67,
            'acogs': 76.67,
            'ocogs': 76.67,
            'file_id': '20180727T121212'
        },
    ])

    print "input costs history"
    costs.show()

    result, bad, deltas_till_file = build_snapshot.compute_snapshot_and_bad_records(costs,
                                                                                    ["product_id", "region_code",
                                                                                     "start_date",
                                                                                     "end_date"], spark_context,
                                                                                    hive_context)
    delta = build_snapshot._generate_deltas(result, deltas_till_file, hive_context, spark_context)
    latest_snapshot_id = build_snapshot._generate_latest_snapshot_id(deltas_till_file)

    print "Result costs snapshot"
    result.show()
    print "Bad records"
    bad.show()
    print "Delta"
    delta.show()
    assert_frame_equal_with_sort(result.toPandas(), expected_costs_snapshot_pd, key_columns)
    assert_frame_equal_with_sort(delta.toPandas(), expected_costs_delta_pd, key_columns)
    assert latest_snapshot_id == '20180727T121212.SNAPSHOT'
