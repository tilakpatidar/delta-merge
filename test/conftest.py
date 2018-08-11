# coding=utf-8
import datetime

import findspark
from pandas.util.testing import assert_frame_equal

findspark.init()

import logging
import pytest

from pyspark import HiveContext
from pyspark import SparkConf
from pyspark import SparkContext
from pyspark.streaming import StreamingContext


def quiet_py4j():
    """ turn down spark logging for the test context """
    logger = logging.getLogger('py4j')
    logger.setLevel(logging.WARN)


@pytest.fixture(scope="session")
def spark_context(request):
    """ fixture for creating a spark context
    Args:
        request: pytest.FixtureRequest object
    """
    conf = (SparkConf().setMaster("local[2]").setAppName("pytest-pyspark-local-testing"))
    sc = SparkContext(conf=conf)
    request.addfinalizer(lambda: sc.stop())

    quiet_py4j()
    return sc


@pytest.fixture(scope="session")
def hive_context(spark_context):
    """  fixture for creating a Hive Context. Creating a fixture enables it to be reused across all
        tests in a session
    Args:
        spark_context: spark_context fixture
    Returns:
        HiveContext for tests
    """
    return HiveContext(spark_context)


@pytest.fixture(scope="session")
def streaming_context(spark_context):
    return StreamingContext(spark_context, 1)


def assert_frame_equal_with_sort(results, expected, keycolumns):
    results_sorted = results.sort_values(by=keycolumns).reset_index(drop=True).sort_index(axis=1)
    expected_sorted = expected.sort_values(by=keycolumns).reset_index(drop=True).sort_index(axis=1)
    assert_frame_equal(results_sorted, expected_sorted, check_index_type=False)


def date(date_str):
    return datetime.datetime.strptime(date_str, "%Y-%m-%d").date()


def date_before_today(delta):
    return (datetime.datetime.now() - datetime.timedelta(days=delta)).date()


def date_after_today(delta):
    return (datetime.datetime.now() + datetime.timedelta(days=delta)).date()


def _today_year():
    return datetime.datetime.now().date().year


def _today_month():
    return datetime.datetime.now().date().month
