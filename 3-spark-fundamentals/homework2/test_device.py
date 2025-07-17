from chispa.dataframe_comparer import *
from ..jobs.device import do_dedup_events_transformation
from collections import namedtuple

deviceDetail = namedtuple("deviceDetail","identifier type properties")
device = namedtuple("device","USER_ID DEVICE_ID HOST EVENT_TIME")
                          

def test_device_transformation(spark):

    source_data=[
        device(1,22,"Zach",1234567890),
        device(1,22,"Zach",1234567890),
    ]

    source_df=spark.createDataFrame(source_data)

    actual_df=do_dedup_events_transformation(spark,source_df)

    # The expected output should have the same schema as the input, but with duplicates removed.
    expected_output = [
        device(1, 22, "Zach", 1234567890)
    ]

    expected_df = spark.createDataFrame(expected_output)
    assert_df_equality(actual_df, expected_df, ignore_nullable=True)