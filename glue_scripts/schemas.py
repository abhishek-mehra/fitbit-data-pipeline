from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.types import *


calories_daily_schema = StructType([
    StructField("participantidentifier", StringType(), True),
    StructField("date", DateType(), True),
    StructField("value", DoubleType(), True)
])

calories_intraday_schema = StructType([
    StructField("participantidentifier", StringType(), True),
    StructField("datetime", TimestampType(), True),
    StructField("value", DoubleType(), True),
    StructField("level", IntegerType(), True),
    StructField("mets", DoubleType(), True)
])

distance_daily_schema = StructType([
    StructField("participantidentifier", StringType(), True),
    StructField("date", DateType(), True),
    StructField("value", DoubleType(), True)
])

distance_intraday_schema = StructType([
    StructField("participantidentifier", StringType(), True),
    StructField("datetime", TimestampType(), True),
    StructField("value", DoubleType(), True)
])

elevation_daily_schema = StructType([
    StructField("participantidentifier", StringType(), True),
    StructField("date", DateType(), True),
    StructField("value", DoubleType(), True)
])

elevation_intraday_schema = StructType([
    StructField("participantidentifier", StringType(), True),
    StructField("datetime", TimestampType(), True),
    StructField("value", DoubleType(), True)
])

floors_daily_schema = StructType([
    StructField("participantidentifier", StringType(), True),
    StructField("date", DateType(), True),
    StructField("value", DoubleType(), True)
])

floors_intraday_schema = StructType([
    StructField("participantidentifier", StringType(), True),
    StructField("datetime", TimestampType(), True),
    StructField("value", DoubleType(), True)
])

heart_zones_schema = StructType([
    StructField("participantidentifier", StringType(), True),
    StructField("date", DateType(), True),
    StructField("zone_name", StringType(), True),
    StructField("calories_out", DoubleType(), True),
    StructField("min_heart_rate", IntegerType(), True),
    StructField("max_heart_rate", IntegerType(), True),
    StructField("minutes", DoubleType(), True)
])

heart_intraday_schema = StructType([
    StructField("participantidentifier", StringType(), True),
    StructField("datetime", TimestampType(), True),
    StructField("value", DoubleType(), True)
])

steps_daily_schema = StructType([
    StructField("participantidentifier", StringType(), True),
    StructField("date", DateType(), True),
    StructField("value", DoubleType(), True)
])

steps_intraday_schema = StructType([
    StructField("participantidentifier", StringType(), True),
    StructField("datetime", TimestampType(), True),
    StructField("value", DoubleType(), True)
])

# Schema mapping for easy access
schema_mapping = {
    'calories': {
        'daily': calories_daily_schema,
        'intraday': calories_intraday_schema
    },
    'distance': {
        'daily': distance_daily_schema,
        'intraday': distance_intraday_schema
    },
    'elevation': {
        'daily': elevation_daily_schema,
        'intraday': elevation_intraday_schema
    },
    'floors': {
        'daily': floors_daily_schema,
        'intraday': floors_intraday_schema
    },
    'heart': {
        'daily': heart_zones_schema,
        'intraday': heart_intraday_schema
    },
    'steps': {
        'daily': steps_daily_schema,
        'intraday': steps_intraday_schema
    }
} 