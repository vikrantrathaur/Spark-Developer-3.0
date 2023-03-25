# Databricks notebook source
import datetime
users = [(1,
  'Corrie',
  'Van den Oord',
  'cvandenoord0@etsy.com',
  True,
  1000.55,
  datetime.date(2021, 1, 15),
  datetime.datetime(2021, 2, 10, 1, 15)),
 (2,
  'Nikolaus',
  'Brewitt',
  'nbrewitt1@dailymail.co.uk',
  True,
  900.0,
  datetime.date(2021, 2, 14),
  datetime.datetime(2021, 2, 18, 3, 33)),
 (3,
  'Orelie',
  'Penney',
  'openney2@vistaprint.com',
  True,
  850.55,
  datetime.date(2021, 1, 21),
  datetime.datetime(2021, 3, 15, 15, 16, 55)),
 (4,
  'Ashby',
  'Maddocks',
  'amaddocks3@home.pl',
  False,
  None,
  None,
  datetime.datetime(2021, 4, 10, 17, 45, 30)),
 (5,
  'Kurt',
  'Rome',
  'krome4@shutterfly.com',
  False,
  None,
  None,
  datetime.datetime(2021, 4, 2, 0, 55, 18))]

# COMMAND ----------

from pyspark.sql.types import *

# COMMAND ----------

users_schema = StructType([
    StructField('id', IntegerType()),
    StructField('first_name', StringType()),
    StructField('last_name', StringType()),
    StructField('email', StringType()),
    StructField('is_customer', BooleanType()),
    StructField('amount_paid', FloatType()),
    StructField('customer_from', DateType()),
    StructField('last_updated_ts', TimestampType())
])

# COMMAND ----------

type(users_schema)

# COMMAND ----------

spark.createDataFrame(users, schema=users_schema)

# COMMAND ----------

spark.createDataFrame(users, schema=users_schema).show()

# COMMAND ----------

spark.createDataFrame(users, schema=users_schema).rdd.collect()

# COMMAND ----------


