# Processing DataFrame
df = spark.read.csv("output/merged/project.csv")

df = df.withColumnRenamed("_c0","date")
df = df.withColumnRenamed("_c1","new_covid")
df = df.withColumnRenamed("_c2","new_trips")
df = df.withColumnRenamed("_c3","new_mta")

df = df.withColumn("new_covid", df.new_covid.cast('int'))
df = df.withColumn("new_trips", df.new_trips.cast('int'))
df = df.withColumn("new_mta", df.new_mta.cast('int'))

# Analysis 1: Correlation Index Between Columns (All Time Range)
df.corr("new_trips", "new_mta")
df.corr("new_covid", "new_mta")
df.corr("new_covid", "new_trips")

# Analysis 2: Correlation Index Between Columns (2020-03-01 TO 2020-04-31)
df.filter((df["date"] >= "2020-03-01") & (df["date"] <= "2020-04-31")).corr("new_covid", "new_mta")
df.filter((df["date"] >= "2020-03-01") & (df["date"] <= "2020-04-31")).corr("new_covid", "new_trips")

# Analysis 3: Correlation Index Between Columns (2020-05-01 TO 2020-10-31)
df.filter((df["date"] >= "2020-05-01") & (df["date"] <= "2020-10-31")).corr("new_covid", "new_mta")
df.filter((df["date"] >= "2020-05-01") & (df["date"] <= "2020-10-31")).corr("new_covid", "new_trips")

# Analysis 4: Correlation Index Between Columns (2020-11-01 TO 2021-04-31)
df.filter((df["date"] >= "2020-11-01") & (df["date"] <= "2021-04-31")).corr("new_covid", "new_mta")
df.filter((df["date"] >= "2020-11-01") & (df["date"] <= "2021-04-31")).corr("new_covid", "new_trips")

# Analysis 5
import numpy as np
from pyspark.sql.types import *
import pyspark.sql.functions as F

df = df.withColumn("date", df['date'].cast(DateType()))
df = df.withColumn("date_timestamp", F.unix_timestamp(df["date"]))
df.show()

x = df.filter((df["date"] >= "2020-05-01") & (df["date"] <= "2020-10-31")).toPandas()["date_timestamp"].values.tolist()

y_new_trips = df.filter((df["date"] >= "2020-05-01") & (df["date"] <= "2020-10-31")).toPandas()["new_trips"].values.tolist()

np.polyfit(x, y_new_trips, 1)

y_new_mta = df.filter((df["date"] >= "2020-05-01") & (df["date"] <= "2020-10-31")).toPandas()["new_mta"].values.tolist()

np.polyfit(x, y_new_mta, 1)
