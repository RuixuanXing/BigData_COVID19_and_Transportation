# Analysis 1: Difference in Max Value of Two Major Waves of Covid
df_c = spark.read.csv("{YOUR_COVID_OUTPUT_PATH_2}/whole.csv")

df_c = df_c.withColumnRenamed("_c0","date")
df_c = df_c.withColumnRenamed("_c1","county")
df_c = df_c.withColumnRenamed("_c2","new_positive")

df_c = df_c.withColumn("new_positive", df_c.new_positive.cast('int'))

df_c.filter((df_c["date"] >= "2020-03-01") & (df_c["date"] <= "2020-04-31") & (df_c["county"] == "New York")).describe("new_positive").filter("summary == 'max'").select("new_positive").show()

df_c.filter((df_c["date"] >=  "2020-11-01") & (df_c["date"] <= "2021-04-31") & (df_c["county"] == "New York")).describe("new_positive").filter("summary == 'max'").select("new_positive").show()

# Analysis 2: Percent Increase of New Positive in Nassau and Kings During December 2020
df_1 = spark.read.csv("{YOUR_COVID_OUTPUT_PATH_2}/whole.csv")

df_1 = df_1.withColumnRenamed("_c0","test_date")
df_1 = df_1.withColumnRenamed("_c1","county_name")
df_1 = df_1.withColumnRenamed("_c2","new_cases")
df_1 = df_1.withColumnRenamed("_c3","agg_cases")
df_1 = df_1.withColumnRenamed("_c4","test_performed")
df_1 = df_1.withColumnRenamed("_c5","agg_test_performed")

df_1 = df_1.withColumn("new_cases", df_1.new_cases.cast('int'))
df_1 = df_1.withColumn("agg_cases", df_1.agg_cases.cast('int'))

prev1 = int(df_1.filter((df_1["test_date"] == "2020-12-01") & (df_1["county_name"] == "Nassau")).select("agg_cases").collect()[0].asDict()["agg_cases"])

aft1 = int(df_1.filter((df_1["test_date"] == "2020-12-31") & (df_1["county_name"] == "Nassau")).select("agg_cases").collect()[0].asDict()["agg_cases"])

perInc1 = (aft1 - prev1) / prev1

prev2 = int(df_1.filter((df_1["test_date"] == "2020-12-01") & (df_1["county_name"] == "Kings")).select("agg_cases").collect()[0].asDict()["agg_cases"])

aft2 = int(df_1.filter((df_1["test_date"] == "2020-12-31") & (df_1["county_name"] == "Kings")).select("agg_cases").collect()[0].asDict()["agg_cases"])

perInc2 = (aft2 - prev2) / prev2

(perInc1, perInc2)

# Analysis 3: Average Increasing Rate of New Positive in Nassau and Kings During December 2020
aveRate1 = (aft1 - prev1)/31
aveRate2 = (aft2 - prev2)/31

(aveRate1, aveRate2)

# Analysis 4: Variation in NYS Counties, mean and standard deviation
from pyspark.sql.functions import sqrt

df_1.filter((df_1["test_date"] >= "2020-12-01") & (df_1["test_date"] <= "2020-12-31")).groupBy("county_name").mean("new_cases").show()

df_1.filter((df_1["test_date"] >= "2020-12-01") & (df_1["test_date"] <= "2020-12-31")).groupBy("county_name").agg({"new_cases": "stddev"}).show()