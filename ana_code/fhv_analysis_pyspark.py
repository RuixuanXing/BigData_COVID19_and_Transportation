# Analysis 5
# use spark to get mean of two specific period
df = spark.read.csv("{YOUR_FHV_OUTPUT_PATH}/part-r-00000")
df = df.withColumnRenamed("_c0", "date")
df = df.withColumnRenamed("_c1", "trips")

# mean number of trips from 2020-03-1 to 2020-05-01" as the covid begins
df.filter((df["date"] >= "2020-03-01") & (df["date"] <= "2020-05-01")).summary().show()

# mean number of trips from 2020-11-01 to 2020-02-01" as the covid begins
df.filter((df["date"] >= "2020-11-01") & (df["date"] <= "2021-02-01")).summary().show()
# found people are not so sensible to take ubers in the second breakout

# mean number of trips from 2020-03-1 to 2020-05-01" as the covid begins
df.filter((df["date"] >= "2020-03-01") & (df["date"] <= "2020-05-01")).summary().show()

# Analysis 6
# percent of trips decrease in the first breakout from 03-01 to 03-31
df = df.withColumn("trips", df.trips.cast('int'))
prev = df.filter((df["date"] == "2020-03-01")).select("trips").collect()[0].asDict()["trips"]
aftr = df.filter((df["date"] == "2020-04-01")).select("trips").collect()[0].asDict()["trips"]

percent = float(aftr - prev) / prev
percent