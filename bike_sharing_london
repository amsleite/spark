from pyspark.sql import SparkSession
session = SparkSession.builder.appName("BikeShareLondon").getOrCreate()
dataFrameReader = session.read
responses = dataFrameReader \
.option("header", "true") \
.option("inferSchema", value = True) \
.csv("gs://dataproc-79793f81-6176-4a53-b4a2-9dec5309e7ab-us-central1/london_merged.csv")
print("=== Print out schema ===")
responses.printSchema()
responseWithSelectedColumns = responses.select("is_weekend", "season","timestamp","is_holiday","wind_speed")
print("=== Print the selected columns of the table is_weekend,season===")
responseWithSelectedColumns.show()
print("=== Print records where the response is from date 2015-01-04 at 01 AM ===")
responseWithSelectedColumns.filter(responseWithSelectedColumns["timestamp"] == "2015-01-04 01:00:00").show()
print("=== Print the count of flag holiday ===")
groupedData = responseWithSelectedColumns.groupBy("is_holiday")
groupedData.count().show()
print("=== Print records with wind speed higher than 8")
responseWithSelectedColumns.filter(responseWithSelectedColumns["wind_speed"] > 8).show()
session.stop()
