from pyspark.sql import SparkSession
from pyspark.sql import functions as func

spark = SparkSession.builder.appName("SparkSQL").getOrCreate()

people = spark.read.option("header", "true").option("inferSchema", "true")\
    .csv("file:///Users/Gulzar/Development/Spark/Spark-course/src/resources/fakefriends-header.csv")

print("Here is our inferred schema:")
people.printSchema()

friendsByAge = people.select("age", "friends")

print("Average friends for each age")
friendsByAge.groupBy("age").avg("friends").show()

print("Average friends for each age sorted")
friendsByAge.groupBy("age").avg("friends").sort("age").show()


print("Average friends for each age sorted and formatted")
friendsByAge.groupBy("age").agg(func.round(func.avg("friends"), 2)).sort("age").show()

print("Average friends for each age sorted and formatted and aliased")
friendsByAge.groupBy("age").agg(func.round(func.avg("friends"), 2).alias("avg-friends")).sort("age").show()

spark.stop()

