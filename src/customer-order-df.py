from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

spark = SparkSession.builder.appName("TotalCustomerSpent").getOrCreate()

schema = StructType([
    StructField("customerID", IntegerType(), False),
    StructField("itemID", IntegerType(), False),
    StructField("amount", FloatType(), False)])


orders = spark.read.schema(schema).csv("file:///Users/Gulzar/Development/Spark/Spark-course/src/resources/customer-orders.csv")

print("Here is our inferred schema:")
orders.printSchema()

orders.groupBy(orders.customerID).agg(func.round(func.sum(orders.amount), 2).alias("total-spent"))\
    .sort("total-spent").show(orders.count())


spark.stop()

