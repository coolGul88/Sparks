from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

spark = SparkSession.builder.appName("MostPopularSuperhero").getOrCreate()

schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True)])

names = spark.read.schema(schema).option("sep", " ").csv(
    "file:///Users/Gulzar/Development/Spark/Spark-course/src/resources/MarvelNames.txt")

lines = spark.read.text("file:///Users/Gulzar/Development/Spark/Spark-course/src/resources/MarvelGraph.txt")

# Small tweak vs. what's shown in the video: we trim each line of whitespace as that could
# throw off the counts.
connections = lines.withColumn("id", func.split(func.trim(func.col("value")), " ")[0]) \
    .withColumn("connections", func.size(func.split(func.trim(func.col("value")), " ")) - 1) \
    .groupBy("id").agg(func.sum("connections").alias("connections"))

minConnectionCnt = connections.agg(func.min("connections")).first()[0]

mostObscure = connections.filter(func.col("connections") == minConnectionCnt)

mostObscureNames = mostObscure.join(names, "id").select("name")

print(mostObscureNames.show(mostObscureNames.count()))
