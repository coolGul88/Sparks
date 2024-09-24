from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("CustomerOrder")
sc = SparkContext(conf=conf)


def parse_line(line):
    fields = line.split(',')
    return int(fields[0]), float(fields[2])


lines = sc.textFile("file:////Users/Gulzar/Development/Spark/Spark-course/src/resources/customer-orders.csv")
parsedLines = lines.map(parse_line)
orders = parsedLines.reduceByKey(lambda x, y: float(x +y)).sortByKey()
results = orders.collect()

for result in results:
    print(result)
