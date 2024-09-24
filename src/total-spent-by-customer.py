from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("SpendByCustomer")
sc = SparkContext(conf=conf)


def extractCustomerPricePairs(line):
    fields = line.split(',')
    return int(fields[0]), float(fields[2])


orders = sc.textFile("file:///Users/Gulzar/Development/Spark/Spark-course/src/resources/customer-orders.csv")
mappedInput = orders.map(extractCustomerPricePairs)
totalByCustomer = mappedInput.reduceByKey(lambda x, y: x + y).map(lambda x: (x[1], x[0])).sortByKey()

results = totalByCustomer.collect()
for result in results:
    print(result)
