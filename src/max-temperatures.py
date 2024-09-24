from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("MaxTemperatures")
sc = SparkContext(conf=conf)


def parse_line(line):
    fields = line.split(',')
    station_id = fields[0]
    entry_type = fields[2]
    temperature = float(fields[3]) * 0.1 * (9.0 / 5.0) + 32.0
    return station_id, entry_type, temperature


lines = sc.textFile("file:///Users/Gulzar/Development/Spark/Spark-course/src/resources/1800.csv")
parsedLines = lines.map(parse_line)
maxTemps = parsedLines.filter(lambda x: "TMAX" in x[1])
stationTemps = maxTemps.map(lambda x: (x[0], x[2]))
maxTemps = stationTemps.reduceByKey(lambda x, y: max(x, y))
results = maxTemps.collect()

for result in results:
    print(result[0] + "\t{:.2f}F".format(result[1]))
