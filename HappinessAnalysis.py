from pyspark import SparkConf, SparkContext
import re

DATA_POINTS = ["Country", "Region", "Score"]

# method extract country name, region & happiness score
def parseLine(line, columns):
    fields = line.split(',')
    result = []

    for index, field in enumerate(fields):
        if index in columns:
            result.append(field)

    for index, element in enumerate(result):
        if re.search("/d", element):
            result[index] = float(element)
        else:
            result[index] = str(re.sub('"', "", element))

    return result

# Since not all files are structured the same way we have to organize them:
def cleanFile(lines):
    header = lines.first().split(',')
    columns = []

    for index, columnName in enumerate(header):
        for dataPoint in DATA_POINTS:
            if dataPoint in columnName:
                columns.append(index)

    return lines.map(lambda row: parseLine(row, columns)).filter(lambda x: x[0] != DATA_POINTS[0])


def flipKeyValue(line):
    if len(line) == 3:
        return line[2], line[1], line[0]
    elif len(line) == 2:
        return line[1], line[0]

# Create Spark context and name the project
conf = SparkConf().setMaster("local").setAppName("HappinessProjectSPARK")
sc = SparkContext(conf=conf)

# 2017 -> no Region
# 2018 & 2019 -> Country or Region

# LOAD DATA
data_files = {
    "2015" : cleanFile(sc.textFile("./dataFiles/2015.csv")),
    "2016" : cleanFile(sc.textFile("./dataFiles/2016.csv")),
    "2017" : cleanFile(sc.textFile("./dataFiles/2017.csv")),
    "2018" : cleanFile(sc.textFile("./dataFiles/2018.csv")),
    "2019" : cleanFile(sc.textFile("./dataFiles/2019.csv"))
}


# FIRST ANALYSIS: Happiest country for each year
for year, rdd in data_files.items():

    alphabetically = rdd.sortByKey()
    # flipped = rdd.map(flipKeyValue)
    # sortedRDD = flipped.sortByKey().map(flipKeyValue)
    results = alphabetically.collect()
    print(year)
    #print(results[-1])

    for result in results:
        print(result)



# # happiest year of a country:
# happiest = flipped.reduceByKey(lambda x, y: max(x,y))
# result = happiest.collect()
# #print(result)

