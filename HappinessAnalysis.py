from pyspark import SparkConf, SparkContext
import re

DATA_POINTS = ["Country", "Region", "Score"]
YEARS = [2015, 2016, 2017, 2018, 2019]
FILE_DIRECTORY = "./dataFiles/"
FILE_EXTENSION = ".csv"
UNWANTED_PATTERN = r'\".*?\,.*?\"'


# method extract country name, region & happiness score
def parseLine(line, columns):

    if re.search(UNWANTED_PATTERN, line):
        line = str(re.sub('"', "", line))
        columns[1] += 1

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
# 2017 -> no Region & countries surrounded by ""
# 2018 & 2019 -> Country or Region
def cleanFile(lines):
    header = lines.first().split(',')
    columns = []

    for index, columnName in enumerate(header):
        for dataPoint in DATA_POINTS:
            if dataPoint in columnName:
                columns.append(index)

    return lines.map(lambda row: parseLine(row, columns)).filter(lambda x: DATA_POINTS[0] not in x[0])


def flipKeyValue(line):
    if len(line) == 3:
        return line[2], line[1], line[0]
    elif len(line) == 2:
        return line[1], line[0]


# Create Spark context and name the project
conf = SparkConf().setMaster("local").setAppName("HappinessProjectSPARK")
sc = SparkContext(conf=conf)

# LOAD DATA
happinessRDDs = []
for year in YEARS:
    happinessRDDs.append((year, cleanFile(sc.textFile(FILE_DIRECTORY + str(year) + FILE_EXTENSION))))


# FIRST ANALYSIS: Happiest country for each year
for year, rdd in happinessRDDs:

    alphabetically = rdd.sortByKey()
    flipped = rdd.map(flipKeyValue)
    sortedRDD = flipped.sortByKey().map(flipKeyValue)
    sortedCountries = sortedRDD.collect()
    print "YEAR:", year, "- Happiest Country:", sortedCountries[-1][0], "Score:", sortedCountries[-1][-1]


# # happiest year of a country:
# happiest = flipped.reduceByKey(lambda x, y: max(x,y))
# result = happiest.collect()
# #print(result)

