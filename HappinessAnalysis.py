from pyspark import SparkConf, SparkContext
import re

import collections

DATA_POINTS = ["Country", "Region", "Score"]
YEARS = [2015, 2016, 2017, 2018, 2019]
FILE_DIRECTORY = "./dataFiles/"
FILE_EXTENSION = ".csv"
UNWANTED_PATTERN = r'\".*?\,.*?\"'
COUNTRY_NAME_POS = 0
REGION_NAME_POS = 1

SCORE_NUM_POS = 1
YEAR_POS = 0


# Method to extract country name, region & happiness score
def parseLine(line, columns, year):
    indexes = list(columns)
    if re.search(UNWANTED_PATTERN, line):
        indexes[1] = indexes[1] + 1

    line = str(re.sub('"', "", line))
    fields = line.split(',')
    key = ""
    values = []

    for index, field in enumerate(fields):
        if index in indexes:
            if not key:
                key = field
            else:
                values.append(field)

    for index, element in enumerate(values):
        if element.replace('.', '', 1).isdigit():
            values[index] = ((int(year), float(element)),)
        else:
            values[index] = element

    if len(values) == 1:
        values.insert(0, "")

    return key, tuple(values)


# Since not all files are structured the same way we have to organize them:
# 2017 -> no Region & countries surrounded by ""
# 2018 & 2019 -> Country or Region
def cleanFile(lines, year):
    header = lines.first().split(',')
    columns = []

    for index, columnName in enumerate(header):
        for dataPoint in DATA_POINTS:
            if dataPoint in columnName:
                columns.append(index)

    return lines.map(lambda row: parseLine(row, columns, year)).filter(lambda x: DATA_POINTS[0] not in x[0])


# Method to flip without mattering whether it has a "Region" value or not
def flipKeyValue(line):
    return line[1][1][0][1], (line[1][0], ((line[1][1][0][0], line[0]), ))


def join(tuple1, tuple2):
    if isinstance(tuple1[0], int):
        tuple1 = (tuple(tuple1), ) + tuple2
    else:
        tuple1 = tuple1+tuple2
    return tuple1

def findYearScore(yearAndScore, targetYear):
    for value in yearAndScore:
        if isinstance(value, int):
            return yearAndScore[1]
        if value[0] == targetYear:
            return value[1]

    return -1


# Create Spark context and name the project
conf = SparkConf().setMaster("local").setAppName("HappinessProjectSPARK")
sc = SparkContext(conf=conf)

# LOAD DATA
happinessRDDs = []
for year in YEARS:
    happinessRDDs.append((year, cleanFile(sc.textFile(FILE_DIRECTORY + str(year) + FILE_EXTENSION), year)))

# FIRST ANALYSIS: Happiest country for each year
# for year, rdd in happinessRDDs:
#     r = rdd.collect()
#     flipped = rdd.map(flipKeyValue)
#     sortedRDD = flipped.sortByKey().map(flipKeyValue)
#     sortedCountries = sortedRDD.collect()
#     print "YEAR", year, "| Happiest Country:", sortedCountries[-1][COUNTRY_NAME_POS], "| Score:", sortedCountries[-1][1][SCORE_NUM_POS][0][SCORE_NUM_POS]


# SECOND ANALYSIS: Happiest region for each year
allRDDs = []
for row in happinessRDDs:
    allRDDs.append(row[1])

combinedRDD = sc.union(allRDDs)
readableResults = combinedRDD.reduceByKey(lambda x, y: (x[0], join(x[1], y[1]))).sortByKey()
results = readableResults.collect()

print

# for year in YEARS:
#     regionsRDD = readableResults.filter(lambda x: x[1][0] != "")
#     regionHappinessRDD = regionsRDD.map(lambda x: (x[1][0], findYearScore(x[1][1], year))).filter(lambda x: x[1]>0)
#     aggregatedRegionHappiness = regionHappinessRDD.mapValues(lambda x:(x, 1))\
#         .reduceByKey(lambda x, y: (x[0] + y[0], x[1]+y[1]))\
#         .mapValues(lambda x: x[0]/x[1])
#     sortedRegionHappiness = aggregatedRegionHappiness.map(lambda x: (x[1], x[0])).sortByKey().map(lambda x: (x[1], x[0]))
#     regionResults = sortedRegionHappiness.collect()
#     print year, "-Happiest Region:", regionResults[-1][0]
#     # for region in regionResults:
#     #     print(region)


# THIRD ANALYSIS: Happiest country overall
countriesTotalHappinessRDD = 

# FOURTH ANALYSIS: Happiest region overall

# for result in results:
#     print(result)
#print(readableResults.count())
# # happiest year of a country:
# happiest = flipped.reduceByKey(lambda x, y: max(x,y))
# result = happiest.collect()
# #print(result)

