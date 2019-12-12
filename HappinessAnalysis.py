from pyspark import SparkConf, SparkContext
import re

DATA_POINTS = ["Country", "Region", "Score"]
YEARS = [2015, 2016, 2017, 2018, 2019]
FILE_DIRECTORY = "./dataFiles/"
FILE_EXTENSION = ".csv"
UNWANTED_PATTERN = r'\".*?\,.*?\"'
COUNTRY_NAME_POS = 0
REGION_NAME_POS = 1
SCORE_NAME_POS = -1


# Method to extract country name, region & happiness score
def parseLine(line, columns):
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
        if re.search("/d", element):
            values[index] = float(element)
        else:
            values[index] = element

    return key, values


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


# Method to flip without mattering whether it has a "Region" value or not
def flipKeyValue(line):
    if len(line[1]) == 2:
        return line[1][1], (line[1][0], line[0])
    elif len(line[1]) == 1:
        return line[1][0], [line[0]]


# Create Spark context and name the project
conf = SparkConf().setMaster("local").setAppName("HappinessProjectSPARK")
sc = SparkContext(conf=conf)

# LOAD DATA
happinessRDDs = []
for year in YEARS:
    happinessRDDs.append((year, cleanFile(sc.textFile(FILE_DIRECTORY + str(year) + FILE_EXTENSION))))

# FIRST ANALYSIS: Happiest country for each year
for year, rdd in happinessRDDs:
    flipped = rdd.map(flipKeyValue)
    sortedRDD = flipped.sortByKey().map(flipKeyValue)
    sortedCountries = sortedRDD.collect()
    print "YEAR", year, "| Happiest Country:", sortedCountries[-1][COUNTRY_NAME_POS], "| Score:", sortedCountries[-1][SCORE_NAME_POS][-1]

# SECOND ANALYSIS: Happiest region for each year

a = happinessRDDs[0][1].union(happinessRDDs[1][1])
#b = a.map(lambda x: (x[0], (x[1], x[2])))
c = a.reduceByKey(lambda x, y: (x[0], (x[1], y[1]))).sortByKey()
# print(c.count())
# e = c.collect()
# for w in e:
#     print(w)



#

# # happiest year of a country:
# happiest = flipped.reduceByKey(lambda x, y: max(x,y))
# result = happiest.collect()
# #print(result)

