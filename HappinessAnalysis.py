from pyspark import SparkConf, SparkContext


# method extract country name, region & happiness score
def parseLine(line, columns):
    fields = line.split(',')
    result = []

    for index, field in enumerate(fields):
        if index in columns:
            result.append(field)

    for index, element in enumerate(result):
        if element.replace('.', '', 1).isdigit():
            result[index] = float(element)
        else:
            result[index] = str(element)

    return result


# Create Spark context and name the project
conf = SparkConf().setMaster("local").setAppName("HappinessProjectSPARK")
sc = SparkContext(conf=conf)

# 2017 -> no Region
# 2018 & 2019 -> Country or Region

# LOAD DATA
data_files = {
    "2015" : sc.textFile("./dataFiles/2015.csv"),
    "2016" : sc.textFile("./dataFiles/2016.csv"),
    "2017" : sc.textFile("./dataFiles/2017.csv"),
    "2018" : sc.textFile("./dataFiles/2018.csv"),
    "2019" : sc.textFile("./dataFiles/2019.csv")

}


# Since not all files are structured the same way we
def cleanFile(lines):
    header = lines.first().split(',')

    dataPoints = ["Country", "Region", "Score"]
    columns = []

    for index, columnName in enumerate(header):
        for dataPoint in dataPoints:
            if dataPoint in columnName:
                columns.append(index)

    rdd = lines.map(lambda row: parseLine(row, columns)).filter(lambda x: x[0] != dataPoints[0])

    return rdd

# .filter(lambda x: x[0] != DataPoints[0])

# FIRST ANALYSIS: Happiest country for each year
lines = sc.textFile("./dataFiles/2015.csv")

a = cleanFile(lines).sortByKey().collect()
for i in a:
    print(i)

# for year, lines in data_files.items():
#     print(year)
#
#     header = lines.first().split(',')
#     for
#
#     # Create the RDD: 1) extract the 3 values we want 2) take out the header
#     rdd = lines.map(parseLine).filter(lambda x: x[0] != header)
#
#     alphabetically = rdd.sortByKey()
#
#     results = alphabetically.collect()
#
#     for result in results:
#         print(result)

# rdd = lines_2015.map(parseLine).filter(lambda x: x[0]!=header)
# happinessPerCountry = rdd.sortByKey()
#
# # reverse the key and the value, it swops x for y, and y for x
# flipped = happinessPerCountry.map(lambda x : (x[1], x[0]))
# sortedHappinessPerCountry = flipped.sortByKey().map(lambda x : (x[1], x[0]))
# resultsHappinessPerCountry = sortedHappinessPerCountry.collect()[-1]
# print "The happiest country is: ", str(resultsHappinessPerCountry[0]), "with a happiness score of: ", resultsHappinessPerCountry[1]
# print "The happiest country is: ", str(resultsHappinessPerCountry[0]), "with a happiness score of: ", resultsHappinessPerCountry[1]
#
# #for key, value in resultsHappinessPerCountry:
#     #print(str(key), value)
#
#
#
# # happiest year of a country:
# happiest = flipped.reduceByKey(lambda x, y: max(x,y))
# result = happiest.collect()
# #print(result)
#
# #for resul in result:
#     #print(resul[0])
