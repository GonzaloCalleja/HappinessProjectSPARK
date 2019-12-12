from pyspark import SparkConf, SparkContext
import re

# IMPORTANT CONSTANTS
DATA_POINTS = ["Country", "Region", "Score"]
YEARS = [2015, 2016, 2017, 2018, 2019]
FILE_DIRECTORY = "./dataFiles/"
FILE_EXTENSION = ".csv"
UNWANTED_PATTERN = r'\".*?\,.*?\"'
COUNTRY_NAME_POS = 0
REGION_NAME_POS = 1

SCORE_NUM_POS = 1
YEAR_POS = 0


# Method to extract country name, region & happiness score from a line
def parseLine(line, columns, dataYear):
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
            values[index] = ((int(dataYear), float(element)),)
        else:
            values[index] = element

    if len(values) == 1:
        values.insert(0, "")

    return key, tuple(values)


# Since not all files are structured the same way we have to structure them the same way them:
# 2017 -> no Region & countries surrounded by ""
# 2018 & 2019 -> Country or Region
def cleanFile(lines, dataYear):
    header = lines.first().split(',')
    columns = []

    for index, columnName in enumerate(header):
        for dataPoint in DATA_POINTS:
            if dataPoint in columnName:
                columns.append(index)

    return lines.map(lambda row: parseLine(row, columns, dataYear)).filter(lambda x: DATA_POINTS[0] not in x[0])


# Method to flip without mattering whether it has a "Region" value or not
def flipScoreAndName(line):
    return line[1][1][0][1], (line[1][0], ((line[1][1][0][0], line[0]), ))


# Method to join 2 years information into large RDD keeping logical data structure
def join(tuple1, tuple2):
    if isinstance(tuple1[0], int):
        tuple1 = (tuple(tuple1), ) + tuple2
    else:
        tuple1 = tuple1+tuple2
    return tuple1


# Method to find the score in a certain year in the RDD (-1 if there is no data for the year
def findYearScore(yearAndScore, targetYear):
    for value in yearAndScore:
        if isinstance(value, int):
            return yearAndScore[1]
        if value[0] == targetYear:
            return value[1]
    return -1


# Method to return the average of all the Happiness scores given a tuple: ((year, score), .. )
def averageHappiness(happinessPerYear):
    total = 0
    for value in happinessPerYear:
        if isinstance(value, int):
            return happinessPerYear[1]
        total += value[1]
    return total / len(happinessPerYear)

# HERE START THE ANALYSIS

# FIRST ANALYSIS: Happiest country for each year
# for each year, place the score as the key, sort it, and then flip it again to return usable information
def countriesByHappinessInYear(rdd):
    flipped = rdd.map(flipScoreAndName)
    sortedYear = flipped.sortByKey().map(flipScoreAndName)
    sortedYearResult = sortedYear.collect()
    return sortedYearResult


# SECOND ANALYSIS: Happiest region for each year
def regionsByHappinessInYear(year, combinedRDD):
    regionHappinessRDD = combinedRDD\
        .map(lambda x: (x[1][0], findYearScore(x[1][1], year)))\
        .filter(lambda x: x[1] > 0)

    aggregatedRegionHappinessRDD = regionHappinessRDD\
        .mapValues(lambda x: (x, 1)) \
        .reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1])) \
        .mapValues(lambda x: x[0] / x[1])

    sortedRegionHappinessRDD = aggregatedRegionHappinessRDD\
        .map(lambda x: (x[1], x[0])).sortByKey().map(lambda x: (x[1], x[0]))

    sortedRegionResults = sortedRegionHappinessRDD.collect()
    return sortedRegionResults


# THIRD ANALYSIS: Happiest country overall
def countriesByAverageHappiness(combinedRDD):
    countriesAverageHappinessRDD = combinedRDD.map(lambda x: (x[0], averageHappiness(x[1][1])))

    sortedAverageHappinessRDD = countriesAverageHappinessRDD\
        .map(lambda x: (x[1], x[0])).sortByKey().map(lambda x: (x[1], x[0]))

    resultAverageHappiness = sortedAverageHappinessRDD.collect()
    return resultAverageHappiness


# FOURTH ANALYSIS: Happiest region overall
def regionByAverageHappiness(combinedRDD):
    regionAverageHappinessRDD = combinedRDD.map(lambda x: (x[1][0], averageHappiness(x[1][1])))

    aggregatedRegionAverageHappinessRDD = regionAverageHappinessRDD\
        .mapValues(lambda x:(x, 1))\
        .reduceByKey(lambda x, y: (x[0] + y[0], x[1]+y[1]))\
        .mapValues(lambda x: x[0]/x[1])

    sortedRegionAveragesRDD = aggregatedRegionAverageHappinessRDD.map(lambda x: (x[1], x[0]))\
        .sortByKey().map(lambda x: (x[1], x[0]))

    resultRegionAverageHappiness = sortedRegionAveragesRDD.collect()
    return resultRegionAverageHappiness

# FIFTH ANALYSIS happiest country per region
def happiestAverageCountryInRegion(combinedRDD):

    avergageHappinessRegionAsKeyRDD = combinedRDD.map(lambda x: (x[1][0], (averageHappiness(x[1][1]), x[0])))
    regionAndMaxAverageHappinessRDD = avergageHappinessRegionAsKeyRDD.reduceByKey(lambda x, y: max(x, y))
    resultHappiestAverageCountryInRegion = regionAndMaxAverageHappinessRDD.collect()
    return resultHappiestAverageCountryInRegion

# SIXTH ANALYSIS happiest country per region per year

def happiestCountryInRegionPerYear(year, combinedRDD):
    countryHappinessWithRegionAsKeyRDD = combinedRDD.map(lambda x: (x[1][0], (findYearScore(x[1][1], year), x[0])))
    regionMaxHappinessRDD = countryHappinessWithRegionAsKeyRDD.reduceByKey(lambda x, y: max(x, y))
    resultHappiestInRegion = regionMaxHappinessRDD.collect()
    return resultHappiestInRegion


# Create Spark context and name the project
conf = SparkConf().setMaster("local").setAppName("HappinessProjectSPARK")
sc = SparkContext(conf=conf)

# LOAD DATA into an array that stores the Year along with the RDD it corresponds to [(year, RDD), .. ]
happinessRDDs = []
for year in YEARS:
    happinessRDDs.append((year, cleanFile(sc.textFile(FILE_DIRECTORY + str(year) + FILE_EXTENSION), year)))

# Setting up large RDD that contains all important information of all years
# Useful because the region variable is only in some of the data sets, and this way we can use them in all
allRDDs = []
for row in happinessRDDs:
    allRDDs.append(row[1])

combinedRDD = sc.union(allRDDs)\
    .reduceByKey(lambda x, y: (x[0], join(x[1], y[1])))\
    .filter(lambda x: x[1][0] != "")

# PRINT FIRST ANALYSIS
print("FIRST ANALYSIS: Happiest Countries per Year")
for year, rdd in happinessRDDs:
    result = countriesByHappinessInYear(rdd)
    print('YEAR %d | Happiest Country: %-12s | Score:%.3f' %
           (year, result[-1][COUNTRY_NAME_POS], result[-1][1][SCORE_NUM_POS][0][SCORE_NUM_POS]))

# PRINT SECOND ANALYSIS
print("SECOND ANALYSIS: Happiest Regions per Year")
for year in YEARS:
    result = regionsByHappinessInYear(year, combinedRDD)
    print('YEAR %d | Happiest Region: %-12s | Average Score:%.3f' % (year, result[-1][0], result[-1][1]))

# PRINT THIRD ANALYSIS
print("THIRD ANALYSIS: Happiest Country on Average")
sortedCountriesByAverage = countriesByAverageHappiness(combinedRDD)
print('Happiest country on average: %s | Average Score: %.3f' %
      (sortedCountriesByAverage[-1][0], sortedCountriesByAverage[-1][1]))

# PRINT FOURTH ANALYSIS
print("FOURTH ANALYSIS: Sorted Average Region Happiness")
for region in regionByAverageHappiness(combinedRDD):
    print('Region: %-32s | Average Score: %.3f' % (region[0], region[1]))

# PRINT FIFTH ANALYSIS
print("FIFTH ANALYSIS: Happiest Country per Region on Average")
for region in happiestAverageCountryInRegion(combinedRDD):
    print('Region: %-32s | Happiest Average Country: %-15s | Average Score: %.3f' %
          (region[0], region[1][1], region[1][0]))

# PRINT SIXTH ANALYSIS
print("SIXTH ANALYSIS: Happiest Country in Region per Year")
for year in YEARS:
    print(year)
    for region in happiestCountryInRegionPerYear(year, combinedRDD):
        print('Region: %-32s | Happiest Average Country: %-15s | Average Score: %.3f' %
              (region[0], region[1][1], region[1][0]))

