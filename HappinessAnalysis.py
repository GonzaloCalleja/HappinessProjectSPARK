import re
from pyspark import SparkConf, SparkContext

# Create Spark context and name the project
conf = SparkConf().setMaster("local").setAppName("HappinessProjectSPARK")
sc = SparkContext(conf = conf)

#LOAD DATA
lines_2015 = sc.textFile("./dataFiles/2015.csv")
header_country = lines_2015.first().split(',')[0]
lines_2016 = sc.textFile("./dataFiles/2016.csv")
lines_2017 = sc.textFile("./dataFiles/2017.csv")
lines_2018 = sc.textFile("./dataFiles/2018.csv")
lines_2019 = sc.textFile("./dataFiles/2019.csv")


def parseLine(line):
    fields = line.split(',')
    country = fields[0]
    if country == ("Country"):
        return (country, 0)
    happiness = float(fields[3])
    return (country, happiness)

rdd = lines_2015.map(parseLine).filter(lambda x : (x[0] != header_country))
happinessPerCountry = rdd.sortByKey()

# reverse the key and the value, it swaps x for y, and y for x
flipped = happinessPerCountry.map(lambda x : (x[1], x[0]))
sortedHappinessPerCountry = flipped.sortByKey().map(lambda x : (x[1], x[0]))
resultsHappinessPerCountry = sortedHappinessPerCountry.collect()

# DEFINE HAPPIEST COUNTRY PER YEAR (year = 2015)
happiest_country = flipped.reduceByKey(lambda x, y: max(x, y))
print happiest_country.collect()





"""
for key, value in resultsHappinessPerCountry:
        print key, value
 """

