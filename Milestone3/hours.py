
import findspark
findspark.init()
import pyspark
from pyspark import SparkContext as sc
from pyspark import SparkConf
import os
import re
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
import json


#Setting up Spark environment
spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext

#Create Dataframe
stars = spark.read.json("../yelp_dataset/yelp_academic_dataset_review.json").select("business_id","stars")
# hours = spark.read.json("../yelp_dataset/yelp_academic_dataset_business.json").select("business_id","hours")
hours = spark.read.json("../yelp_dataset/yelp_academic_dataset_business.json").select("hours","business_id")


def getHours(data):
	data = str(data)
	data = data.split("(")[-1]
	data = data.split(")")[0]
	data = data.split(", ")
	for i in range(len(data)):
		data[i] = data[i].split("=")[-1]
		if(data[i] != "None"):
			data[i] = re.split("[']",data[i])
			data[i] = str(data[i])[6:15]
			data[i] = data[i].split("-")
			data[i] = abs(int(data[i][0].split(":")[0]) - int(data[i][-1].split(":")[0]))
	total = 0
	div = 0
	for i in range(len(data)):
		if(data[i] != "None"):
			total = total + data[i]
			div = div + 1
	if(div != 0):
		data = total/div
	else:
		data = 0

	return(data)




combo = stars.join(hours, on = "business_id", how = "outer")
combo = combo.select("hours","stars")
combo = combo.rdd

greater = combo.map(lambda x: (getHours(x[0]),x[1])) \
		.filter(lambda x: (x[0]>=12)) 
greater = greater.collect()

add = 0
for i in range(len(greater)):
	add = add+greater[i][1]
greaterTotal = add/len(greater)


average = combo.map(lambda x: (getHours(x[0]),x[1])) \
		.filter(lambda x: ((x[0]<12)and(x[0]>9))) 
average = average.collect()

add = 0
for i in range(len(average)):
	add = add+average[i][1]
averageTotal = add/len(average)


less = combo.map(lambda x: (getHours(x[0]),x[1])) \
		.filter(lambda x: ((x[0]<=9)and(x[0]>0))) 
less = less.collect()

add = 0
for i in range(len(less)):
	add = add+less[i][1]
lessTotal = add/len(less)


file = open("HourCorrelation.txt", "w+")
line = "Average rating for store open greater or egual to 12 hours a day on average is "+str(greaterTotal)+"\n"
file.write(line)
line = "Average rating for store open between 9 and 12 hours a day on average is "+str(averageTotal)+"\n"
file.write(line)
line = "Average rating for store open less than or egual to 9 hours a day on average is "+str(lessTotal)+"\n"
file.write(line)



