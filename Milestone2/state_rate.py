import findspark
findspark.init()
import pyspark
from pyspark import SparkContext as sc
from pyspark import SparkConf
import os
import re
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext

stateStopwords = ['AK', 'AL', 'AR', 'AZ', 'CA', 'CO', 'CT', 'DC', 'DE', 'FL', 'GA', 'HI', 'IA', 'ID', 'IL', 'IN', 'KS', 'KY', 'LA', 'MA', 
'MD', 'ME', 'MI', 'MN', 'MO', 'MS', 'MT', 'NC', 'ND', 'NE', 'NH', 'NJ', 'NM', 'NV', 'NY', 'OH', 'OK', 'OR', 'PA', 'RI', 'SC', 'SD', 'TN', 
'TX', 'UT', 'VA', 'VT', 'WA', 'WI', 'WV', 'WY']

#Setting up Spark environment
spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext

#Create Dataframe
stars = spark.read.json("../yelp_dataset/yelp_academic_dataset_review.json").select("business_id","stars")
state = spark.read.json("yelp_academic_dataset_business.json").select("business_id","state")

combo = stars.join(state, on = "business_id", how = "outer")

combo = combo.select("state","stars")

combo = combo.rdd

stateRank = combo.filter(lambda x: (x[1] is not None) and (x[0] in stateStopwords)) \
			.reduceByKey(lambda a, b: (a + b)/2)
stateRank = stateRank.takeOrdered(10, key= lambda x: -x[1])
#stateRank = stateRank.collect()
file = open("State_Rating.txt", "w+")
for i in range(0, len(stateRank)):
	if(stateRank[i][0] != ""):
		line = stateRank[i][0] +"	"+str(stateRank[i][1])+"\n"
		file.write(line)


