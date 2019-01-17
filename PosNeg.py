import findspark
findspark.init()
import pyspark
from pyspark import SparkContext as sc
from pyspark import SparkConf
import os
import re
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext

stopWords = ["a","about","above","after","again","against","all","am","an","and","any","are",
"arent","as","at","be","because","been","before","being","below","between","both","but","by",
"cant","cannot","could","couldnt","did","didnt","do","does","doesnt","doing","dont","down",
"during","each","few","for","from","further","had","hadnt","has","hasnt","have","havent","having",
"he","hed","hell","hes","her","here","heres","hers","herself","him","himself","his","how","hows",
"i","id","ill","im","ive","if","in","into","is","isnt","it","its","its","itself","lets","me","more",
"most","mustnt","my","myself","no","nor","not","of","off","on","once","only","or","other","ought",
"our","oursourselves","out","over","own","same","shant","she","shed","shell","shes","should",
"shouldnt","so","some","such","than","that","thats","the","their","theirs","them","themselves",
"then","there","theres","these","they","theyd","theyll","theyre","theyve","this","those","through",
"to","too","under","until","up","very","was","wasnt","we","wed","well","were","weve","were","werent",
"what","whats","when","whens","where","wheres","which","while","who","whos","whom","why","whys","with",
"wont","would","wouldnt","you","youd","youll","youre","youve","your","yours","yourself","yourselves","", ""]

#Setting up Spark environment
# spark = SparkSession.builder.getOrCreate()
# sc = spark.sparkContext

choice = input("Please enter a file path you would like to test: ")
text = open(choice)
database = open("Positivity_DB.txt")


words_text = []
for i in text:
	words_text.extend(i.lower().split())
for i in words_text:
	if(i in stopWords):
		words_text.remove(i)

#Compare the list against database
score = -1
for i in database:
	i = i.split()
	if(len(i) == 2):
		i[1] = float(i[1])
		if(i[0] in words_text):
			if(score == -1):
				score = i[1]
			else:
				score = (score + i[1])/2



print("Given text has a positivity score of " +str(score)+ " out of 5.0")















