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
spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext

#Create Dataframe
df = spark.read.json("yelp_dataset/millionlines.json").select("stars","text")
#Convert to RDD
rdd = df.rdd
#Helper Function
def score(words,score):
	final = []
	for i in words:
		final.append((i,score))
	return final


#turns the json line into a tuple with the word and the review
#flatMap goes through each line in the file. On the text we are getting all the words seperate -> putting it into a helper function.
#filer will make sure its not in stopwords, filtering numbers out, and making sure its not nothing.
#map will remove the numbers in our x "text" and then we lower the text and keep the score. 
#reducedByKey will take word "a" and "b" and average a score for it. Example: "bad" = (1 + 2)/2 = 1.5
Bucket = rdd.flatMap(lambda a: score(a.text.split(" "),a.stars)) \
	.filter(lambda x: (x[0].lower() not in stopWords) and (re.match('^[\w]+$', x[0]) is not None) and len(x[0])!= 0) \
	.map(lambda x: (re.sub("[\d-]","",x[0].lower()),x[1])) \
	.reduceByKey(lambda a, b: (a + b)/2)


# .filter(lambda x: (x.lower() not in stopWords) and (re.match('^[\w]+$', x) is not None) and len(x)!= 0) \
# .map(lambda word: (re.sub("[\d-]","",word.lower()),1)) \



#Bucket.collect takes it out of RDD
Bucket = Bucket.collect()

file = open("Positivity_DB.txt", "w+")
for i in range(0, len(Bucket)):
	if(Bucket[i][0] != ""):
		line = Bucket[i][0] +"	"+str(Bucket[i][1])+"\n"
		file.write(line)








