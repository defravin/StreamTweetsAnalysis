# Databricks notebook source
### TREND ANALYSIS ON TWITER LIVE DATA USING SPARK STREAMING ###

#Import sparkContext & StreamingContext from PySpark library
from __future__ import print_function
from pyspark import SparkContext
from pyspark.streaming import StreamingContext

# COMMAND ----------

# Create a sparkContext with AppName "StreamingTwitterAnalysis"
# Setting the LogLevel of SparkContext to ERROR. This will no print all the logs which are INFO or WARN level.
# Create Spark Streaming Context using SC (spark context). Parameter 10 is the batch interval.
# Every 10 second the analysis will be done.

#sc = SparkContext(appName="StreamingTwitterAnalysis")
sc.setLogLevel("ERROR")
ssc =StreamingContext(sc,10)

# COMMAND ----------

#Connect to socket broker using ssc (spark streaming context)
socket_stream = ssc.socketTextStream("0.0.0.0", 5555)

# COMMAND ----------

#window function parameter sets the Window lenght. All the analysis will be done on tweets stored for 20 secs.
lines=socket_stream.window(20)

# COMMAND ----------

### PROCESS THE STREAM ###
#1. Receives tweet message, stored in lines. Input DStream
#2. splits the messages into words. Apply trasformation on DStream: flatMap
#3. filters all the words which start with a hashtag(#). trasformation: filter
#4. converts the words to lowercase. trasformation: map
#5. maps each tag to (word,1). trasformation: map
#6. then reduces and counts occurrences of each hashtag (action:reduceByKey) hashtags=output DStream

hashtags = lines.flatMap(lambda text: text.split(" ")).filter( lambda word: word.lower().startswith("#")).map( lambda word: (word.lower(),1)).reduceByKey(lambda a,b: a+b)

# COMMAND ----------

#Sort hashtags based on the counts in decreasing order
author_counts_sorted_dstream = hashtags.transform(lambda foo:foo.sortBy(lambda x:x[0].lower()).sortBy(lambda x:x[1],ascending=False))

# COMMAND ----------

#Print the final analysis: Most popular hashtags on streaming twitter data
author_counts_sorted_dstream.pprint()

# COMMAND ----------

### STARTING THE SPARK STREAMING ###
#Spark Streaming code we have written till now will not execute, until we start the ssc.
#ssc.start() will start the spark streaming context. This is the Action for the whole code.
#Now it'll create the lineage & DAG & do the lazy evaluation & start running the whole sequence of code
ssc.start()

# COMMAND ----------

#awaiTermination() is very important to stop the SSC. 
#When we kill this python process then this signal will be sent to awaitTermination() function.
#It will finally stop the spark streaming job
ssc.awaitTermination()
