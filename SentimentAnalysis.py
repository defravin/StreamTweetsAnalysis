# Databricks notebook source
#Python's library for processing textual data
%pip install textblob

# COMMAND ----------

### IMPORT THE NECESSARY PACKAGES ###
# We use pyspark, which is the Python API for Spark. Here, we use Spark Structured Streaming, which is a stream processing engine built on
# the Spark SQL engine and that's why we import the pyspark.sql module. We import its classes; SparkSession to create a stream session, 
# function, and types to make a list of built-in functions and data types available. We also use textblob for the tweet text classification.

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import functions as F
from textblob import TextBlob

# COMMAND ----------

### TWEET PREPROCESSING ###
#We preprocess the tweets so we can have only the clean text of the tweet. In each batch, we receive many tweets from the Twitter API
#and split the tweets at the string t_end. Then, we remove the empty rows and apply regular expressions to clean up the tweet text.
#in more detail; we remove the links(https://..), the username (@..), the hashtags (#), the string that shows if the current tweet 
#is a retweet (RT), and the character :.

def preprocessing(lines):
  #explode - PySpark explode array to rows
  #Pyspark function explode(e: Column) is used to explode or create array to rows. When an array is passed to this function, it created a new 
  #default column "col1" and it contains all array elements. With alias we set the name of column.
  words = lines.select(explode(split(lines.value, "t_end")).alias("word"))
  
  #replace returns a new DataFrame replacing a value with another value.
  words = words.na.replace('',None)
  
  #By default drop() without arguments remove all rows that have null values on any column of DataFrame
  words = words.na.drop()
  
  #PySpark withColumn() is a transformation function of DataFrame which is used to change the value, convert the datatype of an existing column, create a new column, and many
  #more.
  words = words.withColumn('word', F.regexp_replace('word', r'http\S+', ''))
  words = words.withColumn('word', F.regexp_replace('word', '@\w+', ''))
  words = words.withColumn('word', F.regexp_replace('word', '#', ''))
  words = words.withColumn('word', F.regexp_replace('word', 'RT', ''))
  words = words.withColumn('word', F.regexp_replace('word', ':', ''))
