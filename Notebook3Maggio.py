# Databricks notebook source
# MAGIC %pip install tweepy

# COMMAND ----------

#Import the necessary packages
import tweepy
from tweepy import Stream
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler

# COMMAND ----------

#Variabili che contengono le credenziali utente per accedere alle Twitter API
consumer_key='QOBLwiGRcQjXvtouXOHH2Xbf3'
consumer_secret='Nui1JkVjLMyaWhiMqqUt1F5Tva8yxXYdi6R7BvFcRLuexj1i5e'
access_token ='1388042760092016640-ObM8yGdyHVbdKF5PqW0boituNLLJnQ'
access_secret='Z1Owe44whPPUCeTRnrEVMkrCyUpNhlNiUbP4ADjo92gzF'

# COMMAND ----------

#Class for streaming and processing live tweets
class TwitterStreamer():
  def __init__(self):
    pass
  #I pass the filename of where we want to let's say write our tweets to instead of showing them on the terminal
  #we can both. I pass also the hashtag list to filter.
  def stream_tweets(self, fetched_tweets_filename, hashtag_list):
    # This handles Twitter authentication and the connection to the twitter streaming api.
    # create an object of the classe StdOutListener which is inheriting from StreamListener class
    listener = StdOutListener(fetched_tweets_filename)
    #authenticate using the credetials that I have stored in the other file.
    auth = OAuthHandler(consumer_key,consumer_secret)
    auth.set_access_token(access_token,access_secret)
    #at this point my application hopefully should be properly authenticated so I'll create a Twitter Stream
    #I will pass two parameters: the authentication token to verify that we have acutally authenticate properly
    #and then the object listener that we have created.
    stream = Stream(auth,listener)
    #I want to filter the tweets
    stream.filter(track=hashtag_list)
  
  
#This is a basic Listener class that just prints the received tweets to stdout
#The class StdOutListener is going to inherit from StreamListener, which provide methods that we can directly override.
class StdOutListener(StreamListener):
  
  def __init__(self,fetched_tweets_filename):
    self.fetched_tweets_filename=fetched_tweets_filename
  
  #on_data is an overrun method which will take in the data that is streamed in from StreamListener (so the one that it is listening for tweets)
  #and then it is going to print whatever we want with that data/tweets.
  def on_data(self,data):
    try:
      print(data)
      #mi salvo i tweets su file. "a" sta per append
      with open(self.fetched_tweets_filename, 'a') as tf:
        tf.write(data)
      return True
    except BaseException as e:
      print("Error on data: %s" %str(e))
      return True
  
  #on_error is a method that we are overriding from the StreamListener class that happens that there is an error that occurs.
  #we will print the status message of that error.
  def on_error(self,status):
    print(status)

#if we are in the main program
if __name__ == "__main__":
  hashtag_list = ["Fedez","DDL ZAN"]
  fetched_tweets_filename = "Tweets.json"
  
  twitter_streamer = TwitterStreamer()
  twitter_streamer.stream_tweets(fetched_tweets_filename, hashtag_list)
 
  
