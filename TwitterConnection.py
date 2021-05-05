# Databricks notebook source
### GET TWEETS IN REAL TIME USING TWITTER API ###
# We are using Tweepy library to route all the Live tweets to a TCP socket server.
# This server is hosted in cloud

# COMMAND ----------

#Install tweepy
%pip install tweepy

# COMMAND ----------

#Import tweepy library
import tweepy

# COMMAND ----------

#Import OuthHandler-
#It is used for Authentication protocol (OAuth) of Twitter API
from tweepy import OAuthHandler

# COMMAND ----------

#Import Stream & StreamListener
#This actually listen to streaming data getting from the socket
from tweepy import Stream
from tweepy.streaming import StreamListener

# COMMAND ----------

#Import socket. It is used to create socket connection with localhost i.e local server
#Json is used because twitter data comes in json format
import socket
import json

# COMMAND ----------

#to connect to API Twitter, we need these 4 Keys:
consumer_key='QOBLwiGRcQjXvtouXOHH2Xbf3'
consumer_secret='Nui1JkVjLMyaWhiMqqUt1F5Tva8yxXYdi6R7BvFcRLuexj1i5e'
access_token ='1388042760092016640-ObM8yGdyHVbdKF5PqW0boituNLLJnQ'
access_secret='Z1Owe44whPPUCeTRnrEVMkrCyUpNhlNiUbP4ADjo92gzF'

# COMMAND ----------

#Class TweetsListener 
#1. It is listening to the stream via StreamingListener.
#2. constructor function (init) : initializing the socket.
#3. data contains the tweets json coming from the stream.
#4. In this json object field "text" contains the actual Tweet.
#5. The actual tweet is extracted & sent to the client socket.
#6. Error handling is done to catch & throw errors.

class TweetsListener(StreamListener):
  def __init__(self,csocket):
    self.client_socket = csocket
  def on_data(self,data):
    try:
      msg = json.loads(data)
      print( msg['text'].encode('utf-8'))
      #print( msg['created_at'].encode('utf-8'))
      #print( msg['user']['name'].encode('utf-8'))
      self.client_socket.send( msg['text'].encode('utf-8'))
      return True
    except BaseException as e:
      print("Error on_data: %s" % str(e))
      return True
  def on_error(self, status):
    print(status)
    return True

# COMMAND ----------

### FUNCTION SendData(): ###
#1. auth is doing Authentication check using OAuthHandler. It uses 4 keys of Twitter APIs.
#2. consumer_key & consumer_secret are like username & access_token & access_secret are like password.
#3. twitter_stream will get the actual twitter live stream data.
#4. It'll call Stream with auth & call the Class TweetsListener & pass c_socket info to the class.
#5. From the stream of tweets, filter & take only tweets which contains "Covid" word

def sendData(c_socket):
  auth = OAuthHandler(consumer_key, consumer_secret)
  auth.set_access_token(access_token, access_secret)
  twitter_stream = Stream(auth, TweetsListener(c_socket))
  twitter_stream.filter(track=['covid19'])
  

# COMMAND ----------

### SOCKET CREATION ###
#1. Socket is created by using socket function 
#2. Host is localhost, where this notebook is running
#3. Port is 5555 (It can be anything, unique for this service )
#4. Now bind host & port address to the socket.

s=socket.socket() #create a socket object
host= "0.0.0.0" #get local machine name
port=5555  #Reserve a port for your service
s.bind((host,port)) #Bind to the port
print("Listening on port: %s" % str(port))

# COMMAND ----------

#socket will wait & listen for few seconds.
#after that connection is made
s.listen()   #Now wait for client connection
c, addr=s.accept()  #Establish connection with client
print( "Received request from: "+ str(addr))

# COMMAND ----------

sendData(c)
