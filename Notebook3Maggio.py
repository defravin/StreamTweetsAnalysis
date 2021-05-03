# Databricks notebook source
# MAGIC %pip install tweepy

# COMMAND ----------

#Step 1: Import the necessary packages
import tweepy
from tweepy import Stream
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
import socket
import json

# COMMAND ----------

#Step 2: Insert your credentials
consumer_key='QOBLwiGRcQjXvtouXOHH2Xbf3'
consumer_secret='Nui1JkVjLMyaWhiMqqUt1F5Tva8yxXYdi6R7BvFcRLuexj1i5e'
access_token ='1388042760092016640-ObM8yGdyHVbdKF5PqW0boituNLLJnQ'
access_secret='Z1Owe44whPPUCeTRnrEVMkrCyUpNhlNiUbP4ADjo92gzF'
