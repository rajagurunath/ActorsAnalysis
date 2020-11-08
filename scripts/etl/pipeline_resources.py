from dagster import IntSource, StringSource, resource
from sqlalchemy import create_engine
import tweepy
import os

@resource(
    config_schema={
        "consumer_key":StringSource,
        "consumer_secret":StringSource

    }
)
def twitter(init_context):
    class twitterAuth:
        def __init__(self,resource_config):
            self.consumer_key    =  os.environ['t_consumer_key'] if resource_config['consumer_key']=="" else resource_config['consumer_key']
            self.consumer_secret =  os.environ['t_consumer_secret'] if resource_config['consumer_secret']=="" else resource_config['consumer_secret']
            self.auth = tweepy.AppAuthHandler(self.consumer_key, self.consumer_secret)
            self.api= tweepy.API(self.auth)

        def api(self):
            return self.api

        def get_user(self,userID):

            return self.api.get_user(userID)

    return twitterAuth(init_context.resource_config)

