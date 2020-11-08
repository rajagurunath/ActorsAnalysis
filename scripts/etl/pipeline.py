from dagster import execute_pipeline, pipeline, solid,Field,Int,InputDefinition,ModeDefinition
import pandas as pd
from pipeline_resources import twitter
from kafka import KafkaProducer
from kafka.errors import KafkaError
import json
import os
import json
import logging

quote_keys=['quoted_status.user.name','quoted_status.user.followers_count','quoted_status.user.friends_count',
           'quoted_status.user.listed_count','quoted_status.user.favourites_count','quoted_status.retweet_count',
           'quoted_status.favorite_count']
normal_timeline_keys=['retweet_count','favorite_count','user.screen_name']


@solid(
    config_schema={
      "actors_twitter_id":Field(str,is_required=False,default_value="@RanaDaggubati")
    },
    required_resource_keys={"twitter"}
)
def get_friends(context):
    userID=context.solid_config['actors_twitter_id']
    user=context.resources.twitter.api.get_user(userID)
    context.log.info(f"Preparing data for {user.screen_name}")
    context.log.info(f"{user.screen_name} has {user.followers_count} followers count")
    return userID

@solid(
    config_schema={
        # "actors_twitter_id":Field(str,is_required=False,default_value="@RanaDaggubati"),
        "tweet_count":Field(Int,is_required=False,default_value=50)
    },
    required_resource_keys={"twitter"}
)
def get_timeline(context,userID):
    # userID = context.solid_config['actors_twitter_id']
    user = context.resources.twitter.api.get_user(userID)
    timeline = user.timeline(count=context.solid_config['tweet_count'])
    timelineDF = pd.json_normalize([t._json for t in timeline])
    return timelineDF

@solid(
    config_schema={

        "data_path":Field(str,is_required=False,default_value="../../data")
    }
)
def store_raw_data(context,userID,timelineDF):
    data_path=os.path.join(context.solid_config['data_path'],userID)
    if not os.path.exists(data_path):
        os.mkdir(data_path)

    timelineDF.to_csv(os.path.join(data_path,"example1.csv"))

@solid #(input_defs=[InputDefinition("timeline",pd.DataFrame)])
def refine_the_data(context,timelineDF):
    rows=[]
    for r in timelineDF.iterrows():
        tmp = {}
        if r[1]['is_quote_status']:
            for k in quote_keys:
                tmp[f"Tweet_{r[1]['id_str']}.{k}"] = r[1][k]

        for k in normal_timeline_keys:
            tmp[f"Tweet_{r[1]['id_str']}.{k}"] = r[1][k]
        rows.append(tmp)
    return rows

@solid(
    config_schema={
        "broker_url":Field(str,is_required=False,default_value="localhost:9092")
    }
)
def kafka_publisher(context,rows):
    # producer = KafkaProducer(bootstrap_servers=[context.solid_config['broker_url']],
    #                          value_serializer=lambda m: json.dumps(m).encode('ascii'))
    context.log.info("Producer intialised")
    for row in rows:
        context.log.info(f"Publishing {row}")
    # print(record_metadata.topic)
    # print(record_metadata.partition)
    # print(record_metadata.offset)
    #
    return

@pipeline(

    mode_defs=[
        ModeDefinition("default", resource_defs={"twitter": twitter}),
        # ModeDefinition("prod", resource_defs={"database": postgres_database}),
    ],
)
def twitter_pipeline():
    s1=get_friends()
    s2=get_timeline(s1)
    store_raw_data(s1,s2)
    s3=refine_the_data(s2)
    s4=kafka_publisher(s3)
