import boto3
import pandas as pd
import transformation
import psycopg2
import os

rdsEndpoint = os.environ['endpoint']
rdsPort = os.environ['port']
rdsUser = os.environ['user']
rdsRegion = os.environ['region']
rdsDatabaseName = os.environ['database']
rdsPassword = os.environ['password']
johnHopkinsURL = os.environ['jh']
nytURL = os.environ['nyt']
snsARN = os.environ['sns']

def notify(text):
    try:
        sns = boto3.client('sns')
        sns.publish(TopicArn = snsARN, Message = text)
    except Exception as e:
        print("Not able to send notification due to {}".format(e))
        exit(1)

def fetch_datasets(url):
    try:
        df=pd.read_csv(url)
        return df
    except Exception as e:
        print("Failed fetching datasets - {}".format(e))
        exit(1)

def lambda_handler(event, context):

    nyt_df=fetch_datasets(nytURL)

    jh_df=fetch_datasets(johnHopkinsURL)

    final_df=transform(nyt_df,jh_df)

    connect_string = "postgresql://{}:{}@{}:{}/{}" .format(rdsUser,rdsPassword,rdsEndpoint,rdsPort,rdsDatabaseName)

    final_df.to_sql("covid_case",con=connect_string,if_exists='replace')
