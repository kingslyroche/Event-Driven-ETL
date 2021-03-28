import boto3
import pandas as pd
from transformation import transform
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
        sys.exit(1)

def fetch_datasets(url):
    try:
        df=pd.read_csv(url)
        return df
    except Exception as e:
        notify("Failed fetching datasets - {}".format(e))
        sys.exit(1)

param_dic = {
    "host"      : rdsEndpoint,
    "database"  : rdsDatabaseName,
    "user"      : rdsUser,
    "password"  : rdsPassword
}

def connect(params_dic):
    """ Connect to the PostgreSQL database server """
    conn = None
    try:
        conn = psycopg2.connect(**params_dic)
    except (Exception, psycopg2.DatabaseError) as error:
        notify("Failed connecting to database- {}".format(e))
        sys.exit(1) 
    return conn

def single_insert(conn, insert_req):
    """ Execute a single INSERT request """
    cursor = conn.cursor()
    try:
        cursor.execute(insert_req)
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        notify("Error: %s" % error)
        conn.rollback()
        cursor.close()
        return 1
    cursor.close()

def lambda_handler(event, context):

    nyt_df=fetch_datasets(nytURL)

    jh_df=fetch_datasets(johnHopkinsURL)

    dataframe=transform(nyt_df,jh_df)

    # Connecting to the database
    conn = connect(param_dic)
    
    # Delete existing data
    cursor=conn.cursor()
    cursor.execute("delete from covid_case where true")
    conn.commit()
    cursor.close()

    # Inserting each row
    for index, row in dataframe.iterrows():
        query = """
        INSERT into covid_case(date, cases, deaths, Recovered) values('%s',%s,%s,%s);
        """ % (row['date'], row['cases'], row['deaths'],int(row['Recovered']))
        single_insert(conn, query)
    # Close the connection
    conn.close()


#just curious