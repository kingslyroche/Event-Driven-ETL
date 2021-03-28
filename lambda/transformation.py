import pandas as pd

def transform(nyt_df,jh_df):
    nyt_df["date"]=pd.to_datetime(nyt_df["date"],infer_datetime_format=True).dt.date
    jh_df["Date"]=pd.to_datetime(jh_df["Date"],infer_datetime_format=True).dt.date
    jh_df=jh_df[jh_df["Country/Region"]=="US"]
    merge_df=pd.merge(nyt_df,jh_df,how="inner",left_on='date', right_on='Date')
    final_df=merge_df[["date","cases","deaths","Recovered"]]
    return final_df