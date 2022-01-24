import boto3
from pprint import pprint as pp
import pandas as pd
import io

s3_client = boto3.client("s3")
s3_resource = boto3.resource("s3")
bucket_name = "data-eng-resources"

pd.set_option("display.max_rows", 500)
pd.set_option("display.max_columns", 500)
pd.set_option("display.width", 1000)

def read_csv(): #Reading CSV files
    fm_mon_object = s3_client.get_object(Bucket=bucket_name, Key="python/fish-market-mon.csv")
    pp(fm_mon_object["Body"])
    df_mon = pd.read_csv(fm_mon_object["Body"])

    fm_tue_object = s3_client.get_object(Bucket=bucket_name, Key="python/fish-market-tues.csv")
    pp(fm_tue_object["Body"])
    df_tue = pd.read_csv(fm_tue_object["Body"])

    s3_object = s3_client.get_object(Bucket=bucket_name, Key="python/fish-market.csv")
    pp(s3_object["Body"])
    df = pd.read_csv(s3_object["Body"])

    return df_mon, df_tue, df

def combine_df(df_mon, df_tue, df): # Merge 3 dataframes together
    combined_df = pd.concat([df_mon, df_tue, df])
    return combined_df

def find_average(combined_df): # Find mean of each column in combined dataframe
    final_df = combined_df.groupby("Species").mean()
    print(final_df)
    return final_df

def upload_to_s3(): #Upload dataframe to S3 bucket
    str_buffer = io.StringIO()
    final_df.to_csv(str_buffer)
    s3_client.put_object(
        Body=str_buffer.getvalue(),
        Bucket=bucket_name,
        Key="Data26/fish/Yi.csv")

df_mon, df_tue, df = read_csv()
combined_df = combine_df(df_mon, df_tue, df)
final_df = find_average(combined_df)
upload_to_s3()