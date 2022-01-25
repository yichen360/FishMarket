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

class Fish :
    def __init__(self, bucket_name = "data-eng-resources"):
        self.bucket_name = bucket_name
        self.fm_list = []
        self.df_list = []
        self.filtered_final_df = []
        self.has_been_called = False

    def get_objects(self): # List all objects in bucket with prefix
        bucket_contents = s3_client.list_objects_v2(Bucket=bucket_name, Prefix="python/fish-market")
        for object in bucket_contents["Contents"]:
            self.fm_list.append(str(object["Key"]))
        return self.fm_list

    def read_csv(self): # Reading CSV files
        for x in self.fm_list:
            fm_object = s3_client.get_object(Bucket=bucket_name, Key=x)
            df = pd.read_csv(fm_object["Body"])
            self.df_list.append(df)

        return self.df_list

    def combine_df(self): # Merge dataframes together
        combined_df = pd.concat(self.df_list)
        return combined_df

    def find_average(self, combined_df): # Find mean of each column in combined dataframe
        final_df = combined_df.groupby("Species").mean()
        print(final_df)
        return final_df

    def one_species(self, Species): #Filter dataframe for desired Species
        self.filtered_final_df = final_df.loc[str(Species)] #Locate row with Species name
        print(self.filtered_final_df)
        self.has_been_called = True
        return self.filtered_final_df, self.has_been_called

    def upload_to_s3(self): #Upload dataframe to S3 bucket
        if self.has_been_called == False:
            str_buffer = io.StringIO()
            final_df.to_csv(str_buffer)
            s3_client.put_object(
                Body=str_buffer.getvalue(),
                Bucket=self.bucket_name,
                Key="Data26/fish/Yi.csv")
        else:
            str_buffer = io.StringIO()
            self.filtered_final_df.to_csv(str_buffer)
            s3_client.put_object(
                Body=str_buffer.getvalue(),
                Bucket=self.bucket_name,
                Key="Data26/fish/Yi.csv")

fish1 = Fish()
fish1.get_objects()
fish1.read_csv()
combined_df = fish1.combine_df()
final_df = fish1.find_average(combined_df)
fish1.one_species("Bream")
fish1.upload_to_s3()