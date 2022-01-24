import boto3
from pprint import pprint as pp
import pandas as pd
import io

s3_client = boto3.client("s3")
s3_resource = boto3.resource("s3")

pd.set_option("display.max_rows", 500)
pd.set_option("display.max_columns", 500)
pd.set_option("display.width", 1000)

class Fish:
    def __init__(self, bucket_name = "data-eng-resources"):
        self.bucket_name = bucket_name
        self.filepath1 = "python/fish-market-mon.csv"
        self.filepath2 = "python/fish-market-tues.csv"
        self.filepath3 = "python/fish-market.csv"
        self.filtered_final_df = []
        self.has_been_called = False

    def read_csv(self): #Reading CSV files
        fm_mon_object = s3_client.get_object(Bucket=self.bucket_name, Key=self.filepath1)
        pp(fm_mon_object["Body"])
        df_mon = pd.read_csv(fm_mon_object["Body"])

        fm_tue_object = s3_client.get_object(Bucket=self.bucket_name, Key=self.filepath2)
        pp(fm_tue_object["Body"])
        df_tue = pd.read_csv(fm_tue_object["Body"])

        s3_object = s3_client.get_object(Bucket=self.bucket_name, Key=self.filepath3)
        pp(s3_object["Body"])
        df = pd.read_csv(s3_object["Body"])

        return df_mon, df_tue, df

    def combine_df(self, df_mon, df_tue, df): # Merge 3 dataframes together
        combined_df = pd.concat([df_mon, df_tue, df])
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
df_mon, df_tue, df = fish1.read_csv()
combined_df = fish1.combine_df(df_mon, df_tue, df)
final_df = fish1.find_average(combined_df)
fish1.one_species("Bream")
fish1.upload_to_s3()