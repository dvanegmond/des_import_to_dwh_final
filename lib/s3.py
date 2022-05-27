import boto3
from io import StringIO
client = boto3.client('s3')
s3_resource = boto3.resource('s3')
import pandas as pd

'''
Get object from s3
'''
def get_csv2df(fileKey,sourceBucket):
    obj1 = client.get_object(Bucket=sourceBucket, Key=fileKey)
    df = pd.read_csv(obj1['Body'], sep ='|')
    return df
'''
Put object to s3
'''
def put_df2csv(df,file_name,bucket):
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, sep='|',index=False)
    s3_resource = boto3.resource('s3')
    s3_resource.Object(bucket, file_name).put(Body=csv_buffer.getvalue())
