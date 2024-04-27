from dotenv import load_dotenv
import os
import sys
import numpy as np
import pandas as pd
import boto3
import re

def clean_text(text):
    if pd.isnull(text):
        return text  # Return as is if text is None (NaN)
    text = text.strip()  # Remove leading and trailing whitespace
    text = text.lower()  # Convert text to lower case
    text = re.sub(r'[^\w\s]', '', text)  # Remove punctuation
    text = re.sub(r'\s+', ' ', text)  # Replace multiple spaces with a single space
    return text

# create a bucket and upload .csv with aws sdk
def upload_file_to_s3(file_name, bucket, object_name=None):
    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = file_name

    # Upload the file
    s3 = boto3.client('s3')
    try:
        _ = s3.create_bucket(Bucket=bucket)
        print("Bucket created successfully.")
        _ = s3.upload_file(file_name, bucket, object_name)
    except Exception as e:
        print(f"An error occurred: {e}")
        return False
    return True

def data_pipeline(yelp_review_dir, yelp_business_dir):

    df_review = []
    df_business = []
    review_dtype = {"stars": np.float16, 
                "useful": np.int32, 
                "funny": np.int32,
                "cool": np.int32,
            }
    business_dtypes = {"stars": np.float16, 
                "latitude": np.float32, 
                "longitude": np.float32,
                "review_count": np.int32,
            }

    with open(yelp_review_dir, "r", encoding="utf-8") as f:

        print('Reading Yelp review data')
        review_reader = pd.read_json(f, orient="records", lines=True, encoding='utf-8', 
                            dtype=review_dtype, chunksize=1000)
            
        for chunk in review_reader:
            reduced_chunk = chunk.drop(columns=['review_id', 'user_id'])\
                                .query("`date` >= '2020-01-01'")
            df_review.append(reduced_chunk)
        
    df_review = pd.concat(df_review, ignore_index=True).rename({"stars":"review_stars"})

    print('Finish reading Yelp review data')

    with open(yelp_business_dir, "r", encoding="utf-8") as f:

        print('Reading Yelp business data')

        business_reader = pd.read_json(f, orient="records", lines=True, encoding='utf-8', 
                            dtype=business_dtypes, chunksize=1000)
            
        for chunk in business_reader:
            reduced_chunk = chunk.drop(columns=['address', 'postal_code'])\
                                .query("`is_open` == 1")
            df_business.append(reduced_chunk)
        
    df_business = pd.concat(df_business, ignore_index=True).rename({"stars":"business_stars"})

    print('Finish reading Yelp business data')

    print('Joining data')

    joined_df = pd.merge(df_review, df_business, on='business_id', how='inner')

    return joined_df

if __name__ == "__main__":

    load_dotenv()

    aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID'),
    aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY')

    s3_bucket_name = sys.argv[1]
    yelp_review_dir = sys.argv[2]
    yelp_business_dir = sys.argv[3]

    print('Data pipeline start')

    joined_df = data_pipeline(yelp_review_dir, yelp_business_dir)

    print('Data pipeline finish')

    print('Data Cleaning')

    joined_df['text'] = joined_df['text'].apply(clean_text)

    indices_to_drop = joined_df[joined_df['text'] == ''].index
    joined_df.drop(indices_to_drop, inplace=True)

    print('Data transforming to .csv')
        
    temp_csv_path = 'temp/upload_to_s3.csv'
    joined_df.to_csv(temp_csv_path, sep='|', index=False)

    print('Data transformed to .csv')

    print('Uploading .csv')
    
    # Upload to S3
    s3_object_name = 'yelp_review_with_business.csv'
    if upload_file_to_s3(temp_csv_path, s3_bucket_name, s3_object_name):
        print(f"File uploaded successfully to {s3_bucket_name}/{s3_object_name}")
    else:
        print("File upload failed.")
    
    os.remove(temp_csv_path)