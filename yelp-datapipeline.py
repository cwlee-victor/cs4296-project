import numpy as np
import pandas as pd
import re

def clean_text(text):
    if pd.isnull(text):
        return text  # Return as is if text is None (NaN)
    text = text.strip()  # Remove leading and trailing whitespace
    text = text.lower()  # Convert text to lower case
    text = re.sub(r'[^\w\s]', '', text)  # Remove punctuation
    text = re.sub(r'\s+', ' ', text)  # Replace multiple spaces with a single space
    return text

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
        
    df_review = pd.concat(df_review, ignore_index=True)
    df_review = df_review.rename(columns={"stars":"review_stars"})

    print('Finish reading Yelp review data')

    with open(yelp_business_dir, "r", encoding="utf-8") as f:

        print('Reading Yelp business data')

        business_reader = pd.read_json(f, orient="records", lines=True, encoding='utf-8', 
                            dtype=business_dtypes, chunksize=1000)
            
        for chunk in business_reader:
            reduced_chunk = chunk.drop(columns=['address', 'postal_code', 'attributes', 'categories', 'hours'])\
                                .query("`is_open` == 1")
            df_business.append(reduced_chunk)
        
    df_business = pd.concat(df_business, ignore_index=True)
    df_business = df_business.rename(columns={"stars": "business_stars"})

    print('Finish reading Yelp business data')

    print('Joining data')

    joined_df = pd.merge(df_review, df_business, on='business_id', how='inner')

    return joined_df

if __name__ == "__main__":

    yelp_review_dir = 'yelp_academic_dataset_review.json'
    yelp_business_dir = 'yelp_academic_dataset_business.json'

    print('Data pipeline start')

    joined_df = data_pipeline(yelp_review_dir, yelp_business_dir)

    print('Data pipeline finish')

    print('Data Cleaning')

    joined_df['text'] = joined_df['text'].apply(clean_text)

    indices_to_drop = joined_df[joined_df['text'] == ''].index
    joined_df.drop(indices_to_drop, inplace=True)

    print('Data transforming to .csv')
        
    csv_path = 'data/yelp.csv'
    joined_df.to_csv(csv_path, sep='|', index=False)

    print('Data transformed to .csv')