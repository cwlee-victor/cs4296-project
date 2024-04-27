# 56611230 LEE CHUN WING CityU SDSC4296 Project Repo
## Technical project: Opinion Mining on Large-scale Business Review Data
### Project description:
- Data pipeline of Yelp dataset from local to AWS (S3)
- Data analysis, Sentiment analysis and modeling with PySpark on AWS (EMR)
- Web dashboard deployment

#### 1. Data pipeline
- 2 versions
- yelp-datapipeline.py: running on local, then manually input the .csv to the AWS S3 busket defined by yourself
```bash
# Please first download yelp dataset from: https://www.yelp.com/dataset
# Please get the yelp_academic_dataset_review.json and yelp_academic_dataset_business.json
    # and put them at cs4296-project/
# Then you need to create the environment, it is recommended to use conda
conda create -n your_env python=3.9
pip install -r requirements.txt
# Yun can then try to run:
python yelp-datapipeline.txt # on local
# OR run:
python yelp-to-s3.py <your_busket> <yelp_revivew_json_dir> <yelp_business_json_dir> # sdk pipeline to S3
```

#### 2. PySpark modelings
- Use spark to transform, process and model data
- This should be ran on AWS EMR (by client mode)
- Results will be stored in defined busket
```bash
# These 2 files should be uploaded to S3 first.
setting.sh # Run this shell script on EMR first to resolve the numpy module not found problem
yelp-spark-app.py # Then run this directly
yelp-spark-local-ipynb # This is the PySpark local implementations.
# Some plot which are hard to visualize but good to include in report will be plotted here
```

#### 3. Web dashboard
- This is a dashboard implemented by Python flask, plotly and dash.
- To show a use case for the cloud pipeline deployment
```bash
# Local deployment
python app.py
```
