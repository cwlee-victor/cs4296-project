from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, count, avg
from pyspark.ml.feature import Tokenizer, StopWordsRemover
from pyspark.ml.feature import HashingTF, IDF, StringIndexer
from pyspark.sql.types import FloatType, IntegerType
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.classification import NaiveBayes

if __name__ == "__main__":

    spark = SparkSession.builder.appName("SparkEMR").getOrCreate()

    df = spark.read.option("multiLine", True).csv("s3://cs3103-yelp-raw/yelp.csv", header=True, sep='|')
    df = df.withColumn("review_stars", col("review_stars").cast(FloatType())) \
        .withColumn("useful", col("useful").cast(IntegerType())) \
        .withColumn("funny", col("funny").cast(IntegerType())) \
        .withColumn("cool", col("cool").cast(IntegerType())) \
        .withColumn("latitude", col("latitude").cast(FloatType())) \
        .withColumn("longitude", col("longitude").cast(FloatType())) \
        .withColumn("business_stars", col("business_stars").cast(FloatType())) \
        .withColumn("review_count", col("review_count").cast(IntegerType())) \
        .withColumn("is_open", col("is_open").cast(IntegerType()))
    df = df.withColumn("sentiment", when(df.review_stars >= 3.5, 1).otherwise(when(df.review_stars <= 2.5, -1).otherwise(0)))

    # Tokenize text
    tokenizer = Tokenizer(inputCol="text", outputCol="tokens")
    df_tokens = tokenizer.transform(df)

    # Stop word remove
    stopwords_remover = StopWordsRemover(inputCol="tokens", outputCol="words")
    filtered_data = stopwords_remover.transform(df_tokens)

    # HashingTF to convert text to numeric features
    hashingTF = HashingTF(inputCol="words", outputCol="rawFeatures", numFeatures=2**20)
    featurized_data = hashingTF.transform(filtered_data)

    # IDF to rescale features
    idf = IDF(inputCol="rawFeatures", outputCol="features")
    idf_model = idf.fit(featurized_data)
    rescaled_data = idf_model.transform(featurized_data)

    data = rescaled_data.select(
        col("business_stars"),
        col("review_count"),
        col("rawfeatures"),
        col("features"),
        col("sentiment")
    )
    label_stringIdx = StringIndexer(inputCol = "sentiment", outputCol = "label")
    label_model = label_stringIdx.fit(data)
    data = label_model.transform(data)

    (train, test) = data.randomSplit([0.8, 0.2])

    nb = NaiveBayes(smoothing=1.0, modelType="multinomial")

    nbModel = nb.fit(train)

    predictions = nbModel.transform(test)

    evaluator = MulticlassClassificationEvaluator(predictionCol="prediction")
    nb_accuracy = evaluator.evaluate(predictions, {evaluator.metricName: "accuracy"})
    print("Naive Bayes Classification Model Test Accuracy = %g" % nb_accuracy)

    result = spark.createDataFrame([
        ("Naive Bayes Classification", nb_accuracy)
    ], ["model", "accuracy"])

    result.write.format("csv").option("header", "true").mode("overwrite").save("s3://cs3103-yelp-raw/result")

    top10_result_review = df.groupBy("name").agg(count("review_stars").alias("review_stars_total")).orderBy(col("review_stars_total").desc()).limit(10)
    top10_result_review.write.format("csv").option("header", "true").mode("overwrite").save("s3://cs3103-yelp-raw/top10_result_review")

    top10_business_review = df.groupBy("name").agg(count("business_stars").alias("business_stars_total")).orderBy(col("business_stars_total").desc()).limit(10)
    top10_business_review.write.format("csv").option("header", "true").mode("overwrite").save("s3://cs3103-yelp-raw/top10_business_review")

    top10_city_review = df.groupBy("city").agg(count("business_stars").alias("business_stars_total")).orderBy(col("business_stars_total").desc()).limit(10)
    top10_city_review.write.format("csv").option("header", "true").mode("overwrite").save("s3://cs3103-yelp-raw/top10_city_review")

    review_stars_dist = df.groupBy("review_stars").agg(count("review_stars").alias("review_stars_total")).orderBy(col("review_stars").desc())
    review_stars_dist.write.format("csv").option("header", "true").mode("overwrite").save("s3://cs3103-yelp-raw/review_stars_dist")

    business_stars_dist = df.groupBy("business_stars").agg(count("business_stars").alias("business_stars_total")).orderBy(col("business_stars").desc())
    business_stars_dist.write.format("csv").option("header", "true").mode("overwrite").save("s3://cs3103-yelp-raw/business_stars_dist")

    top10_city_business_count = df.groupBy('city').count().sort(col("count").desc()).limit(10)
    top10_city_business_count.write.format("csv").option("header", "true").mode("overwrite").save("s3://cs3103-yelp-raw/top10_city_business_count")

    top10_most_reviewed_business = df.groupBy(col('name'), col('review_count'), col('city')).agg(avg("review_stars").alias("review_stars_average")).orderBy(col("review_count").desc()).limit(10)
    top10_most_reviewed_business.write.format("csv").option("header", "true").mode("overwrite").save("s3://cs3103-yelp-raw/top10_most_reviewed_business")

    sentiment_dist = data.groupBy("sentiment").agg(count("sentiment").alias("sentiment_total")).orderBy(col("sentiment").desc())
    sentiment_dist.write.format("csv").option("header", "true").mode("overwrite").save("s3://cs3103-yelp-raw/sentiment_dist")

    spark.stop()






