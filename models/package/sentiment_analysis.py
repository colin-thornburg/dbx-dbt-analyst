import pyspark.sql.types as T
import pyspark.sql.functions as F
from nltk.sentiment import SentimentIntensityAnalyzer
import nltk

# Download required NLTK data
nltk.download('vader_lexicon')

@F.udf(returnType=T.StructType([
    T.StructField("compound", T.FloatType()),
    T.StructField("positive", T.FloatType()),
    T.StructField("neutral", T.FloatType()),
    T.StructField("negative", T.FloatType())
]))
def analyze_sentiment(text):
    sia = SentimentIntensityAnalyzer()
    sentiment = sia.polarity_scores(text)
    return (
        sentiment['compound'],
        sentiment['pos'],
        sentiment['neu'],
        sentiment['neg']
    )

def model(dbt, session):
    dbt.config(
        materialized="table",
        packages=["nltk"]
    )
    
    # Reference your upstream model
    df = dbt.ref("package_flight_surveys")
    
    # Apply sentiment analysis to the comment_txt field
    df_with_sentiment = df.withColumn(
        "sentiment",
        analyze_sentiment(F.col("comment_txt"))
    )
    
    # Unpack the sentiment struct for easier querying
    final_df = df_with_sentiment.select(
        "*",
        F.col("sentiment.compound").alias("sentiment_compound"),
        F.col("sentiment.positive").alias("sentiment_positive"),
        F.col("sentiment.neutral").alias("sentiment_neutral"),
        F.col("sentiment.negative").alias("sentiment_negative")
    ).drop("sentiment")
    
    return final_df