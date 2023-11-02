import pandas
import pymongo
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from sparknlp.base import *
from sparknlp.annotator import *
from pyspark.ml import Pipeline


def analysts_sentiment(Data):
    pipeline = Pipeline().setStages([document, token, normalizer, vivekn, finisher])
    pipelineModel = pipeline.fit(Data.select("Content Tweet"))
    result = pipelineModel.transform(Data.select("Tag Name", "Date Create Tweet", "Comment", "Retweet", "Quote", "Heart", "Image", "Content Tweet"))
    data = result.withColumn("Sentiment", col("Sentiment").cast("string"))
    # data.write.mode("overwrite").csv("hdfs://localhost:9000/Data_tweet/" + User)
    result.show()


def select_user():
    file = spark.read.csv("D:/data/data_tweet.csv", header=True)
    analysts_sentiment(file)


spark = SparkSession.builder.appName("Handle-data").master("local").getOrCreate()
document = DocumentAssembler().setInputCol("Content Tweet").setOutputCol("document")
token = Tokenizer().setInputCols(["document"]).setOutputCol("token")
normalizer = Normalizer().setInputCols(["token"]).setOutputCol("normal")
vivekn = ViveknSentimentModel.pretrained().setInputCols(["document", "normal"]).setOutputCol("result_sentiment")
finisher = Finisher().setInputCols("result_sentiment").setOutputCols("Sentiment")

select_user()
