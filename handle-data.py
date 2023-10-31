import pandas
import pymongo
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from sparknlp.base import *
from sparknlp.annotator import *
from pyspark.ml import Pipeline
import matplotlib.pyplot as plt


def plot_grap(User):
    data = pandas.read_csv("D:/data/" + User + ".csv", header=0)
    Comment = data["Comment"].replace(",", "", regex=True).astype(int).values
    Retweet = data["Retweet"].replace(",", "", regex=True).astype(int).values
    Quote = data["Quote"].replace(",", "", regex=True).astype(int).values
    Heart = data["Heart"].replace(",", "", regex=True).astype(int).values
    Day = data["Date Create Tweet"]
    for i in range(len(Day)):
        Day[i] = Day[i].split(",")[0]

    plt.figure(figsize=(18, 7))
    plt.plot(Day, Comment, label="Comment")
    plt.plot(Day, Retweet, label="Retweet")
    plt.plot(Day, Quote, label="Quote")
    plt.plot(Day, Heart, label="Heart")

    plt.legend()
    plt.savefig("D:/data/Image/" + User + ".jpg", format="jpg")
    plt.close()


def analysts_sentiment(Data, User):
    pipeline = Pipeline().setStages([document, token, normalizer, vivekn, finisher])
    pipelineModel = pipeline.fit(Data.select("Content Tweet"))
    result = pipelineModel.transform(Data.select("Tag Name", "Date Create Tweet", "Comment", "Retweet", "Quote", "Heart", "Image", "Content Tweet"))
    data = result.withColumn("Sentiment", col("Sentiment").cast("string"))
    data.write.mode("overwrite").csv("hdfs://localhost:9000/Data_tweet/" + User)


def select_user():
    users = spark.read.csv("D:/data/User.csv", header=True).collect()
    for user in users:
        file = spark.read.csv("D:/data/" + user[0] + ".csv", header=True)
        analysts_sentiment(file, user[0])
        plot_grap(user[0])


spark = SparkSession.builder.appName("Handle-data").master("local").getOrCreate()

document = DocumentAssembler().setInputCol("Content Tweet").setOutputCol("document")
token = Tokenizer().setInputCols(["document"]).setOutputCol("token")
normalizer = Normalizer().setInputCols(["token"]).setOutputCol("normal")
vivekn = ViveknSentimentModel.pretrained().setInputCols(["document", "normal"]).setOutputCol("result_sentiment")
finisher = Finisher().setInputCols("result_sentiment").setOutputCols("Sentiment")

select_user()