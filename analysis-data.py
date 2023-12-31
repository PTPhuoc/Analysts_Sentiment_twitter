import csv
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.feature import HashingTF
from pyspark.sql import SparkSession
import re
from pyspark.sql.functions import col, split, udf
from pyspark.sql.types import StringType


def clean_text(text):
    cleaned_text = re.sub(r"[!@#$%^&*()+=,.:;'/-]", "", text.replace("\"", "").replace("\n", ""))
    return cleaned_text


def clean_data():
    data = spark.read.csv("D:/data/Reviews.csv", header=True)
    data = data.withColumn("Score", col("Score").cast("Int"))
    with open("D:/data/Data_clean_train.csv", "w", encoding="utf-8", newline="") as file:
        writer = csv.writer(file)
        writer.writerow(["Text", "label"])
    for dt in data.select("Text", "Score").collect():
        if dt["Text"] is not None:
            if dt["Score"] is not None:
                with open("D:/data/Data_clean_train.csv", "a", encoding="utf-8", newline="") as file:
                    writer = csv.writer(file)
                    writer.writerow([clean_text(dt["Text"]), dt["Score"]])


def sentiment_data(prediction):
    if prediction == 0:
        return "Saddest"
    if prediction == 1:
        return "Sad"
    if prediction == 2:
        return "Normal"
    if prediction == 3:
        return "Happy"
    if prediction == 4:
        return "Happiest"
    if prediction >= 5:
        return "Very Happy"
    else:
        return "So Sad"


def train_data():
    data = spark.read.csv("D:/data/Data_clean_train.csv", header=True)
    data = data.withColumn("label", col("label").cast("Int"))
    tokenized_text = data.withColumn("tokens", split("Text", " "))
    tokenized_text.show()
    hashingTF = HashingTF(inputCol="tokens", outputCol="features")
    labeled_text = hashingTF.transform(tokenized_text)
    labeled_text.show()
    trainData, testData = labeled_text.randomSplit([0.8, 0.2])
    lr = LogisticRegression()
    model = lr.fit(labeled_text)
    predictions = model.transform(labeled_text)
    predictions.show()
    test_predictions = model.transform(testData)
    evaluator = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="accuracy")
    accuracy = evaluator.evaluate(test_predictions)
    print("Độ chính xác trên tập dữ liệu kiểm tra: {:.2%}".format(accuracy))
    return model


spark = SparkSession.builder \
    .appName("Analysis_data") \
    .config("spark.driver.memory", "8g") \
    .config("spark.shuffle.spill.numElementsForceSpillThreshold", 50000) \
    .config("spark.memory.offHeap.enabled", "true") \
    .config("spark.memory.offHeap.size", "1g") \
    .config("spark.executorEnv.OPENBLAS_NUM_THREADS", "1") \
    .getOrCreate()

data = spark.read.csv("D:/data/data_tweet.csv", header=True)
data = data.select("Name", "Content Tweet")
data.show()
tokenized_text = data.withColumn("tokens", split("Content Tweet", " "))
hashingTF = HashingTF(inputCol="tokens", outputCol="features")
labeled_text = hashingTF.transform(tokenized_text)
model = train_data()
predictions = model.transform(labeled_text)
predictions.show()
predictions.printSchema()
sentiment_udf = udf(sentiment_data, StringType())
predictions_with_sentiment = predictions.withColumn("Sentiment", sentiment_udf(predictions["prediction"]))
predictions_with_sentiment.show()
