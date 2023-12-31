from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.feature import HashingTF
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split
import joblib

spark = SparkSession.builder \
    .appName("Train Model") \
    .config("spark.driver.memory", "8g") \
    .config("spark.shuffle.spill.numElementsForceSpillThreshold", 50000) \
    .config("spark.memory.offHeap.enabled", "true") \
    .config("spark.memory.offHeap.size", "1g") \
    .config("spark.executorEnv.OPENBLAS_NUM_THREADS", "1") \
    .getOrCreate()
data = spark.read.csv("D:/data/Data_clean_train.csv", header=True)
data = data.withColumn("label", col("label").cast("Int"))
tokenized_text = data.withColumn("tokens", split("Text", " "))
hashingTF = HashingTF(inputCol="tokens", outputCol="features")
labeled_text = hashingTF.transform(tokenized_text)
labeled_text.show()
trainData, testData = labeled_text.randomSplit([0.8, 0.2])
lr = LogisticRegression()
model = lr.fit(labeled_text)
predictions = model.transform(labeled_text)
predictions.show()
test_predictions = model.transform(testData)
test_predictions.show()
evaluator = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="accuracy")
accuracy = evaluator.evaluate(test_predictions)
print("Độ chính xác trên tập dữ liệu kiểm tra: {:.2%}".format(accuracy))
