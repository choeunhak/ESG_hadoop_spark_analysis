#-*- coding:utf-8 -*-
from esg_word import get_e_word, get_s_word,get_g_word
from pyspark.sql import SparkSession
from pyspark.mllib.feature import HashingTF, IDF
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.classification import NaiveBayes
from pyspark.mllib.classification import NaiveBayesModel 
from pyspark.sql.types import IntegerType

def makeTFIDF(df):
    tf = HashingTF(numFeatures=100).transform(
    df.map(lambda doc: doc["summary"].split(), preservesPartitioning=True))
    idf = IDF().fit(tf)
    tfidf = idf.transform(tf)
    return tfidf

if __name__ == "__main__":
    spark = SparkSession.builder.appName("test_accuracy").getOrCreate()

    df = spark.read.format("csv").option("header", "true").option("escape","\"").option("encoding", "UTF-8").load("hdfs:///user/maria_dev/data/preprocessed_newsdata/pre_labeldata.csv")

    #label integer로 형변환
    df = df.withColumn("label", df["label"].cast(IntegerType()))
    df = df.na.drop()
    rdd=df.rdd

    #labels불러오기
    labels = rdd.map(lambda doc: doc["label"],)
    tfidf = makeTFIDF(rdd)

    #모델 불러오기
    saved_model = NaiveBayesModel.load(spark,"/user/maria_dev/model/{}".format("pre_test"))

    pred=saved_model.predict(tfidf)
    print(labels.count())
    predictionAndLabel = labels.zip(pred)
    print(pred.count())
    print(predictionAndLabel.count())
    accuracy = 1.0 * predictionAndLabel.filter(lambda pl: pl[0] == pl[1]).count() / rdd.count()
    print('model accuracy {}'.format(accuracy))

