#-*- coding:utf-8 -*-
from esg_word import get_e_word, get_s_word,get_g_word
from pyspark.sql import SparkSession
from pyspark.mllib.feature import HashingTF, IDF
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.classification import NaiveBayes
from pyspark.mllib.classification import NaiveBayesModel 
from pyspark.sql.types import IntegerType

def makeTFIDF(df):
    tf = HashingTF(numFeatures=128).transform(
    df.map(lambda doc: doc["summary"].split(), preservesPartitioning=True))
    idf = IDF().fit(tf)
    tfidf = idf.transform(tf)
    return tfidf

def create_nbmodel(corp):
    training = corp.map(lambda x: LabeledPoint(x[0], x[1]))
    model = NaiveBayes.train(training)
    model.save(spark,"/user/maria_dev/model/{}".format("pre_test"))

if __name__ == "__main__":
    spark = SparkSession.builder.appName("test_accuracy").getOrCreate()

    df = spark.read.format("csv").option("header", "true").option("escape","\"").option("encoding", "UTF-8").load("hdfs:///user/maria_dev/data/pre_labeldata.csv")
    #label integer로 형변환
    df = df.withColumn("label", df["label"].cast(IntegerType()))
    df=df.rdd

    #labels불러오기
    labels = df.map(
            lambda doc: doc["label"],
    )

    tfidf = makeTFIDF(df)
    corp=labels.zip(tfidf)
    # create_nbmodel(corp)

    #모델 불러오기
    saved_model = NaiveBayesModel.load(spark,"/user/maria_dev/model/{}".format("pre_test"))#accuracytest였음 원래

    pred=saved_model.predict(tfidf)

    print(pred.take(50))
    predictionAndLabel = labels.zip(pred)
    print(labels.count())
    accuracy = 1.0 * predictionAndLabel.filter(lambda pl: pl[0] == pl[1]).count() / df.count()
    print('model accuracy {}'.format(accuracy))

    #분류하기 테스트
    # labels_and_preds = labels.zip(saved_model.predict(tfidf)).map(
    #     lambda x: {"actual": x[0], "predicted": float(x[1])})

    #분류할 데이터 불러오기
    # unClassifiedDf = spark.read.format("csv").option("header", "true").option("escape","\"").option("encoding", "UTF-8").load("hdfs:///user/maria_dev/data/kgini.csv")

    # unClassifiedRdd=unClassifiedDf.rdd
    # tfidf = makeTFIDF(unClassifiedRdd)
    # pred=saved_model.predict(tfidf)
    # classifiedRdd=unClassifiedRdd.zip(pred)

    # #esg 단어 불러오기
    # e_word=get_e_word()
    # s_word=get_s_word()
    # g_word=get_g_word()

    # #0,1 분류
    # posRdd = classifiedRdd.filter(lambda x:x[1]==1)
    # negRdd = classifiedRdd.filter(lambda x:x[1]==0)

    # #2017년 1월 추출
    # years=["2018"]#,"2019","2020","2021"
    # months=["01","02","03","04","05","06","07","08""09","10","11","12"]
    # eg_result=[]
    # eb_result=[]
    # sg_result=[]
    # sb_result=[]
    # gg_result=[]
    # gb_result=[]
    # for year in years:
    #     for month in months:
    #         year_month=str(year)+str(month)
    #         if(year_month=="202111"):
    #             break
    #         # print(year_month)
    #         pos_dateFiltered = posRdd.filter(lambda x:x[0][0]==year_month)
    #         neg_dateFiltered = negRdd.filter(lambda x:x[0][0]==year_month)
    #         #flatmap으로 뭉치기
    #         pos_word = pos_dateFiltered.flatMap(lambda x:x[0][3].split())
    #         neg_word = neg_dateFiltered.flatMap(lambda x:x[0][3].split())

    #         # E단어에있는 단어만
    #         eg_word = pos_word.filter(lambda x:x in e_word)
    #         eb_word = neg_word.filter(lambda x:x in e_word)
    #         # sg_word = pos_word.filter(lambda x:x in s_word)
    #         # sb_word = neg_word.filter(lambda x:x in s_word)
    #         # gg_word = pos_word.filter(lambda x:x in g_word)
    #         # gb_word = neg_word.filter(lambda x:x in g_word)

    #         #전체 크기 구하기
    #         eg_size = eg_word.count()
    #         eb_size = eb_word.count()
    #         # sg_size = sg_word.count()
    #         # sb_size = sb_word.count()
    #         # gg_size = gg_word.count()
    #         # gb_size = gb_word.count()

    #         eg_result.append(eg_size)
    #         eb_result.append(eb_size)
    #         # sg_result.append(sg_size)
    #         # sb_result.append(sb_size)
    #         # gg_result.append(gg_size)
    #         # gb_result.append(gb_size)
    # print(eg_result)
    # print(eb_result)
    # print(sg_result)
    # print(sb_result)
    # print(gg_result)
    # print(gb_result)


