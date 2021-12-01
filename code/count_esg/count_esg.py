#-*- coding:utf-8 -*-
from numpy import true_divide
from esg_word import get_e_word, get_s_word,get_g_word
from pyspark.sql import SparkSession
from pyspark.mllib.feature import HashingTF, IDF
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.classification import NaiveBayes
from pyspark.mllib.classification import NaiveBayesModel 
from pyspark.sql.types import IntegerType
import pandas as pd

def makeTFIDF(df):
    df = df.map(lambda doc: doc["summary"].split(), preservesPartitioning=True)
    tf = HashingTF(numFeatures=100).transform(df)
    idf = IDF().fit(tf)
    tfidf = idf.transform(tf)
    return tfidf

def create_nbmodel(labels, tfidf):
    corp=labels.zip(tfidf)
    training = corp.map(lambda x: LabeledPoint(x[0], x[1]))
    model = NaiveBayes.train(training)
    model.save(spark,"/user/maria_dev/model/{}".format("pre_test"))

if __name__ == "__main__":
    spark = SparkSession.builder.appName("classify_news").getOrCreate()

    df = spark.read.format("csv").option("header", "true").option("escape","\"").option("encoding", "UTF-8").load("hdfs:///user/maria_dev/data/preprocessed_newsdata/pre_labeldata.csv")
    #label integer로 형변환
    df = df.withColumn("label", df["label"].cast(IntegerType()))

    rdd=df.rdd
    #summary none값 제거(전처리)
    rdd = rdd.filter(lambda doc:doc['summary'] is not None)
    #labels불러오기
    tfidf = makeTFIDF(rdd)
    labels = rdd.map(lambda doc: doc["label"])
    create_nbmodel(labels, tfidf)

    # 모델 불러오기
    saved_model = NaiveBayesModel.load(spark,"/user/maria_dev/model/{}".format("pre_test"))

    #분류할 데이터 불러오기
    corp_name="ssang"
    unClassifiedDf = spark.read.format("csv").option("header", "true").option("escape","\"").option("encoding", "UTF-8").load("hdfs:///user/maria_dev/data/preprocessed_newsdata/pre_{}.csv".format(corp_name))

    unClassifiedRdd=unClassifiedDf.rdd
    tfidf = makeTFIDF(unClassifiedRdd)
    pred=saved_model.predict(tfidf)

    print(unClassifiedRdd.count())
    print(pred.count())
    classifiedRdd=unClassifiedRdd.zip(pred)

    #esg 단어 불러오기
    e_word=get_e_word()
    s_word=get_s_word()
    g_word=get_g_word()

    #0,1 분류
    posRdd = classifiedRdd.filter(lambda x:x[1]==1)
    negRdd = classifiedRdd.filter(lambda x:x[1]==0)
    
    years=["2015","2016","2017","2018","2019","2020","2021"]
    months=["01","02","03","04","05","06","07","08","09","10","11","12"]

    #데이터프레임 생성
    columns = ['year', 'month', 'eg', 'eb', 'sg', 'sb', 'gg', 'gb']
    
    #E단어 빈도수 구하기, ESG를 한꺼번에 구할 경우 메모리 오류가 발생
    for year in years:
        for month in months:
            df = pd.DataFrame(columns=columns)
            year_month=str(year)+str(month)
            if(year_month=="202111"):#202111월건제외
                break
            pos_dateFiltered = posRdd.filter(lambda x:x[0][0]==year_month)
            neg_dateFiltered = negRdd.filter(lambda x:x[0][0]==year_month)

            pos_word = pos_dateFiltered.flatMap(lambda x:x[0][3].split())
            neg_word = neg_dateFiltered.flatMap(lambda x:x[0][3].split())

            eg_word = pos_word.filter(lambda x:x in e_word)
            eb_word = neg_word.filter(lambda x:x in e_word)

            eg_size = eg_word.count()
            eb_size = eb_word.count()

            sg_word = pos_word.filter(lambda x:x in s_word)
            sb_word = neg_word.filter(lambda x:x in s_word)

            sg_size = sg_word.count()
            sb_size = sb_word.count()

            gg_word = pos_word.filter(lambda x:x in g_word)
            gb_word = neg_word.filter(lambda x:x in g_word)

            gg_size = gg_word.count()
            gb_size = gb_word.count()

            tmp_df = {
                'year' : year,
                'month' : month,
                'eg' : eg_size,
                'eb' : eb_size,
                'sg' : sg_size,
                'sb' : sb_size,
                'gg' : gg_size,
                'gb' : gb_size
            }
            print(tmp_df)

            df = df.append(tmp_df, ignore_index=True)
            print(df)
            df.to_csv("{}_res.csv".format(corp_name), mode='a',header=False)

        print(year,"년 완료")
        

    spark.stop()