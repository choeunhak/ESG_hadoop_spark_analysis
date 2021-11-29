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
import datetime
from subprocess import PIPE, Popen
import os

def makeTFIDF(df):
    df = df.map(lambda doc: doc["summary"].split(), preservesPartitioning=True)
    tf = HashingTF(numFeatures=100).transform(df)
    idf = IDF().fit(tf)
    tfidf = idf.transform(tf)
    return tfidf

if __name__ == "__main__":
    spark_classify = SparkSession.builder.appName("classify_news").getOrCreate()

    # 모델 불러오기
    com_list=["kg이니시스"]#"kg이니시스", "안랩", "BGF리테일", "씨젠", "셀트리온제약","현대그린푸드", "풀무원", "s-oil","쌍방울","kcc건설"
    saved_model = NaiveBayesModel.load(spark_classify,"/user/maria_dev/model/{}".format("pre_test"))

    today = datetime.date.today()
    first = today.replace(day=1)
    lastMonth = first - datetime.timedelta(days=1)
    year = lastMonth.strftime('%Y')
    month = lastMonth.strftime('%m')

    #분류할 데이터 불러오기
    for com in com_list:
        unClassifiedDf = spark_classify.read.format("csv").option("header", "false").option("escape","\"").option("encoding", "UTF-8").load("hdfs:///user/maria_dev/batch/{}.csv".format(com))
        unClassifiedDf = unClassifiedDf.toDF('time','news','title','summary')

        #null값제거
        unClassifiedDf = unClassifiedDf.na.drop()
        print(unClassifiedDf.show(50))
        unClassifiedRdd=unClassifiedDf.rdd
        tfidf = makeTFIDF(unClassifiedRdd)
        pred=saved_model.predict(tfidf)

        classifiedRdd=unClassifiedRdd.zip(pred)

        e_word=get_e_word()
        s_word=get_s_word()
        g_word=get_g_word()

        posRdd = classifiedRdd.filter(lambda x:x[1]==1)
        negRdd = classifiedRdd.filter(lambda x:x[1]==0)

        #데이터프레임 생성
        columns = ['year', 'month', 'eg', 'eb', 'sg', 'sb', 'gg', 'gb']
        df = pd.DataFrame(columns=columns)
        year_month=str(year)+str(month)

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
        df.to_csv("res/{}_res.csv".format(com),header=False, index=False)

        # hdfs_path = os.path.join(os.sep, 'user', 'maria_dev', "batch", "{}.csv".format(com))

        # #하둡에서 결과csv파일을 가져온다

        # #저장한다
        # put = Popen(["hadoop", "fs", "-put", "-f", "./batch/preprocessed_data/{}.csv".format(com), hdfs_path], stdin=PIPE, bufsize=-1)
        # put.communicate()

    print(year,"년 완료")
        

    spark_classify.stop()