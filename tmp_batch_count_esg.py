#-*- coding:utf-8 -*-
from esg_word import get_e_word, get_s_word,get_g_word
from pyspark.sql import SparkSession
from pyspark.mllib.feature import HashingTF, IDF
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.classification import NaiveBayes
from pyspark.mllib.classification import NaiveBayesModel 
from pyspark.sql.types import IntegerType
import datetime

def makeTFIDF(df):
    df = df.map(lambda doc: doc["summary"].split(), preservesPartitioning=True)
    tf = HashingTF(numFeatures=100).transform(df)
    idf = IDF().fit(tf)
    tfidf = idf.transform(tf)
    return tfidf

if __name__ == "__main__":
    corp_dic = {'kg이니시스':'kgini', '안랩':'ahnlab', 'BGF리테일': 'bgf','씨젠':'ceegene','셀트리온제약':'celltrion_ph','현대그린푸드':'hyundaegreenfood','kcc건설':'kcc_build','풀무원':'pulmuone','s-oil':'soil','쌍방울':'ssang'}

    spark = SparkSession.builder.appName("classify_news").getOrCreate()

    # 모델 불러오기
    corp_list=["kg이니시스"]#"kg이니시스", "안랩", "BGF리테일", "씨젠", "셀트리온제약","현대그린푸드", "풀무원", "s-oil","쌍방울","kcc건설"
    saved_model = NaiveBayesModel.load(spark,"/user/maria_dev/model/{}".format("pre_test"))

    today = datetime.date.today()
    firstDay = today.replace(day=1)
    lastMonth = firstDay - datetime.timedelta(days=1)
    year = lastMonth.strftime('%Y')
    month = lastMonth.strftime('%m')

    #분류할 데이터 불러오기
    for corp in corp_list:
        unClassifiedDf = spark.read.format("csv").option("header", "false").option("escape","\"").option("encoding", "UTF-8").load("hdfs:///user/maria_dev/batch/{}.csv".format(corp_dic[corp]))
        unClassifiedDf = unClassifiedDf.toDF('time','news','title','summary')

        #null값제거
        unClassifiedDf = unClassifiedDf.na.drop()
        unClassifiedRdd=unClassifiedDf.rdd
        tfidf = makeTFIDF(unClassifiedRdd)
        pred=saved_model.predict(tfidf)

        classifiedRdd=unClassifiedRdd.zip(pred)

        e_word=get_e_word()
        s_word=get_s_word()
        g_word=get_g_word()

        posRdd = classifiedRdd.filter(lambda x:x[1]==1)
        negRdd = classifiedRdd.filter(lambda x:x[1]==0)

        year_month=str(year)+str(month)
        pos_dateFiltered = posRdd.filter(lambda x:x[0][0]==year_month)
        neg_dateFiltered = negRdd.filter(lambda x:x[0][0]==year_month)

        pos_word = pos_dateFiltered.flatMap(lambda x:x[0][3].split())
        neg_word = neg_dateFiltered.flatMap(lambda x:x[0][3].split())

        eg_word = pos_word.filter(lambda x:x in e_word)
        eb_word = neg_word.filter(lambda x:x in e_word)

        sg_word = pos_word.filter(lambda x:x in s_word)
        sb_word = neg_word.filter(lambda x:x in s_word)

        gg_word = pos_word.filter(lambda x:x in g_word)
        gb_word = neg_word.filter(lambda x:x in g_word)

        eg_size = eg_word.count()
        eb_size = eb_word.count()

        sg_size = sg_word.count()
        sb_size = sb_word.count()

        gg_size = gg_word.count()
        gb_size = gb_word.count()

        columns = ['year','month','eg','eb','sg','sb','gg','gb']
        tmp_df = spark.createDataFrame([(year,month,eg_size,eb_size,sg_size,sb_size,gg_size,gb_size)], columns)

        #하둡에 저장
        tmp_df.write.csv("hdfs:///user/maria_dev/batch/res/{}_res.csv".format(corp_dic[corp]),mode="append",header=True)

    print(year,"년 완료")

    spark.stop()