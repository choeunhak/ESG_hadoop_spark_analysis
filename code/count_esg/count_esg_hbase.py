#-*- coding:utf-8 -*-
from numpy import true_divide
from esg_word import get_e_word, get_s_word,get_g_word
from pyspark.sql import SparkSession
from pyspark.mllib.feature import HashingTF, IDF
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.classification import NaiveBayes
from pyspark.mllib.classification import NaiveBayesModel 
from pyspark.sql.types import IntegerType
from starbase import Connection

#기업별로 테이블을 다르게해야하나?
c = Connection()
frequency = c.table("ahnlab")
if (frequency.exists()):
    frequency.drop()
frequency.create("ahnlab")
batch = frequency.batch()


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
    model.save(spark_classify,"/user/maria_dev/model/{}".format("test1000"))

if __name__ == "__main__":
    spark_classify = SparkSession.builder.appName("classify_news").getOrCreate()

    # df = spark_classify.read.format("csv").option("header", "true").option("escape","\"").option("encoding", "UTF-8").load("hdfs:///user/maria_dev/data/labeltest.csv")
    # #label integer로 형변환
    # df = df.withColumn("label", df["label"].cast(IntegerType()))
    
    # df=df.rdd
    # #summary none값 제거(전처리)
    # df = df.filter(lambda doc:doc['summary'] is not None)
    # #labels불러오기
    # tfidf = makeTFIDF(df)
    # labels = df.map(lambda doc: doc["label"])
    # print(labels.take(1002))
    # create_nbmodel(labels, tfidf)

    # 모델 불러오기
    saved_model = NaiveBayesModel.load(spark_classify,"/user/maria_dev/model/{}".format("test1000"))

    #분류할 데이터 불러오기
    unClassifiedDf = spark_classify.read.format("csv").option("header", "true").option("escape","\"").option("encoding", "UTF-8").load("hdfs:///user/maria_dev/data/ahnlab15_21.csv")

    # unClassifiedDf=unClassifiedDf.filter(unClassifiedDf.time.startswith('2021'))
    unClassifiedRdd=unClassifiedDf.rdd
    tfidf = makeTFIDF(unClassifiedRdd)
    pred=saved_model.predict(tfidf)

    print(unClassifiedRdd.count())
    print(pred.count())
    classifiedRdd=unClassifiedRdd.zip(pred)

    # classifiedRdd.saveAsObjectFile("/user/maria_dev/testobjectrdd")
    # classifiedRdd = spark_classify.objectFile("hdfs:///user/maria_dev/testobjectrdd")
    # spark_count = SparkSession.builder.appName("count_esg").getOrCreate()

    #esg 단어 불러오기
    e_word=get_e_word()
    s_word=get_s_word()
    g_word=get_g_word()

    #0,1 분류
    posRdd = classifiedRdd.filter(lambda x:x[1]==1)
    negRdd = classifiedRdd.filter(lambda x:x[1]==0)
    
    #2017년 1월 추출
    years=["2015"]#,"2017","2018","2019","2019","2020","2021", 2015년부터 2021년까지 실행
    months=["01","02","03","04","05","06","07","08""09","10","11","12"]
    eg_result=[]
    eb_result=[]
    sg_result=[]
    sb_result=[]
    gg_result=[]
    gb_result=[]

    #E단어 빈도수 구하기, ESG를 한꺼번에 구할 경우 메모리 오류가 발생
    for year in years:
        for month in months:
            year_month=str(year)+str(month)
            if(year=="2016"):
                break
            pos_dateFiltered = posRdd.filter(lambda x:x[0][0]==year_month)
            neg_dateFiltered = negRdd.filter(lambda x:x[0][0]==year_month)

            pos_word = pos_dateFiltered.flatMap(lambda x:x[0][3].split())
            neg_word = neg_dateFiltered.flatMap(lambda x:x[0][3].split())

            eg_word = pos_word.filter(lambda x:x in e_word)
            eb_word = neg_word.filter(lambda x:x in e_word)

            # eg_word.mapPartitions(lambda iter: [sum(1 for _ in iter)]).collect()
            # eb_word.mapPartitions(lambda iter: [sum(1 for _ in iter)]).collect()
            eg_size = eg_word.count()
            eb_size = eb_word.count()

            if batch:
                batch.insert("ahnlab"+"EG"+year+month, { 'EG_frequency' : { year+month : eg_size }}) 
                batch.insert("ahnlab"+"EB"+year+month, { 'EB_frequency' : { year+month : eb_size }}) 

            eg_result.append(eg_size)
            eb_result.append(eb_size)

        print(year,"년 완료")
    batch.commit(finalize=True)
    
    # for year in years:
    #     for month in months:
    #         year_month=str(year)+str(month)
    #         if(year=="2016"):
    #             break
    #         pos_dateFiltered = posRdd.filter(lambda x:x[0][0]==year_month)
    #         neg_dateFiltered = negRdd.filter(lambda x:x[0][0]==year_month)

    #         pos_word = pos_dateFiltered.flatMap(lambda x:x[0][3].split())
    #         neg_word = neg_dateFiltered.flatMap(lambda x:x[0][3].split())

    #         sg_word = pos_word.filter(lambda x:x in s_word)
    #         sb_word = neg_word.filter(lambda x:x in s_word)

    #         sg_size = sg_word.count()
    #         sb_size = sb_word.count()

    #         sg_result.append(sg_size)
    #         sb_result.append(sb_size)
    
    # for year in years:
    #     for month in months:
    #         year_month=str(year)+str(month)
    #         if(year=="2016"):
    #             break
    #         pos_dateFiltered = posRdd.filter(lambda x:x[0][0]==year_month)
    #         neg_dateFiltered = negRdd.filter(lambda x:x[0][0]==year_month)

    #         pos_word = pos_dateFiltered.flatMap(lambda x:x[0][3].split())
    #         neg_word = neg_dateFiltered.flatMap(lambda x:x[0][3].split())

    #         gg_word = pos_word.filter(lambda x:x in s_word)
    #         gb_word = neg_word.filter(lambda x:x in s_word)

    #         gg_size = gg_word.count()
    #         gb_size = gb_word.count()

    #         gg_result.append(gg_size)
    #         gb_result.append(gb_size)
    
    print("eg", eg_result)
    print("eb", eb_result)
    # print("sg", sg_result)
    # print("sb", sb_result)
    # print("gg", gg_result)
    # print("gb", gb_result)

    spark_classify.stop()

