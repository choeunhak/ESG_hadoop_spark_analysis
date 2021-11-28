```
%pyspark
raw_BGF = spark.read.load("hdfs:///user/maria_dev/result/BGFretail_esg.csv", format="csv", sep=",", inferSchema="true", header="true", encoding="utf-8")
raw_BGF.registerTempTable("BGF")
```

```
%sql
select to_date(concat(year,lpad(month,2,'0'),'01'), 'yyyyMMdd'), eg,eb,sg,sb,gg,gb from BGF
```

->한국기업지배구조원의 우수, 비우수와 내가 평가한 지표 2021년 거보여주고
년도별 추이 보여주기!->완성