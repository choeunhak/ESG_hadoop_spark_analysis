#-*- coding:utf-8 -*-
from konlpy.tag import Okt
import csv
from subprocess import PIPE, Popen
import os
import io

#encoding utf-8로 지정
import sys 
reload(sys) 
sys.setdefaultencoding('utf8')

okt=Okt()

def makeArrToStr(arr):
    tmp=""
    for i in arr:
        tmp=tmp+" "+i
    return tmp

com_list=["kg이니시스"]#"kg이니시스", "안랩", "BGF리테일", "씨젠", "셀트리온제약","현대그린푸드", "풀무원", "s-oil","쌍방울","kcc건설"
for com in com_list:
    corp_dic = {'kg이니시스':'kgini', '안랩':'ahnlab', 'BGF리테일': 'bgf'}
    com_name=com
    f = io.open(r"./batch/raw_data/raw_{}.csv".format(com),'r', encoding='UTF8')#, 
    rdr = csv.reader(f)
    next(rdr)
    lines = []
    for line in rdr:
        line[3]=makeArrToStr(okt.nouns(line[3]))
        lines.append(line)
    
    f = io.open(r"./batch/preprocessed_data/{}.csv".format(com),'wb')
    wr = csv.writer(f)
    wr.writerows(lines)
    print(com+"전처리 완료")

    hdfs_path = os.path.join(os.sep, 'user', 'maria_dev', "batch", "{}.csv".format(com_name))

    put = Popen(["hadoop", "fs", "-put", "-f", "./batch/preprocessed_data/{}.csv".format(com_name), hdfs_path], stdin=PIPE, bufsize=-1)
    put.communicate()
 
f.close()