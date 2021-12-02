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

f = io.open(r"./tmp_labeldata.csv",'r', encoding='UTF8')#, 
rdr = csv.reader(f)
next(rdr)
lines = []
for line in rdr:
    line[3]=makeArrToStr(okt.nouns(line[3]))
    lines.append(line)

f = io.open(r"./pre_labeldata.csv",'wb')
wr = csv.writer(f)
wr.writerows(lines)
print("전처리 완료")

 
f.close()