#-*- coding:utf-8 -*-
import requests
from bs4 import BeautifulSoup
import pandas as pd
import datetime

com_list=["kg이니시스"]#"kg이니시스", "안랩", "BGF리테일", "씨젠", "셀트리온제약","현대그린푸드", "풀무원", "s-oil","쌍방울","kcc건설"
day_slist = ["01", "16"]
day_elist = ["15", "31"]

info = []
today = datetime.date.today()
first = today.replace(day=1)
lastMonth = first - datetime.timedelta(days=1)
for com in com_list:
    com_name=com
    for day_smem, day_emem in zip(day_slist, day_elist):
        ds = lastMonth.strftime('%Y') + "." + lastMonth.strftime('%m') + "." + day_smem   # 시작날 형식 입력
        de = lastMonth.strftime('%Y') + "." + lastMonth.strftime('%m') + "." + day_emem  # 끝날 형식 입력
        print(ds + "-" + de)
        start = -9  # page: 1부터 10씩 증가
        while (True):
            start = start + 10
            raw = requests.get(
                "https://search.naver.com/search.naver?where=news&sm=tab_pge&query=" + com_name + "&sort=0&photo=0&field=0&pd=3&ds=" + ds + "&de=" + de + "&mynews=0&office_type=0&office_section_code=0&news_office_checked=&nso=so:r,p:from0to0,a:all&start=" + str(
                    start) + "",
                headers={'User-Agent': 'Mozilla/5.0'})
            html = BeautifulSoup(raw.text, "html.parser")
            articles = html.select("ul.list_news > li")
            if (len(articles) == 1):# or len(articles) == 0
                break
            i = 0

            for a in articles:
                if(articles[i].select_one("a.info") and articles[i].select_one("a.news_tit") and articles[i].select_one("a.api_txt_lines") is not None):
                    b = articles[i].select_one("a.news_tit").text
                    c = articles[i].select_one("a.info").text
                    d = articles[i].select_one("a.api_txt_lines").text
                    info.append([str(lastMonth.strftime('%Y'))+str(lastMonth.strftime('%m')), c, b, d])
                    i = i + 1   
    # 중복제거 후 저장
    col_name = ['time', 'news', 'title', 'summary']
    list_df = pd.DataFrame(info, columns=col_name)
    newlist_df = list_df.drop_duplicates(['summary'], keep = 'first')
    newlist_df = newlist_df[~newlist_df['summary'].isnull()]
    newlist_df.to_csv(r"./batch/raw_data/raw_{}.csv".format(com_name), header = True, index = False, encoding='utf-8-sig')

    print(com+" 크롤링 완료")
print("크롤링 완료\n")